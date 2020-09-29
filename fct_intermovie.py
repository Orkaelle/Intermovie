import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os
import csv

DATAS_LOCAL_PATH = './DATAS/'
RAW_LOCAL_PATH = DATAS_LOCAL_PATH + 'RAW/'
CURATED_LOCAL_PATH = DATAS_LOCAL_PATH + 'CURATED/'
ZIP_LOCAL_PATH = DATAS_LOCAL_PATH + 'movies_dataset.zip'
URL = 'https://stdatalake005.blob.core.windows.net/public/movies_dataset.zip'
RESULT_LOCAL_PATH = './RESULTS/'


def check_folder ():
    PATH = [DATAS_LOCAL_PATH, RAW_LOCAL_PATH, RESULT_LOCAL_PATH, CURATED_LOCAL_PATH]
    for p in PATH:
        if not os.path.exists(p):
            os.mkdir(p)


def ensure_data_loaded():
    '''
    Ensure if data are already loaded. Download if missing
    '''
    if os.path.exists(ZIP_LOCAL_PATH) == False:
        dl_data()
    else :
        print('Datas already douwnloaded.')
    if len(os.listdir(RAW_LOCAL_PATH)) == 0:
        extract_data()
    else :
        print('Datas already extracted.')

    print ('Datas are successfully loaded.\n')

def dl_data ():
        print ('Downloading...')
        with open(ZIP_LOCAL_PATH, "wb") as f:
            r = requests.get(URL)
            f.write(r.content)
        print ('Dataset dowloaded successfully.')


def extract_data():
    print ('Extracting...')
    with zipfile.ZipFile(ZIP_LOCAL_PATH, 'r') as z:
        z.extractall(RAW_LOCAL_PATH)
    print ('Dataset extracted successfully.')


def create_df ():
    '''
    Création des datasets originaux
    '''
    print ('\nConverting tsv files...')
    actors = pd.read_csv(f'{RAW_LOCAL_PATH}name.basics.tsv', sep = '\t', usecols=['nconst','primaryName'], encoding='utf-8')
    principals = pd.concat([
        pd.read_csv(f'{CURATED_LOCAL_PATH}PRINCIPALS_actor.csv', usecols=['tconst','nconst'], encoding='utf-8'),
        pd.read_csv(f'{CURATED_LOCAL_PATH}PRINCIPALS_actress.csv', usecols=['tconst','nconst'], encoding='utf-8')
        ])
    basics = pd.concat([
        pd.read_csv(f'{CURATED_LOCAL_PATH}BASICS_movie.csv', usecols=['tconst','primaryTitle','originalTitle','genres'], encoding='utf-8'),
        pd.read_csv(f'{CURATED_LOCAL_PATH}BASICS_tvMovie.csv', usecols=['tconst','primaryTitle','originalTitle','genres'], encoding='utf-8')
        ])
    akas = pd.read_csv(f'{CURATED_LOCAL_PATH}AKAS_US.csv', usecols=['titleId','title','region','isOriginalTitle'], encoding='utf-8')
    ratings = pd.read_csv(f'{RAW_LOCAL_PATH}title.ratings.tsv', sep = '\t', usecols=['tconst','averageRating'], dtype = {'averageRating':'float16'}, encoding='utf-8')

    akas.rename(columns={'titleId':'tconst'}, inplace = True)
    basics['genres'] = basics['genres'].str.split(pat=',')

    print ('Datasets imported successfully from tsv to pandas dataframes.\n')
 
    return actors, principals, basics, akas, ratings


def req1 (principals, basics, actors):
    '''
    Actors by film
    '''
    print ('Rq 1 - Work in progress...')
    df = basics.loc[:,['tconst','originalTitle']]

    df_rq1= pd.merge(pd.merge(df, principals, on='tconst'), actors, on='nconst').sort_values('tconst')
    df_rq1.to_csv('./results/01_actor_by_film.csv', index=False)

    print ('CSV file "01_actor_by_film" saved in the results folder.\n')

    return df_rq1


def req2 (akas, basics, ratings):
    '''
    Rating US films
    '''
    print ('Rq 2 - Work in progress...')
    us = akas.loc[:, ['tconst','region']]
    us.drop_duplicates(keep='first', inplace=True)
    films = basics.loc[:,['tconst','originalTitle']].set_index('tconst')

    df_rq2 = pd.merge(pd.merge(films, us, on='tconst'), ratings, on='tconst').set_index('tconst').sort_values('averageRating', ascending=False)
    df_rq2.to_csv('./results/02_usfilm_ratings.csv')

    print ('CSV file "02_usfilm_ratings" saved in the results folder.\n')

    return df_rq2


def req3 (basics, ratings):
    '''
    Ratings by genre
    '''
    print ('Rq 3 - Work in progress...')
    df = pd.merge(basics, ratings, on='tconst')
    df.dropna(subset=['genres'], inplace=True)
    df = pd.DataFrame({
            col:np.repeat(df[col].values, df['genres'].str.len())
            for col in df.columns.drop('genres')}
        ).assign(**{'genres':np.concatenate(df['genres'].values)})[df.columns]
    
    df_rq3 = df.groupby('genres')[['averageRating']].mean().sort_values('averageRating', ascending=False)
    df_rq3.to_csv('./results/03_ratings_by_genre.csv')

    print ('CSV file "03_ratings_by_genre" saved in the results folder.\n')

    return df_rq3


def req4 (principals, actors, ratings):
    '''
    Ratings by actor
    '''
    print ('Rq 4 - Work in progress...')
    df = principals.loc[:,['nconst','tconst']]
    df = pd.merge(pd.merge(df, actors, on='nconst'), ratings, on='tconst')

    df_rq4 = df.groupby('primaryName')[['averageRating']].mean().sort_values('averageRating', ascending=False)
    df_rq4.to_csv('./results/04_ratings_by_actor.csv')

    print ('CSV file "04_ratings_by_actor" saved in the results folder.\n')

    return df_rq4


def split_data(TITLE_FILE_NAME, FOLDER, COLUMN):
    '''
    Break raw data into many files
    '''
    filter = {}
    filter['AKAS'] = ('US')
    filter['BASICS'] = ('movie','tvMovie')
    filter['PRINCIPALS'] = ('actor','actress')
    
    print (f'Spliting {FOLDER}...')
    with open(RAW_LOCAL_PATH + TITLE_FILE_NAME, encoding='utf-8') as file_stream:  
        csv.field_size_limit(10000000)  
        file_stream_reader = csv.DictReader(file_stream, delimiter='\t')

        open_files_references = {}

        for row in file_stream_reader:
            column = row[COLUMN]
     
            if column not in filter[FOLDER]:
                continue

            # Open a new file and write the header
            if column not in open_files_references:
                output_file = open(CURATED_LOCAL_PATH + f'{FOLDER}_{column}.csv', 'w', encoding='utf-8', newline='')
                dictionary_writer = csv.DictWriter(output_file, fieldnames=file_stream_reader.fieldnames)
                dictionary_writer.writeheader()
                open_files_references[column] = output_file, dictionary_writer
            # Always write the row
            open_files_references[column][1].writerow(row)
        # Close all the files
        for output_file, _ in open_files_references.values():
            output_file.close()
    
    print ('Done.')