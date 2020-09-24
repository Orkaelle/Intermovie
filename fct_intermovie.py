import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os


def check_folder ():
    folder = './results'
    if not os.path.exists(folder):
        os.mkdir(folder)


def dl_datasets ():
    url = 'https://stdatalake005.blob.core.windows.net/public/movies_dataset.zip'

    print ('Downloading...')
    r = requests.get(url)
    print ('Extracting...')
    z = zipfile.ZipFile(io.BytesIO(r.content))

    z.extractall('./movies_dataset/')

    print ('Dataset downloaded and extracted successfully.\n')


def tsv_to_dataset ():
    ''' Création des datasets originaux '''
    print ('Converting tsv files...')
    actors = pd.read_csv('./movies_dataset/name.basics.tsv', sep = '\t', usecols=['nconst','primaryName'], encoding='utf-8')
    principals = pd.read_csv('./movies_dataset/title.principals.tsv', sep = '\t', usecols=['tconst','nconst','category'], encoding='utf-8')
    basics = pd.read_csv('./movies_dataset/title.basics.tsv', sep = '\t', usecols=['tconst','primaryTitle','originalTitle','genres'], encoding='utf-8')
    akas = pd.read_csv('./movies_dataset/title.akas.tsv', sep = '\t', usecols=['titleId','title','region','isOriginalTitle'], encoding='utf-8')
    ratings = pd.read_csv('./movies_dataset/title.ratings.tsv', sep = '\t', usecols=['tconst','averageRating'], dtype = {'averageRating':'float16'}, encoding='utf-8')

    akas.rename(columns={'titleId':'tconst'}, inplace = True)
    basics['genres'] = basics['genres'].str.split(pat=',')

    print ('Dataset imported successfully from tsv.\n')
 
    return actors, principals, basics, akas, ratings


def req1 (principals, basics, actors):
    ''' Actors by film '''
    print ('Work in progress...')
    df1 = principals.loc[principals['category'] == 'actor',:]
    df2 = basics.loc[:,['tconst','originalTitle']]

    df_rq1= pd.merge(pd.merge(df1, df2, how='left', on='tconst'), actors, how='left', on='nconst').drop(['category','tconst','nconst'], axis=1)
    df_rq1.to_csv('./results/01_actor_by_film.csv', index=False)

    print ('CSV file "01_actor_by_film" saved in the results folder.\n')

    return df_rq1


def req2 (akas, basics, ratings):
    ''' Rating US films '''
    print ('Work in progress...')
    us = akas.loc[(akas['region'] == 'US'), ['tconst','region']]
    us.drop_duplicates(keep='first', inplace=True)
    films = basics.loc[:,['tconst','originalTitle']].set_index('tconst')

    df_rq2 = pd.merge(pd.merge(us, films, how='left', on='tconst'), ratings, how='left', on='tconst').set_index(['tconst']).drop(['region'], axis=1)
    df_rq2.to_csv('./results/02_usfilm_ratings.csv')

    print ('CSV file "02_usfilm_ratings" saved in the results folder.\n')

    return df_rq2


def req3 (basics, ratings):
    ''' Ratings by genre '''
    print ('Work in progress...')
    df = pd.merge(basics, ratings, how='left', on='tconst')
    df.dropna(subset=['averageRating'], inplace=True)
    df.dropna(subset=['genres'], inplace=True)
    df = pd.DataFrame({
            col:np.repeat(df[col].values, df['genres'].str.len())
            for col in df.columns.drop('genres')}
        ).assign(**{'genres':np.concatenate(df['genres'].values)})[df.columns]
    
    df_rq3 = df.groupby('genres')[['averageRating']].mean()
    df_rq3.to_csv('./results/03_ratings_by_genre.csv')

    print ('CSV file "03_ratings_by_genre" saved in the results folder.\n')

    return df_rq3


def req4 (principals, actors, ratings):
    ''' Ratings by actor '''
    print ('Work in progress...')
    df = principals.loc[principals['category']=='actor',['nconst','tconst']].sort_values('nconst')
    df = pd.merge(pd.merge(df, actors, how='left', on='nconst'), ratings, how='left', on='tconst')
    df.dropna(subset=['averageRating'], inplace = True)

    df_rq4 = df.groupby('primaryName')['averageRating'].mean()
    df_rq4.to_csv('./results/04_ratings_by_actor.csv')

    print ('CSV file "04_ratings_by_actor" saved in the results folder.\n')

    return df_rq4