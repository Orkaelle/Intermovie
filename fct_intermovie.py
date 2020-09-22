import pandas as pd
import numpy as np

def tsv_to_dataset ():
    ''' Création des datasets originaux '''
    actors = pd.read_csv('./movies_dataset/name.basics.tsv', sep = '\t', usecols=['nconst','primaryName'])
    principals = pd.read_csv('./movies_dataset/title.principals.tsv', sep = '\t', usecols=['tconst','nconst','category'])
    basics = pd.read_csv('./movies_dataset/title.basics.tsv', sep = '\t', usecols=['tconst','primaryTitle','originalTitle','genres'])
    akas = pd.read_csv('./movies_dataset/title.akas.tsv', sep = '\t', usecols=['titleId','title','region','isOriginalTitle'])
    ratings = pd.read_csv('./movies_dataset/title.ratings.tsv', sep = '\t', usecols=['tconst','averageRating'], dtype = {'averageRating':'float16'})

    akas.rename(columns={'titleId':'tconst'}, inplace = True)
    basics['genres'] = basics['genres'].str.split(pat=',')
 
    return actors, principals, basics, akas, ratings


def req1 (principals, basics, actors):
    ''' Actors by film '''
    df1 = principals.loc[principals['category'] == 'actor',:]
    df2 = basics.loc[:,['tconst','originalTitle']]

    df_rq1= pd.merge(pd.merge(df1, df2, how='left', on='tconst'), actors, how='left', on='nconst').drop(['category','tconst','nconst'], axis=1)
    df_rq1.to_csv('01_actor_by_film')

    return df_rq1


def req2 (akas, basics, ratings):
    ''' Rating US films '''
    us = akas.loc[(akas['region'] == 'US'), ['tconst','region']]
    us.drop_duplicates(keep='first', inplace=True)
    films = basics.loc[:,['tconst','originalTitle']].set_index('tconst')

    df_rq2 = pd.merge(pd.merge(us, films, how='left', on='tconst'), ratings, how='left', on='tconst').set_index('tconst')
    df_rq2.to_csv('02_usfilm_ratings')

    return df_rq2


def req3 (basics, ratings):
    ''' Ratings by genre '''
    df = pd.merge(basics, ratings, how='left', on='tconst')
    df.dropna(subset=['averageRating'], inplace=True)
    df.dropna(subset=['genres'], inplace=True)
    df = pd.DataFrame({
            col:np.repeat(df[col].values, df['genres'].str.len())
            for col in df.columns.drop('genres')}
        ).assign(**{'genres':np.concatenate(df['genres'].values)})[df.columns]
    
    df_rq3 = df.groupby('genres')['averageRating'].mean()
    df_rq3.to_csv('03_ratings_by_genre')

    return df_rq3