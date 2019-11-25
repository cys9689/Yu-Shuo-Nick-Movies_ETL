#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
import re
from sqlalchemy import create_engine
import psycopg2
from config import dp_password
import time


# In[2]:


#file Direction and file rename for function
file_dir='the-movies-dataset/'
file_wiki='wikipedia-movies.json'
file_movies='movies_metadata.csv'
file_ratings='ratings.csv'


# In[9]:


def challenge(wikipedia_data,kaggle_meta,ratings):
    with open(f'{file_dir}/{wikipedia_data}',mode='r') as file:
        wiki_movies_raw=json.load(file)
    kaggle_metadata=pd.read_csv(f'{file_dir}/{kaggle_meta}')
    ratings=pd.read_csv(f'{file_dir}/{ratings}')
    print('file loaded')
    
    # data cleaning
    # filtering the data with "director" or " directed by " and excluded the no. of episodes series
    wiki_movies=[movie for movie in wiki_movies_raw if ('Director' in movie or 'Directed by' in movie) 
             and 'imdb_link' in movie
             and 'No. of epsiodes' not in movie]
    # wiki_movies data frame creation
    wiki_movies_df=pd.DataFrame(wiki_movies)
    print('wiki movies data frame created')
# nest function of movie data cleaning( substitute all the alternative titles into one column)
    def clean_movie(movie):
        movie = dict(movie) #create a non-destructive copy
        alt_titles = {}
    # combine alternate titles into one list
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune-Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles

    # merge column names, change the name
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

        return movie

    #Running function for clean movies data; change name and combine all the alternate title into a list
    clean_movies=[clean_movie(movie)for movie in wiki_movies]
    wiki_movies_df=pd.DataFrame(clean_movies)
    #Extract the Imdb link by regular expression
    wiki_movies_df['imdb_id']=wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset='imdb_id',inplace=True)
    
    #keep the column has less than 90% null value
    wiki_columns_to_keep=[column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum()< len(wiki_movies_df)*0.9]
    wiki_movies_df=wiki_movies_df[wiki_columns_to_keep]
    print('Wiki movies null valued cleaned')
    
    box_office=wiki_movies_df['Box office'].dropna()
    box_office=box_office.apply(lambda x: ''.join(x) if type(x)==list else x)
    budget=wiki_movies_df['Budget'].dropna()
    budget=budget.map(lambda x:''.join(x) if type(x)==list else x)
    
    # makes a list for each columns
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    running_time=wiki_movies_df['Running time'].dropna().apply(lambda x:''.join(x) if type(x)==list else x)
    
    
    #regular expreission form for budget columns
    #new form one and two to include the rest form
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    
    
    #regular expression for months
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    
    # String conversion
    box_office=box_office.str.replace(r'\$.*[---]($![a-z])','$',regex=True)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    budget=budget.str.replace(r'\[d+\]\s*','')
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

            # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub('\$|,','', s)

            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan
        
        
        #Extract the value 
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    
    #old column drop from the dataframe
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    wiki_movies_df.drop('Budget',axis=1,inplace=True)
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    wiki_movies_df.drop('Release date', axis=1, inplace=True)
    
    print('wiki data cleaning completion')
    
    
    #2.Clean kaggle data
    # drop the adult column and keep rows where the adult column is False
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    
    #create a boolean column, choose only True and goes into the metadata file
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    
    #to.numeric columns
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')

    #datetime conversion
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    ratings['timestamp']=pd.to_datetime(ratings['timestamp'],unit='s')
    # kaggle Data cleaned
    
    # Data merged
    # redundant (common)column
    movies_df=pd.merge(wiki_movies_df,kaggle_metadata,on='imdb_id',suffixes=['_wiki','_kaggle'])
    
    #try-except: drop rows 
    try:
        movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index
    except:
        print('no rows to drop')
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
    
    
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
        df.drop(columns=wiki_column, inplace=True)
    
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    
    try:
        for col in movies_df.columns:
            lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
            value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
            num_values = len(value_counts)
        if num_values == 1:
            movies_df.drop(col,axis=1,inplace=True)
            
    except:
        print('No columns to drop')
    
    # change the order of the column
    movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]
    # Renaming the column
    movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)
    
    print('Merge done between kaggle and wiki---> movies_df')
    
    
    # transform and merge for ratings
    
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count().rename({'userId':'count'},axis=1)    .pivot(index='movieId',columns='rating',values='count')
    
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    
    
    print('Files transformed')
    
    
    #SQL tables upload
    
    db_string=f"postgres://postgres:{dp_password}@127.0.0.1:5432/movie_data"
    engine=create_engine(db_string)
    #data deletion, keep the table
    try:
        connection=psycopg2.connect(db_string)
        cursor=connection.cursor()
        sql_delete_query("DELETE FROM movies")
        cursor.execute(sql_delete_query)
        connection.commit()
        count=cursor.rowcount
        print(count,"of row removed from movies")
    except(Exception,psycopg2.Error) as error:
        print("Record Deleted successfully")
        
    try:
        connection=psycopg2.connect(db_string)
        cursor=connection.cursor()
        sql_delete_query("DELETE FROM ratings")
        cursor.execute(sql_delete_query)
        connection.commit()
        count=cursor.rowcount
        print(count,"of row removed from movies")
    except(Exception,psycopg2.Error) as error:
        print("Record Deleted successfully")
        
    print('Data deleted, Table kept')
    
    
    
    movies_df.to_sql(name='movies',con=engine,if_exists='append')
    row_imported=0
    start_time=time.time()
    for data in pd.read_csv(f'{file_dir}/ratings.csv',chunksize=1000000):
        print(f'importing rows {row_imported} to {row_imported + len(data)}...',end='')
        data.to_sql(name='ratings',con=engine,if_exists='append')
        row_imported += len(data)
        print(f'Done.{time.time()-start_time} total seconds elapsed')
        
    print('file has updated')
    

        
        


# In[10]:


challenge(file_wiki,file_movies,file_ratings)


# In[ ]:




