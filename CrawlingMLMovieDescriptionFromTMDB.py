import numpy as np
import pandas as pd
import datetime
import sys
import time
import requests
import multiprocessing

def GetMLMovieDescription(ML_data_path, save_path):
    df_ML = pd.read_csv(ML_data_path + "ml-25m/ratings.csv")
    df_ML["time"] = pd.to_datetime(df_ML["timestamp"], unit="s")
    df_ML["time"] = df_ML.time.dt.date
    df_ML = df_ML.loc[df_ML.rating >= 5]
    df_link = pd.read_csv(ML_data_path + "ml-25m/links.csv")

    dict_ml2tmdb = dict(zip(df_link.movieId.values, df_link.tmdbId.values))
    movie_list = list(set(df_ML.movieId.unique()) & set(df_link.movieId.unique()))
    index_interval = len(movie_list) // 20 + 1

        def getdescription(movie_list, index):
            dict_Id2des = {}
            for movie in movie_list:
                try:
                    tmdbid = int(dict_ml2tmdb[movie])
                    url = 'https://api.themoviedb.org/3/movie/'+str(tmdbid)+'?api_key=9f8cdf447cb90a033534cf240d33fb48'
                    response = requests.get(url)
                    description = response.json()["overview"]
                    dict_Id2des[movie] = description
                except:
                    1
            np.save(save_path + f"Dict_MovieId2description_{index}.npy", dict_Id2des)

    for j in range(20):
        sub_list = movie_list[j*index_interval:index_interval*(j+1)]

        p = multiprocessing.Process(target=getdescription, args=(sub_list, j))
        p.start()
    p.join()

def MergeMLMovieDescription(save_path):
    dict_id2desc = {}
    for i in range(20):
        path = save_path + f"./Dict_MovieId2description_{i}.npy"
        current_dict = np.load(path, allow_pickle=True).item()
        dict_id2desc = {**dict_id2desc, **current_dict} 
    np.save(save_path + "Dict_MovieId2description.npy", dict_id2desc)