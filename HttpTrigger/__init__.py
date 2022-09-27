import logging
import numpy as np
import azure.functions as func
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import json
from io import BytesIO


def user(dfs_user_art, x):
    logging.info('---1 -------begin user()')
    user = dfs_user_art.loc[dfs_user_art['user_id'] == x]
    if len(user) > 0:
        logging.info('---1 -------end user()')
        return user
    return np.nan


def calcul_cosine_similarity(arts_embedd_acp_user, art_embed):
    cosine_sim = cosine_similarity(arts_embedd_acp_user, art_embed, dense_output=False)
    return cosine_sim


def arts_recommendations(arts, art_embed, x, arts_embedd_acp_user):
    logging.info('---1 -------begin arts_recommendations')
    titles = arts['article_id']
    indices = pd.Series(range(0,len(titles)), index=titles)
    #indices = pd.Series(range(0,5588), index=arts)
    idx = indices[x]
    logging.info('---1 -------begin cosine')
    cosine_sim = calcul_cosine_similarity(arts_embedd_acp_user, art_embed)
    logging.info('---1 -------end cosine')
    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_scores = sim_scores[1:21]
    #movie_indices = [i[0] for i in sim_scores]
    logging.info('---1 -------end arts_recommendations')
    return sim_scores#x.iloc[movie_indices]


def user_recommendation(dfs_user_art, arts,art_embed, x, arts_embedd_acp_user):
    reco = []
    logging.info('---1 -------begin user_recommendation')
    user_arts = user(dfs_user_art, x)
    for art in user_arts["click_article_id"]:
        livre = arts_recommendations(arts,art_embed, art)
        reco.append(livre[0])
    reco = arts_recommendations(arts,art_embed, art, arts_embedd_acp_user)
    sim_scores = sorted(reco, key=lambda x: x[1], reverse=True)
    sim_scores = sim_scores[1:6]
    logging.info('---1 -------end user_recommendation')
    return sim_scores


def transform_to_dataframe(blob):
    dfs = bytearray(blob.read())
    dfs = pd.read_csv(BytesIO(dfs))
    return dfs

def main(req: func.HttpRequest, dfsblob: func.InputStream) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    #dfs = bytearray(dfsblob.read())
    logging.info("------------------------------------------ok")
    return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
    )
