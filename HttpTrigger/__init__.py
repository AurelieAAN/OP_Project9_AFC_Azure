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

def main(req: func.HttpRequest, dfsblob: func.InputStream, blobembed: func.InputStream) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    dfs = transform_to_dataframe(dfsblob)
    dfs_user_art = dfs.groupby(["user_id", "click_article_id"])["click_article_id"].count().reset_index(name="nb_click_by_arts")
    df_arts_embedd_acp = transform_to_dataframe(blobembed)
    arts = dfs["click_article_id"].value_counts().index
    req_body_bytes = req.get_body()
    req_body = req_body_bytes.decode("utf-8")
    # logging.info("test1")
    json_body = json.loads(req_body)
    name = None
    name = json_body['id_user']
    logging.info(f"Request name: {name}")
    logging.info("------------------------------------------ok")
    if name is not None:
        name = int(name)
        user_arts = user(0)
        test = pd.DataFrame(user_arts["click_article_id"])
        test.columns= ["article_id"]
        test2 = df_arts_embedd_acp.merge(test, how='inner', on='article_id')
        arts_embedd_acp_user = test2[[ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10',
       '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22',
       '23', '24', '25', '26', '27']].to_numpy(dtype = np.float32)
        arts_embedd_acp = df_arts_embedd_acp[['0', '1', '2', '3', '4', '5', '6', '7','8', '9', '10',
       '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22',
       '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34',
       '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46',
       '47', '48', '49', '50', '51']].to_numpy(dtype=np.float32)
        result = user_recommendation(dfs_user_art,user_arts,arts_embedd_acp,name, arts_embedd_acp_user)
        result = result.to_json(orient="split")
        #func.HttpResponse.mimetype = 'application/json'
        func.HttpResponse.charset = 'utf-8'
        return func.HttpResponse(
                json.dumps(result),
                status_code=200
                )
    else:
        return func.HttpResponse(
              "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
              status_code=200
        )
