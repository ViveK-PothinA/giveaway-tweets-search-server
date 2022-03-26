from utils.custom_exception import CustomException
from db.session import query_hadoop_indexes_collection, hadoop_entity_to_model, query_tweets_collection, tweets_entity_to_model

async def search_by_hadoop_indexes(query_list: list[str]) -> list[dict]:
    if ("and" not in query_list) and ("or" not in query_list):
        return await hadoop_handle_op(query_list, 'and')

    words = query_list[0::2]
    op = set(query_list[1::2])

    if (len(op) == 1):
        return await hadoop_handle_op(words, list(op)[0])
    else:
        raise CustomException(message="Either 'AND' or 'OR' operations are supported, both can't be used in the query.")

async def hadoop_handle_op(words: list[str], op: str):

    ids_set = set()
    obj_id_to_idf_map = {}

    for word in words:
        word_idf = await find_word(word)
        for idf in word_idf['idf']:
            obj_id_to_idf_map[idf['tweet_obj_id']] = idf['count']

        new_ids_set = set([int(idf['tweet_obj_id']) for idf in word_idf['idf']])

        if (len(ids_set) > 0):
            if (op == 'and'):
                temp = ids_set.intersection(new_ids_set)
                if (len(temp) == 0):
                    temp = set(ids_set)
                ids_set = set(temp)
            elif (op == 'or'):
                ids_set = set(ids_set.union(new_ids_set))
        else:
            ids_set = set(new_ids_set)
    return await generate_search_results(list(ids_set), obj_id_to_idf_map)

async def find_word(word: str) -> dict:
    find_cursor = await query_hadoop_indexes_collection({"word": word}, [], 1)
    l = []
    for e in await find_cursor.to_list(1):
        l.append(hadoop_entity_to_model(e))
    return l[0] if len(l) > 0 else None

async def generate_search_results(ids: list[int], obj_id_to_idf_map: dict, max_results: int = 20) -> list:

    tweet_list = await fetch_tweets_list(ids, max_results)

    for tweet in tweet_list:
        obj_id = tweet['id']
        if (obj_id in obj_id_to_idf_map.keys()):
            idf_count = obj_id_to_idf_map[obj_id]
            tweet['idf_count'] = int(idf_count)

    res_list = rank_tweet_list(tweet_list)
    res_list = calc_search_score(res_list[:max_results])

    return res_list

async def fetch_tweets_list(ids: list[int], max_results:int) -> list:
    find_cursor = await query_tweets_collection({"_id": {"$in" : ids}}, [('posted_at', -1)], max_results)

    tweet_list = []
    for entity in await find_cursor.to_list(length=max_results):
        tweet_json = tweets_entity_to_model(entity)
        tweet_list.append(tweet_json)

    return tweet_list

def calc_search_score(res_list: list) -> list:
    max_score = float('-inf')
    for tweet in res_list:
        score_sum = tweet['verified'] + tweet['followers_count'] + tweet['tweets_count'] 
        + tweet['like_count'] + tweet['retweet_count'] + tweet['listed_count'], -tweet['idf_count']   

        if (score_sum > max_score):
            max_score = score_sum
        tweet['score_sum'] = score_sum

    for tweet in res_list:
        tweet['score'] = f"{round(tweet['score_sum'] / max_score, 7):.7f}"

    return res_list

def rank_tweet_list(tweet_list: list) -> list:
    return sorted(tweet_list, key=lambda tweet: (-tweet['verified'], 
            -tweet['followers_count'], -tweet['tweets_count'], -tweet['like_count'], 
            -tweet['retweet_count'], -tweet['listed_count'], -tweet['idf_count']))