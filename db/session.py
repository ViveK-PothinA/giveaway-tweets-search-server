from os import getenv
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient

# from config import settings

class DataBase:
    tweets_mongo_client: AsyncIOMotorClient = None
    hadoop_indexes_mongo_client: AsyncIOMotorClient = None

    def __init__(self):
        # print(getenv("TWEETS_MONGO_URI"), type(getenv("TWEETS_MONGO_URI")))
        self.tweets_mongo_client = AsyncIOMotorClient(getenv("TWEETS_MONGO_URI"))
        self.hadoop_indexes_mongo_client = AsyncIOMotorClient(getenv("HADOOP_INDEXES_MONGO_URI"))


db = DataBase()

async def query_mongo(client: AsyncIOMotorClient, database:str, collection:str, find_query: dict, sort_query: list, limit: int):
    if (sort_query == None or len(sort_query) == 0):
        return client[database][collection].find(find_query).limit(limit) 
    return client[database][collection].find(find_query).sort(sort_query).limit(limit)

async def query_hadoop_indexes_collection(find_query: dict, sort_query: list, limit: int):
    return await query_mongo(db.hadoop_indexes_mongo_client, getenv("HADOOP_INDEXES_DB", "index"), getenv("HADOOP_INDEXES_COLLECTION", "hadoop-indexes"), find_query, sort_query, limit)

async def query_tweets_collection(find_query: dict, sort_query: list, limit: int):
    return await query_mongo(db.tweets_mongo_client, getenv("TWEETS_DB", "tweets"), getenv("TWEETS_COLLECTION", "tweets_collection"), find_query, sort_query, limit)

def hadoop_entity_to_model(entity) -> dict:
    return {
        "word": entity["word"],
        "idf": entity["idf"]
    }

def tweets_entity_to_model(entity) -> dict:
    return {
        "id": str(entity["_id"]),
        'tweet_id': str(entity["tweet_id"]),
        'user_id': str(entity["user_id"]),
        'tweet': str(entity["tweet"]),
        'user_name': str(entity["user_name"]),
        'processed_tweet': str(entity["processed_tweet"]),
        'posted_at': datetime.strptime(str(entity["posted_at"]), "%Y-%m-%d %H:%M:%S").strftime("%d %B, %Y %H:%M:%S"),
        'followers_count': int(entity['followers_count']),
        'retweet_count': int(entity['retweet_count']),
        'tweets_count': int(entity['tweets_count']),
        'listed_count': int(entity['listed_count']),
        'like_count': int(entity['like_count']),
        'verified': int(entity['verified']),
        }