from fastapi import APIRouter

from db.session import query_hadoop_indexes_collection, hadoop_entity_to_model, query_tweets_collection, tweets_entity_to_model

test_router = APIRouter()

@test_router.get("/test")
async def test():
    find_cursor = await query_hadoop_indexes_collection({"word": "aakashgupta"}, [], 10)
    l = []
    for e in await find_cursor.to_list(10):
        l.append(hadoop_entity_to_model(e))

    find_cursor = await query_tweets_collection({}, [('posted_at', -1)], 2)
    for e in await find_cursor.to_list(10):
        l.append(tweets_entity_to_model(e))
    return l