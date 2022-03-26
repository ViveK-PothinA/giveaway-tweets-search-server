from fastapi import APIRouter
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import datetime

from utils.sentiment_analysis_helper import analyze_json_arr
from services.hadoop_service import search_by_hadoop_indexes
from services.lucene_service import search_by_lucene


search_router = APIRouter()

@search_router.get("/search/hadoop/{query}")
async def search_hadoop(query: str, q: Optional[str] = None):
    start = datetime.now()
    
    query_list = []
    if (len(query.split(' ')) > 1):
        query_list = query.lower().split(' ')
    else:
        query_list.append(query)

    arr = await search_by_hadoop_indexes(query_list)
    arr = analyze_json_arr(arr)

    return JSONResponse(content={'search_time': (datetime.now() - start).total_seconds(), 'search_results': arr})


@search_router.get("/search/lucene/{query}")
async def search_lucene(query: str, q: Optional[str] = None):
    start = datetime.now()
    arr = await search_by_lucene(query)

    return JSONResponse(content={'search_time': (datetime.now() - start).total_seconds(), 'search_results': arr})
    

