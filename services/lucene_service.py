from subprocess import run, PIPE
from json import loads
from operator import itemgetter
from datetime import datetime

from utils.sentiment_analysis_helper import analyze_json_arr

async def search_by_lucene(query:str):
    res = run([f"java -cp '.:utils/lucene/dependency/*' utils/lucene/Searcher utils/lucene/indexes '{query}'"], stdout=PIPE, stderr=PIPE, shell=True)
    
    if (len(res.stderr) > 0):
        print(res.stderr)

    if (len(res.stdout) == 0):
        return []
    json_arr = loads(res.stdout)
    
    for json in json_arr:
        json["posted_at"] = datetime.strptime(str(json["posted_at"]), "%Y-%m-%d %H:%M:%S").strftime("%d %B, %Y %H:%M:%S")

    return sorted(analyze_json_arr(json_arr), key=itemgetter('rank'))