package utils.lucene;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.IOException;

public class Searcher {
    public static void main(String[] args) throws IllegalArgumentException,
            IOException, ParseException {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: java " + Searcher.class.getName()
                    + " <index dir> <query>");
        }

        String indexDir = args[0];
        String q = args[1];

        search(indexDir, q);
    }
    public static void search(String indexDir, String q)
            throws IOException, ParseException {

        Directory directory = FSDirectory.open(new File(indexDir));
        IndexSearcher indexSearcher = new IndexSearcher(directory);

        QueryParser parser = new QueryParser(Version.LUCENE_30,
                "tweet",
                new StandardAnalyzer(
                        Version.LUCENE_30));
        Query query = parser.parse(q);
        long start = System.currentTimeMillis();
        TopDocs hits = indexSearcher.search(query, 20);
        long end = System.currentTimeMillis();

        System.err.println("Found " + hits.totalHits + 
                " document(s) (in " + (end - start) +
                " milliseconds) that matched query '" +
                q + "':");

        JSONArray arr = new JSONArray();
        for(int rank = 0; rank < hits.scoreDocs.length; ++rank) {
            Document hitDoc = indexSearcher.doc(hits.scoreDocs[rank].doc);
            JSONObject res = new JSONObject();
            res.put("rank", rank + 1);
            res.put("score", hits.scoreDocs[rank].score);
            res.put("tweet_id", hitDoc.get("tweet_id"));
            res.put("user_id", hitDoc.get("user_id"));
            res.put("user_name", hitDoc.get("user_name"));
            res.put("tweet", hitDoc.get("tweet"));
            res.put("verified", hitDoc.get("verified"));
            res.put("followers_count", hitDoc.get("followers_count"));
            res.put("tweets_count", hitDoc.get("tweets_count"));
            res.put("retweet_count", hitDoc.get("retweet_count"));
            res.put("listed_count", hitDoc.get("listed_count"));
            res.put("like_count", hitDoc.get("like_count"));
            res.put("posted_at", hitDoc.get("posted_at"));
            arr.add(res);
        }
        System.out.println(arr.toJSONString());
        indexSearcher.close();
        directory.close();
    }
}
