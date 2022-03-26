package utils.lucene;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Indexer {
    private final IndexWriter writer;

    public Indexer(String indexDir) throws IOException {
//        System.out.println(indexDir);
        Directory dir = FSDirectory.open(new File(indexDir));
        writer = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30), true,
                IndexWriter.MaxFieldLength.UNLIMITED);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: java " + Indexer.class.getName() + " <index dir> <data dir>");
        }
        String indexDir = args[0];
        String dataDir = args[1];

        long start = System.currentTimeMillis();
        Indexer indexer = new Indexer(indexDir);
        int numIndexed = -1;
        try {
            numIndexed = indexer.index(dataDir);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.toString());
        }
        finally {
            indexer.close();
        }
        long end = System.currentTimeMillis();

        System.out.println("Indexing " + numIndexed + " tweets took " + (end - start) + " milliseconds");
    }

    public void close() throws IOException {
        writer.close();
    }

    private static List<JSONObject> collectAllJSONs(JSONParser jsonParser, File folder) throws ParseException {

        List<JSONObject> allJSONs = new ArrayList<>();

        int tweetCount = 0;
        for (File eachJson : Objects.requireNonNull(folder.listFiles())) {
            if (eachJson.isFile()) {

                try (FileReader reader = new FileReader(eachJson.getAbsolutePath())) {
                    Object obj = jsonParser.parse(reader);

                    JSONArray tweetsList = (JSONArray) obj;
//                    System.out.println(tweetsList);

//                    System.out.println(eachJson.getName());
                    for (Object tweetObj : tweetsList) {
                        JSONObject tweetJObj = (JSONObject) tweetObj;
                        tweetCount = tweetCount + 1;
                        allJSONs.add(tweetJObj);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Loaded " + tweetCount + " tweets.");
        return allJSONs;
    }

    public int index(String dataPath) throws Exception {

        File dataFile = new File(dataPath);

        JSONParser jsonParser = new JSONParser();
        List<JSONObject> allJSONs = new ArrayList<>();
        allJSONs.addAll(collectAllJSONs(jsonParser, dataFile));

        for (JSONObject tweetJObj : allJSONs) {
            indexEachJSON(tweetJObj);
        }
        return writer.numDocs();
    }

    protected Document getDocument(JSONObject tweetJObj) {

        String userName = (String) tweetJObj.get("user_name");
        String tweet = (String) tweetJObj.get("tweet");
        Long verified = (Long) tweetJObj.get("verified");
        Long followersCount = (Long) tweetJObj.get("followers_count");
        Long tweetsCount = (Long) tweetJObj.get("tweets_count");
        Long retweetCount = (Long) tweetJObj.get("retweet_count");
        Long listedCount = (Long) tweetJObj.get("listed_count");
        Long likeCount = (Long) tweetJObj.get("like_count");

        Document doc = new Document();
        doc.add(new Field("user_name", userName, Field.Store.YES, Field.Index.ANALYZED));
        doc.add(new Field("tweet", tweet, Field.Store.YES, Field.Index.ANALYZED));
        
        doc.add(new Field("tweet_id", String.valueOf(tweetJObj.get("tweet_id")), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("verified", String.valueOf(tweetJObj.get("verified")), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("user_id", String.valueOf(tweetJObj.get("user_id")), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("followers_count", String.valueOf(tweetJObj.get("followers_count")), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("tweets_count", String.valueOf(tweetJObj.get("tweets_count")), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("retweet_count", String.valueOf(tweetJObj.get("retweet_count")), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("listed_count", String.valueOf(tweetJObj.get("listed_count")), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("like_count", String.valueOf(tweetJObj.get("like_count")), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("posted_at", String.valueOf(tweetJObj.get("posted_at")), Field.Store.YES, Field.Index.NOT_ANALYZED));

        doc.setBoost(verified == 1 ? 1.5F : 0.7F);
        doc.setBoost(followersCount >= 1000 ? 1.5F : 1F);
        doc.setBoost(tweetsCount > 1000 ? 1.5F : 1F);
        doc.setBoost(retweetCount > 1000 ? 1.5F : 1F);
        doc.setBoost(listedCount > 1000 ? 1.5F : 1F);
        doc.setBoost(likeCount > 1000 ? 1.5F : 1F);

        return doc;
    }

    private void indexEachJSON(JSONObject tweetJObj) throws Exception {
        Document doc = getDocument(tweetJObj);
        writer.addDocument(doc);
    }


}
