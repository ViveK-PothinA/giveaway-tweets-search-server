package utils.hadoop;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopIDF {

    public static class MapWordToDoc
            extends Mapper<Object, Text, Text, Text>{
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String documentID = value.toString().substring(0, 
                    value.toString().indexOf("\t"));
            String string =  value.toString()
                    .substring(value.toString().indexOf("\t") + 1);
            StringTokenizer tokenizer = new StringTokenizer(string, " '-");
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken()
                    .replaceAll("[^A-Za-z0-9#@ ]", "").toLowerCase());
                if(word.toString() != "" && !word.toString().isEmpty()){
                    context.write(word, new Text(documentID));
                }
            }
        }
    }
    public static class ReduceDocumentWithWords
            extends Reducer<Text,Text,Text,Text> {
        
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap<String,Integer> wordCountMap = new HashMap<>();
            for (Text val : values) {
                if (!wordCountMap.containsKey(val.toString())) {
                    wordCountMap.put(val.toString(), 1);
                } else {
                    wordCountMap.put(val.toString(), 
                        wordCountMap.get(val.toString()) + 1);
                }
            }
            StringBuilder docValueList = new StringBuilder();
            for(String docID : wordCountMap.keySet()){
                docValueList.append(docID+":"+wordCountMap.get(docID)+"\t");
            }
            context.write(key, new Text(docValueList.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "HadoopIDF");
        job.setJarByClass(HadoopIDF.class);
        job.setMapperClass(MapWordToDoc.class);
        job.setReducerClass(ReduceDocumentWithWords.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}