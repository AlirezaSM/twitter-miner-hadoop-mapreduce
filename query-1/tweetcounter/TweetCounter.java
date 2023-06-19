import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TweetCounter {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text name = new Text();
    private Text numb = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     
      String line = value.toString();
      String[] arr = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
      String tweet = arr[2];
      String like = arr[3];
      String retweet = arr[4];
      
      if ((tweet.contains("#DonaldTrump") || tweet.contains("#Trump")) && (tweet.contains("#JoeBiden") || tweet.contains("#Biden"))) {
      		name.set("DonaldTrump,JoeBiden");
      		numb.set((int)Double.parseDouble(like) + "," + (int)Double.parseDouble(retweet));
      		context.write(name, numb);
      } 
      else if (tweet.contains("#DonaldTrump") || tweet.contains("#Trump")) {
      		name.set("DonaldTrump");
      		numb.set((int)Double.parseDouble(like) + "," + (int)Double.parseDouble(retweet));
      		context.write(name, numb);
      } 
      else if (tweet.contains("#JoeBiden") || tweet.contains("#Biden")) {
            	name.set("JoeBiden");
            	numb.set((int)Double.parseDouble(like) + "," + (int)Double.parseDouble(retweet));
      		context.write(name, numb);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text();
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
                       
      int sum_like = 0;
      int sum_retweet = 0;
      for (Text val : values) {
        String v = val.toString();
        String[] varr = v.split(",");
        int like = Integer.parseInt(varr[0]);
        int retweet = Integer.parseInt(varr[1]);
        sum_like += like;
        sum_retweet += retweet;
      }
      result.set(sum_like + "," + sum_retweet);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(TweetCounter.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
