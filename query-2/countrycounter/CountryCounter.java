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


public class CountryCounter {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text numb = new Text();
    private Text cnt = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     
      String line = value.toString();
      String[] arr = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
      String tweet = arr[2];
      String country = countryChecker(arr[16]);
      cnt.set(country);
      //System.out.println("=====================================================================================================================");
      //System.out.println(country);
      if (!country.equals("other")) {
			if ((tweet.contains("#DonaldTrump") || tweet.contains("#Trump")) && (tweet.contains("#JoeBiden") || tweet.contains("#Biden"))) {
                numb.set("1.0,0.0,0.0,1");
                context.write(cnt, numb);
            } else if (tweet.contains("#DonaldTrump") || tweet.contains("#Trump")) {
                numb.set("0.0,0.0,1.0,1");
				context.write(cnt, numb);
            } else if (tweet.contains("#JoeBiden") || tweet.contains("#Biden")) {
                numb.set("0.0,1.0,0.0,1");
                context.write(cnt, numb);
            }
        }
      //System.out.println("=====================================================================================================================");
    }
    
    public static String countryChecker(String str) {
        str = str.toLowerCase();
        if (str.contains("america"))
            return "america";
        else if (str.contains("iran"))
            return "iran";
        else if (str.contains("netherlands"))
            return "netherlands";
        else if (str.contains("austria"))
            return "austria";
        else if (str.contains("mexico"))
            return "mexico";
        else if (str.contains("emirates"))
            return "emirates";
        else if (str.contains("france"))
            return "france";
        else if (str.contains("germany"))
            return "germany";
        else if (str.contains("england"))
            return "england";
        else if (str.contains("canada"))
            return "canada";
        else if (str.contains("spain"))
            return "spain";
        else if (str.contains("italy"))
            return "italy";

        return "other";
    }
    
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text();
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
                       
      //System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
      //System.out.println(key.toString());
      
      double sum_both = 0;
      double sum_trump = 0;
      double sum_biden = 0;
      int sum_country = 0;
      for (Text val : values) {
        String v = val.toString();
        //System.out.println(v);
        String[] varr = v.split(",");
        int temp = Integer.parseInt(varr[3]);
        sum_both += Double.parseDouble(varr[0]) * temp;
        sum_biden += Double.parseDouble(varr[1]) * temp;
        sum_trump += Double.parseDouble(varr[2]) * temp;
        sum_country += temp;
      }
      String out = sum_both/sum_country + "," + sum_biden/sum_country + "," + sum_trump/sum_country + "," + sum_country;
      //System.out.println("output: " + out);
      result.set(out);
      context.write(key, result);
      //System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(CountryCounter.class);
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
