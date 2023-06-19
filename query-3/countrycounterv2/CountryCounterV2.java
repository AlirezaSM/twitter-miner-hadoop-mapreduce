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


public class CountryCounterV2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text numb = new Text();
    private Text cnt = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     
      String line = value.toString();
      String[] arr = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
      String tweet = arr[2];
      String latitude = arr[13];
      String longitude = arr[14];
      String country = countryChecker(latitude, longitude);
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
    
    public static String countryChecker(String lat, String lon) {
            if (lat.equals("lat") || lat.equals("") || lon.equals(""))
                return "other";
            else {
                double latitude = Double.parseDouble(lat);
                double longitude = Double.parseDouble(lon);
                if ((longitude < -68 && longitude > -161.75) && (latitude < 64.85 && latitude > 19.5))
                    return "america";
                else if ((longitude < 9.45 && longitude > -4.65) && (latitude < 51 && latitude > 41.6))
                    return "france";
            }
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
    job.setJarByClass(CountryCounterV2.class);
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
