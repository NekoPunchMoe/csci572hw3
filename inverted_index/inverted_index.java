import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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

public class inverted_index {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] str = value.toString().split("\t");
      StringTokenizer itr = new StringTokenizer(str[1]);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, new Text(str[0]));
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      HashMap<String, Integer> map = new HashMap<String, Integer>();
      for (Text val : values) {
        String temp = val.toString();
        if (map.containsKey(temp)) {
        	map.put(temp, map.get(temp) + 1);
        } else {
        	map.put(temp, 1);
        }
      }
      StringBuilder result = new StringBuilder();
      for(Map.Entry<String, Integer> entry : map.entrySet()) {
    	  result.append(entry.getKey());
    	  result.append(":");
    	  result.append(entry.getValue());
	  result.append(" ");
      }
      context.write(key, new Text(result.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverted index");
    job.setJarByClass(inverted_index.class);
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
