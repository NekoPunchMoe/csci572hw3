import java.io.IOException;
import java.util.HashSet;
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

  public static class InvertedIndexReducer
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      HashMap<String,Integer> m = new HashMap<String, Integer>();
      for (Text val : values) {
        String temp = val.toString();
       	if (m.containsKey(temp)) {
		m.put(temp, m.get(temp) + 1);
	} else {
		m.put(temp, 1);
	}
      }
      StringBuilder result = new StringBuilder();
      for(Map.Entry<String, Integer> entry : m.entrySet()) {
	if(result.length() != 0) {
		result.append('\t');
	}
	result.append(entry.getKey());
	result.append(":");
	result.append(entry.getValue());
      }
      Text resultText = new Text();
      resultText.set(result.toString());
      context.write(key, resultText);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverted index");
    //Defining the outkey and value class for the mapper.
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setJarByClass(inverted_index.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(InvertedIndexReducer.class);
    //Defining the output value class for the mapper
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
