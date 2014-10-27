package oculus.memex.cloud.test;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.sqoop.lib.RecordParser.ParseError;

public class AdTest {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		@SuppressWarnings("deprecation")
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
//			String line = value.toString();
			ads ad = new ads();
			try {
				ad.parse(value);
			} catch (ParseError e) {
				e.printStackTrace();
			}
			Timestamp time = ad.get_posttime();
			word.set((time==null)?"null":(time.getYear() + "/" + (time.getMonth()+1) + "/" + time.getDate()));
			output.collect(word,one);
//			StringTokenizer tokenizer = new StringTokenizer(line);
//			while (tokenizer.hasMoreTokens()) {
//				word.set(tokenizer.nextToken());
//				output.collect(word, one);
//			}
		}
	}
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(AdTest.class);
		conf.setJobName("adtest");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("/user/eric/ads"));
		FileOutputFormat.setOutputPath(conf, new Path("/user/eric/timeoutput"));
		JobClient.runJob(conf);
	}
}