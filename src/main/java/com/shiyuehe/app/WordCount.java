package com.shiyuehe.app;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class WordCount {

	public static class WordCountMapper extends MapReduceBase implements Mapper<Object,Text,Text,IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		@Override
		public void map(Object arg0, Text arg1,
				OutputCollector<Text, IntWritable> arg2, Reporter arg3)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(arg1.toString());
			while(itr.hasMoreTokens()){
				word.set(itr.nextToken());
				arg2.collect(word, one);
			}
			
		}
	}
	
	public static class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

		private IntWritable result = new IntWritable();
				
		
		@Override
		public void reduce(Text arg0, Iterator<IntWritable> arg1,
				OutputCollector<Text, IntWritable> arg2, Reporter arg3)
				throws IOException {
			int sum = 0;
			while(arg1.hasNext()){
				sum += arg1.next().get();
			}
			result.set(sum);
			arg2.collect(arg0, result);
			
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		 String input = "hdfs://master:9000/user/input/data*";
		 String output = "hdfs://master:9000/user/dev/output1";
		 
		 JobConf conf = new JobConf(WordCount.class);
		 conf.setJobName("WordCount");
		 conf.addResource("classpath:/hadoop/core-site.xml");
		 conf.addResource("classpath:/hadoop/hdfs-site.xml");
		 conf.addResource("classpath:/hadoop/mapred-site.xml");
		 
		 conf.setOutputKeyClass(Text.class);
		 conf.setOutputValueClass(IntWritable.class);
		 
		 conf.setMapperClass(WordCountMapper.class);
		 conf.setCombinerClass(WordCountReducer.class);
		 conf.setReducerClass(WordCountReducer.class);
		 
		 FileInputFormat.setInputPaths(conf, input);
		 FileOutputFormat.setOutputPath(conf, new Path(output));
		 
		 JobClient.runJob(conf);
		 System.exit(0);
		 
	 }
	
}
