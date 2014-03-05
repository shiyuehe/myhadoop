package com.shiyuehe.app;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
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

import com.shiyuehe.app.dto.KPI;
import com.shiyuehe.app.util.KPIUtils;

public class KPIPV {

	public static class KPIPVMapper extends MapReduceBase implements Mapper<Object, Text, Text, LongWritable>{
		
		private LongWritable one = new LongWritable(1);
		private Text report = new Text();
		private KPI kpi = null;
		
		@Override
		public void map(Object key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			kpi = KPIUtils.createKPI(value.toString());
			if(kpi!=null){
				report.set(kpi.getRequest());
				output.collect(report, one);
			}
			
		}
		
	}
	
	public static class KPIPVReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>{

		private LongWritable result = new LongWritable();
		
		@Override
		public void reduce(Text key, Iterator values, OutputCollector output,
				Reporter reporter) throws IOException {
			long sum = 0 ;
			while(values.hasNext()){
				LongWritable ret = (LongWritable) values.next();
				sum += ret.get();
			}
			result.set(sum);
			output.collect(key, result);
		}
		
	}
	
	public static void main(String[] args) throws IOException {
			
		String input = "hdfs://master:9000/user/input/log_kpi/";
		String output = "hdfs://master:9000/user/input/log_kpi/pv";
		
		JobConf conf = new JobConf(KPIPV.class);
		conf.setJobName("kpipv");
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		
		conf.setMapperClass(KPIPVMapper.class);
		conf.setCombinerClass(KPIPVReducer.class);
		conf.setReducerClass(KPIPVReducer.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(LongWritable.class);
		
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		JobClient.runJob(conf);
		System.exit(0);
	}

}
