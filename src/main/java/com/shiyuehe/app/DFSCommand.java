package com.shiyuehe.app;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class DFSCommand {

	private static final String HDFS= "hdfs://master:9000/";
	private String hdfsPath;
	private Configuration conf;
	
	public DFSCommand(Configuration conf){
		this(HDFS,conf);
	}
	
	public DFSCommand(String hdfs, Configuration conf) {
		this.hdfsPath = hdfs;
		this.conf = conf;
	}

	public static JobConf getConf(){
		JobConf conf = new JobConf();
		conf.setJobName("hdfscommand");
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");
		return conf;
	}
	
	public static void main(String[] args) throws IOException {
		JobConf conf = getConf();
		DFSCommand hDfsCommand =  new DFSCommand(conf);
		
		hDfsCommand.mkdirs("/user/tmp/one");
		hDfsCommand.ls("/user");
	}

	public void mkdirs(String folder) throws IOException {
		Path path = new Path( folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		if(!fs.exists(path)){
			fs.mkdirs(path);
			System.out.println("create: " + folder);
		}
	}
	
	public void ls(String folder) throws IOException{
		Path path = new Path( folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		FileStatus[] list = fs.listStatus(path);
		System.out.println("ls: " + folder);
		System.out.println("========================================================");
		for (FileStatus fileStatus : list) {
			System.out.printf("name: %s, folder: %s,size: %d\n", fileStatus.getPath(),fileStatus.isDir(),fileStatus.getLen());
		}
		System.out.println("========================================================");
	}

}
