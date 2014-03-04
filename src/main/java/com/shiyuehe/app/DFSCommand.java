package com.shiyuehe.app;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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
		
		hDfsCommand.touchz("/user/input/data3", "hadoop\nhadoop\ntest\n");
		hDfsCommand.cat("/user/input/data3");
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

	
	public void rmr(String folder) throws IOException{
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		if( fs.deleteOnExit(path) ){
			System.out.println("delete: " + folder);
		}else{
			System.out.println("didn't deleted:" + folder);
		}
		fs.close();
	}
	
	public void copyFromLocal(String local,String remote) throws IOException{
		FileSystem fs  = FileSystem.get(URI.create(hdfsPath), conf);
		fs.copyFromLocalFile(new Path(local), new Path(remote));
		System.out.println("copy from local:" + local + " to " + remote);
		fs.close();
	}

	public void copyToLocal(String remote,String local) throws IOException{
		FileSystem fs  = FileSystem.get(URI.create(hdfsPath), conf);
		fs.copyToLocalFile(new Path(remote), new Path(local));
		System.out.println("copy to local:" + remote + " from " + local );
		fs.close();
	}
	
	public void cat(String remoteFile) throws IOException{
		Path path = new Path(remoteFile);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		FSDataInputStream fsdis = null;
		System.out.println("cat: " + remoteFile);
		
		try {
			fsdis = fs.open(path);
			System.out.println("========================================================");
			IOUtils.copyBytes(fsdis, System.out, 4096,false);
			System.out.println("========================================================");
			
		} catch (Exception e) {
			// TODO: handle exception
		}finally{
			IOUtils.closeStream(fsdis);
			fsdis.close();
		}
		
	}
	
	public void touchz(String remoteFile,String content) throws IOException{
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		byte[] buff = content.getBytes();
		FSDataOutputStream fsdos = null;
		try{
			fsdos = fs.create(new Path(remoteFile));
			fsdos.write(buff);
			System.out.println("Create: " + remoteFile);
		}catch(Exception e){
			
		}finally{
			if(fsdos!=null){
				fsdos.close();
				fs.close();
			}
		}
	}
	
}

