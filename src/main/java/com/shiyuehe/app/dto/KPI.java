package com.shiyuehe.app.dto;

import java.io.Serializable;
import java.util.StringTokenizer;

public class KPI implements Serializable{

	private String remoteName;
	private String remoteMac;
	private String remoteIp;
	private String remoteParam;
	private String remoteuser;
	private String timeLocal;
	private String request;
	private String status;
	private String bodyBytesSent;
	private String httpRefer;
	private String userAgent;
	private String remoteParem1;
	@Override
	public String toString() {
		return "KPI : \n=================================================================="
				+ "\n remoteName=" + remoteName + ",\n remoteMac=" + remoteMac
				+ ",\n remoteIp=" + remoteIp + ",\n remoteParam=" + remoteParam
				+ ",\n remoteuser=" + remoteuser + ",\n timeLocal=" + timeLocal
				+ ",\n request=" + request + ",\n status=" + status
				+ ",\n bodyBytesSent=" + bodyBytesSent + ",\n httpRefer="
				+ httpRefer + ",\n userAgent=" + userAgent + ",\n remoteParem1="
				+ remoteParem1+"\n==================================================================";
	}
	public String getRemoteName() {
		return remoteName;
	}
	public void setRemoteName(String remoteName) {
		this.remoteName = remoteName;
	}
	public String getRemoteMac() {
		return remoteMac;
	}
	public void setRemoteMac(String remoteMac) {
		this.remoteMac = remoteMac;
	}
	public String getRemoteIp() {
		return remoteIp;
	}
	public void setRemoteIp(String remoteIp) {
		this.remoteIp = remoteIp;
	}
	public String getRemoteParam() {
		return remoteParam;
	}
	public void setRemoteParam(String remoteParam) {
		this.remoteParam = remoteParam;
	}
	public String getRemoteuser() {
		return remoteuser;
	}
	public void setRemoteuser(String remoteuser) {
		this.remoteuser = remoteuser;
	}
	public String getTimeLocal() {
		return timeLocal;
	}
	public void setTimeLocal(String timeLocal) {
		if(timeLocal.startsWith("["))
				timeLocal = timeLocal.substring(1);
		if(timeLocal.endsWith("]"))
				timeLocal = timeLocal.substring(0,timeLocal.length()-1);
		this.timeLocal = timeLocal;
	}
	public String getRequest() {
		return request;
	}
	public void setRequest(String request) {
		this.request = request;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getBodyBytesSent() {
		return bodyBytesSent;
	}
	public void setBodyBytesSent(String bodyBytesSent) {
		this.bodyBytesSent = bodyBytesSent;
	}
	public String getHttpRefer() {
		return httpRefer;
	}
	public void setHttpRefer(String httpRefer) {
		this.httpRefer = httpRefer;
	}
	public String getUserAgent() {
		return userAgent;
	}
	public void setUserAgent(String userAgent) {
		if(userAgent.contains("(")){
			int idx = userAgent.indexOf("(");
			userAgent = userAgent.substring(0,idx);
		}
		this.userAgent = userAgent;
	}
	public String getRemoteParem1() {
		return remoteParem1;
	}
	public void setRemoteParem1(String remoteParem1) {
		this.remoteParem1 = remoteParem1;
	}
	
	public static void main(String[] args){
		String line = "\"hctmtzqV1_1_0\" \"00-00-00-00-00-00\" 157.55.36.53 - - [22/Feb/2014:03:13:42 +0800] \"GET / HTTP/1.1\" 200 2975 \"-\" \"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)\" \"-\"";
		String[] lines = line.replace("\"", "").split(" ");
		KPI kpi = new KPI();
		kpi.setRemoteName(lines[0]);
		kpi.setRemoteMac(lines[1]);
		kpi.setRemoteIp(lines[2]);
		kpi.setRemoteParam(lines[3]);
		kpi.setRemoteuser(lines[4]);
		kpi.setTimeLocal(lines[5]);
		kpi.setRequest(lines[8]);
		kpi.setStatus(lines[10]);
		kpi.setBodyBytesSent(lines[11]);
		kpi.setHttpRefer(lines[12]);
		String ua = "";
		for(int i = 13;i<=lines.length-2;i++){
			ua += lines[i];
		}
		kpi.setUserAgent(ua);
		kpi.setRemoteParem1(lines[lines.length-1]);
		System.out.println(kpi);
	}
	
}
