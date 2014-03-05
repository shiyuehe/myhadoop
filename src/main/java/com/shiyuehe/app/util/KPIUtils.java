package com.shiyuehe.app.util;

import com.shiyuehe.app.dto.KPI;

public class KPIUtils {

	public static KPI createKPI(String line){
		String[] lines = line.replace("\"", "").split(" ");
		if(lines.length < 15 ) return null;
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
		StringBuffer ua = new StringBuffer();
		for(int i = 13;i<lines.length-1;i++){
			ua.append(lines[i]);
		}
		kpi.setUserAgent(ua.toString());
		kpi.setRemoteParem1(lines[lines.length-1]);
		System.out.println(kpi);
		return kpi;
	}
	
}
