package org.bigdata.pageRank;

import org.bigdata.util.HadoopUtil;

public class Main {
	
	public static void main(String[] args) throws Exception {
		for(int i =0 ; i< 10 ; i++){
			First.runFirst();
			HadoopUtil.deleteFile("/input2");
			Second.runSecond();
			HadoopUtil.deleteFile("/output");
		}
	}
	
	String  str = "";
	str
}
