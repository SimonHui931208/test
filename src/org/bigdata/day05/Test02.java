/*
 * Hadoop解压演示
 * 2015.5.14
 * SimonHui
 * */
package org.bigdata.day05;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.bigdata.util.HadoopConfig;

public class Test02 {

	public static void main(String [] args) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Path path = new Path("/hello.gz");
		FileSystem fs = FileSystem.get(config);
		InputStream is = fs.open(path);
		GzipCodec codec = new GzipCodec();
		codec.setConf(config);
		CompressionInputStream cis = codec.createInputStream(is);//装饰模式
		byte[] buffer = new byte[1024];
		int read = -1;
		while ((read = cis.read(buffer)) != -1) {
			System.out.println(new String(buffer,0,read));
			
		}
		cis.close();
 	}
}
