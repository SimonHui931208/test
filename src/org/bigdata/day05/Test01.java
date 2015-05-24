/*
 * Hadoop压缩演示
 * 2015.5.10
 * SimonHui
 * */

package org.bigdata.day05;



import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.bigdata.util.HadoopConfig;

public class Test01 {
	
	public static void main(String [] args) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Path path = new Path("/hello.gz");
		FileSystem fs = FileSystem.get(config);
		OutputStream os = fs.create(path);
		CompressionCodec codec = new GzipCodec();
		CompressionOutputStream cos = codec.createOutputStream(os);//装饰模式
		cos.write("hello world".getBytes());
		cos.close();
	}
}
