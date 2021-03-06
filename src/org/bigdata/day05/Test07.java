/*
 * MapFile.Reader��ȡС�ļ�
 * 2015.5.14
 * SimonHui
 * */
package org.bigdata.day05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.bigdata.util.HadoopConfig;

public class Test07 {
	public static void main(String [] args) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Path path = new Path("/map.dat");
		MapFile.Reader reader = new MapFile.Reader(path, config);
		IntWritable key = new IntWritable();
		Text value = new Text();
		while (reader.next(key,value)) {
			
			System.out.println("key-->"+key.get()+" value-->"+value.toString());
		}
		
		reader.close();
	}
}
