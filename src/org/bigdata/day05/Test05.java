/*
 * SequenceFile.Reader读取小文件
 * 2015.5.14
 * SimonHui
 * */
package org.bigdata.day05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;
import org.bigdata.util.HadoopConfig;

public class Test05 {
	
	public static void main(String [] args) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Path path = new Path("/seq.dat");
		Option optPath = SequenceFile.Reader.file(path);
		SequenceFile.Reader reader = new SequenceFile.Reader(config, optPath);
		IntWritable key = new IntWritable();
		Text value = new Text();
		while (reader.next(key,value)) {
			
			System.out.println("key-->"+key.get()+" value-->"+value.toString());
		}
		
		reader.close();
	}
}
