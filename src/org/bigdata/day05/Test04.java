/*
 * SequenceFile.Writer小文件处理，存储，二进制存储
 * 2015.5.14
 * SimonHui
 * */
package org.bigdata.day05;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.bigdata.util.HadoopConfig;


public class Test04 {

	public static void main(String [] args) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Path path = new Path("/seq.dat");
		Option optPath = SequenceFile.Writer.file(path);
		Option optKey = SequenceFile.Writer.keyClass(IntWritable.class);
		Option optValue = SequenceFile.Writer.valueClass(Text.class);
		Writer writer = SequenceFile.createWriter(config, optPath,optKey,optValue);
		
		for (int i = 0;i < 100;i++){
			writer.append(new IntWritable(i), new Text("Hello"));//写入小文件
		}
		
		writer.close();
	}
}
