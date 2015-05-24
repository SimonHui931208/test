package org.bigdata.pageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.util.HadoopConfig;

/**
 *  第一步
 * 
 * @author	: 0xC000005
 * @mailto	: fle4y@outlook.com
 * @blog		: http://0xC000005.github.io/
 * @since  	: 2015年5月21日
 *
 */

public class First {

	private static class FirstMapper extends Mapper<LongWritable, Text,Text,Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String str = value.toString().trim();
			String[] strs = str.split("\t",2);
			context.write(new Text(strs[0]),new Text(strs[1]));
		}
		
	}
	
	private static class FirstReducer extends Reducer<Text,Text,Text,Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String p="";
			String out="";
			for(Text value : values){
				String str = value.toString();
				if(str.startsWith("a")){
					p = str.split("\t")[1];
				}else{
					out = str;
				}
			}
			context.write(key,new Text(p + "#" + out));
		}
	}
	
	public static void runFirst() throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"PageRank First");
		job.setJarByClass(First.class);
		
		job.setMapperClass(FirstMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(FirstReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/input"));
		FileInputFormat.addInputPath(job, new Path("/input2"));
		FileOutputFormat.setOutputPath(job,new Path("/output/"));
		job.waitForCompletion(true);
	}
	
}