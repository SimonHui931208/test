package org.bigdata.pageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.util.HadoopConfig;

/**
 * PageRank 第二步
 * 
 * @author	: 0xC000005
 * @mailto	: fle4y@outlook.com
 * @blog		: http://0xC000005.github.io/
 * @since  	: 2015年5月21日
 *
 */

public class Second {
	
	private static class SecondMapper extends Mapper<LongWritable, Text,Text,DoubleWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split("#");
			String[] strs01 = strs[0].split("\t");
			String outKey = strs01[0];
			Double p = Double.parseDouble(strs01[1]);
			String[] outLinks = strs[1].split("\t");
			context.write(new Text(outKey),new DoubleWritable(0.0));
			for(String str : outLinks){
				context.write(new Text(str),new DoubleWritable(p/outLinks.length));
			}
		}
	}
	
	private static class SecondReducer extends Reducer<Text, DoubleWritable,Text, Text>{
		private static final  double A = 0.85;
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Reducer<Text, DoubleWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			for(DoubleWritable value : values){
				sum = sum + value.get();
			}
			double p = A * sum + (1-A)*0.25;
			context.write(key,new Text("a	"+p));
		}
		
	}
	public static void runSecond() throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"PageRank Second");
		job.setJarByClass(First.class);
		job.setMapperClass(SecondMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setReducerClass(SecondReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/output"));
		FileOutputFormat.setOutputPath(job,new Path("/input2/"));
		job.waitForCompletion(true);
		
	}
}
