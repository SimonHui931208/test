package org.bigdata.day09;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.util.HadoopConfig;

public class SimpleInvertIndex {

	private static class SimpleMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().toString();
			String[] words = value.toString().split(" ");
			for (String word : words){
				context.write(new Text(word), new Text(fileName));
			}
			
		}

	}

	private static class SimpleReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		protected void setup(
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (Text value:values){
				sb.append(value.toString()).append(";");
			}
			context.write(key, new Text(sb.toString()));
		}

	}
	
	public static void main(String[] args) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		
		Job job = Job.getInstance(config, "简单倒排索引");
		job.setJarByClass(SimpleInvertIndex.class);

		job.setMapperClass(SimpleMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(SimpleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("/indexInput"));
		FileOutputFormat.setOutputPath(job, new Path("/indexOutput"));
		System.out.println(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
