package org.bigdata.day05;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.util.HadoopConfig;

public class Weather {

	private static class WeatherMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			String year = str.substring(0, 4);
			String temp = str.substring(14, 19).trim();
			temp.indexOf("");
			if (!"-9999".equals(temp)) {
				context.write(new Text(year),
						new IntWritable(Integer.parseInt(temp)));
			}

		}

	}

	
	private static class WeatherCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			int max = Integer.MIN_VALUE;//最高温度
			for (IntWritable value : values) {
				if (value.get() > max) {
					max = value.get();
				}
			}
			context.write(key, new IntWritable(max));
		}
	}
	
	private static class WeatherReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			int max = Integer.MIN_VALUE;//最高温度
			for (IntWritable value : values) {
				if (value.get() > max) {
					max = value.get();
				}
			}
			context.write(key, new IntWritable(max));
		}

	}

	public static void main(String[] args) throws Exception {
		
		long start = System.currentTimeMillis();
		Configuration congfig = HadoopConfig.getConfig();
		Job job = Job.getInstance(congfig, "统计最高和最低温度");
		job.setJarByClass(Weather.class);

		job.setMapperClass(WeatherMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setCombinerClass(WeatherCombiner.class);

		job.setReducerClass(WeatherReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path("/input"));
		FileOutputFormat.setOutputPath(job, new Path("/output"));
		System.out.println(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("运行时间为：" + (System.currentTimeMillis()-start));
	}
}
