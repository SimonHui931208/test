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

public class WeatherCount {
	
	 
	
	private static class WeatherMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		int index = 0;
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println(index++ + "   "+value);
			String[] strs = value.toString().split(" ");
			int count = 0;
			for (String str:strs){
				if(!str.trim().equals("")){
					if (!str.trim().equals("-9999") && count == 4 || count == 5 ){
						context.write(new Text("MaxTemp"),
								new IntWritable(Integer.parseInt(str)));
						context.write(new Text("MinTemp"),
								new IntWritable(Integer.parseInt(str)));
						if (count == 5){
							context.write(new Text("AvgMinTemp"),
									new IntWritable(Integer.parseInt(str)));
						}else if (count == 4){
							context.write(new Text("AvgMaxTemp"),
									new IntWritable(Integer.parseInt(str)));
						}
					}
					count++;
				}
				if (count == 6)
					break;
			}
			
		}
		
	}
	
	private static class WeatherReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
	
			int varible = 0;
			if ( key.toString().equals("MaxTemp")){
				varible = -1000;
				for (IntWritable value:values){
					if (varible < value.get()){
						varible = value.get();
					}
				}
			}else if (key.toString().equals("MinTemp")){
				varible = 1000;
				for (IntWritable value:values){
					if (varible > value.get()){
						varible = value.get();
					}
				}
			}else{
				int count = 0;
				for (IntWritable value:values){
					varible += value.get();
					count++;
				}
				varible /= count;
			}
			
			context.write(key, new IntWritable(varible));
		}

	}
	
	public static void main(String[] args) throws Exception{
		Configuration congfig = HadoopConfig.getConfig();
		Job job = Job.getInstance(congfig,"统计最高和最低温度");
		job.setJarByClass(WeatherCount.class);
		
		job.setMapperClass(WeatherMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(WeatherReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/input"));
		FileOutputFormat.setOutputPath(job, new Path("/output"));
		System.out.println(job.waitForCompletion(true)?0:1);
	}
}
