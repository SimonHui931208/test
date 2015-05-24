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

public class Earthquake {

	private static class EarthquakeMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			String[] allProvenice = { "四川","北京","天津","上海","澳门","香港","台湾","海南","宁夏","西藏","青海"
					,"广东","贵州","福建","吉林","陕西","内蒙","山西","甘肃","广西","湖北","江西","浙江","江苏","新疆",
					"山东","安徽","湖南","黑龙江","辽宁","云南","河南","河北","重庆"};
			String[] strs = value.toString().split(",");
			
			if (strs.length < 9){
				return;
			}
			//if (strs[7].trim().equals("eq")) {
				for (String str : allProvenice) {
					if (strs[8].indexOf(str.trim()) != -1) {
						context.write(new Text(str.trim()), new IntWritable(1));
						break;
					}
				}
			//}

		}
	}

	private static class EarthquakeCombiner extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			context.write(key, new IntWritable(count));
		}
	}

	private static class EarthquakeReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			context.write(key, new IntWritable(count));
		}

	}

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		Configuration congfig = HadoopConfig.getConfig();
		Job job = Job.getInstance(congfig, "统计地震");
		job.setJarByClass(Earthquake.class);

		job.setMapperClass(EarthquakeMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setCombinerClass(EarthquakeCombiner.class);

		job.setReducerClass(EarthquakeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path("/input"));
		FileOutputFormat.setOutputPath(job, new Path("/output"));
		System.out.println(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("运行时间为：" + (System.currentTimeMillis() - start));
	}
}
