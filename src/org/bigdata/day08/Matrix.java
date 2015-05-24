package org.bigdata.day08;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.util.HadoopConfig;

public class Matrix {

	

	private static class MatrixMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private static int columnN = 0;
		private static int rowM = 0;
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			columnN = config.getInt("columnN", 0);
			rowM = config.getInt("rowM", 0);
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();

			String str = value.toString();
			String[] strs = str.split(",");
			int i = Integer.parseInt(strs[0]);
			String[] strs2 = strs[1].split("\t");
			int j = Integer.parseInt(strs2[0]);
			int val = Integer.parseInt(strs2[1]);
			if (fileName.startsWith("M")) {
				for (int count = 1; count <= columnN; count++) {
					context.write(new Text(i + "," + count), new Text("M,"+j+","+val + ""));
				}
			} else {// filename == N
				for (int count = 1; count <= rowM; count++) {
					context.write(new Text(count + "," + j), new Text("N,"+i+","+val + ""));
				}
			}

		}

	}

	private static class MatrixReducer extends
			Reducer<Text, Text, Text, IntWritable> {

		private static int columnM = 0;
		@Override
		protected void setup(
				Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			columnM = config.getInt("columnM", 0);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			int finalVal = 0;
			int[] mArray = new int[columnM+1];
			int[] nArray = new int[columnM+1];
			for (Text value:values){
				String str = value.toString();
				String[] strs = str.split(",");
				if (strs[0].equals("M")){
					mArray[Integer.parseInt(strs[1])] = Integer.parseInt(strs[2]);
				}else{//N
					nArray[Integer.parseInt(strs[1])] = Integer.parseInt(strs[2]);
				}
			}
			
			for (int i = 1;i < columnM+1;i++){
				finalVal += mArray[i]*nArray[i];
			}
			
			context.write(key, new IntWritable(finalVal));
		}

	}

	public static void main(String[] args) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		config.setInt("columnM", 100);
		config.setInt("columnN", 90);
		config.setInt("rowM", 70);
		
		Job job = Job.getInstance(config, "矩阵相乘");
		job.setJarByClass(Matrix.class);

		job.setMapperClass(MatrixMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MatrixReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path("/input"));
		FileOutputFormat.setOutputPath(job, new Path("/output"));
		System.out.println(job.waitForCompletion(true) ? 0 : 1);
	}
}
