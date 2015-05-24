package org.bigdata.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.bigdata.util.HadoopConfig;

public class KMeansView {
	private static class KMeansViewMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private static List<Cluster> clusters = new ArrayList<Cluster>();

		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			String readClusterPath = config.get("readClusterPath");
			FileSystem fs = FileSystem.get(config);
			FileStatus[] files = fs.listStatus(new Path(readClusterPath));
			if (files != null) {
				for (int i = 0; i < files.length; i++) {
					if (files[i].isFile()) {
						if (files[i].getPath().getName().startsWith("p")) {
							FSDataInputStream fis = fs.open(files[i].getPath());
							LineReader reader = new LineReader(fis, config);
							Text value = new Text();
							while (reader.readLine(value) > 0) {
								Cluster cluster = new Cluster(value.toString());
								clusters.add(cluster);
							}
							System.out.println("初始化簇心");
							for (Cluster cluster : clusters) {
								System.out.println(cluster);
							}
							reader.close();
						}
					}
				}
			}
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Vector vector = new Vector(value.toString());
			System.out.println("vector -->" + vector);
			String id = getNearest(vector);
			System.out.println("nearest id ====>" + id);
			context.write(new Text(vector.toString()), new Text(id));
		}

		private String getNearest(Vector vector) {
			double min = Double.MAX_VALUE;
			String id = "";
			for (Cluster cluster : clusters) {
				double distance = new EuclideanDistance().getDistance(vector,
						cluster.getVector());
				if (distance < min) {
					min = distance;
					id = cluster.getId();
				}
			}
			return id;
		}
	}

	private static class KMeansViewReducer extends
			Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}

	public static void runKMeansViewJob(String readClusterPath,
			String writeClusterPath) throws Exception {
		Configuration config = HadoopConfig.getConfig();
		config.set("readClusterPath", readClusterPath);
		Job job = Job.getInstance(config, "KMeans");
		job.setJarByClass(KMeansView.class);
		job.setMapperClass(KMeansViewMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(KMeansViewReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path("/data/input.txt"));
		FileOutputFormat.setOutputPath(job, new Path(writeClusterPath));
		job.waitForCompletion(true);
	}
}
