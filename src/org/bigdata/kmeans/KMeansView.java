package org.bigdata.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;

public class KMeansView {

	private static class KMeansViewMapper extends Mapper<LongWritable, Text, Text, Text>{
		private static List<Cluster> clusters = new ArrayList<Cluster>();
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			FileSystem fs = FileSystem.get(config);
			FSDataInputStream fis = fs.open(new Path("/data/cluster.txt"));
			LineReader reader = new LineReader(fis, config);
			Text value = new Text();
			
			while (reader.readLine(value) > 0) {
				System.out.println(value.toString());
				Cluster cluster = new Cluster(value.toString());
				clusters.add(cluster);
			}
			reader.close();
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			Vector vector = new Vector(value.toString());
			String id = getNearest(vector);
			context.write(new Text(vector.toString()), new Text(id));
		}

		private static String getNearest(Vector vector) {
			double min = Double.MAX_VALUE;
			String id = "";
			for (Cluster cluster:clusters) {
				double distance = new EuclideanDistance().getDistance(vector, cluster.getVector());
				if (distance < min){
					min = distance;
					id = cluster.getId();
				}
			}
			return id;
		}
		
	}
	
	
}
