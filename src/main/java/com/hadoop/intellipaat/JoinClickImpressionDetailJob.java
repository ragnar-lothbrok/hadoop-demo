package com.hadoop.intellipaat;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This job will combine click and impression on TrackerId
 * 
 * @author raghunandangupta
 *
 */

public class JoinClickImpressionDetailJob {

	public static final String IMPRESSION_PREFIX = "IMPRESSION_PREFIX";
	public static final String CLICK_PREFIX = "CLICK_PREFIX";
	public static final String SEPERATOR = "~";

	public static class TrackerPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return (key.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}

	}

	private static class ImpressionMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			/**
			 * Excluding header
			 */
			if (!(value.toString().indexOf("accountId") != -1)) {
				String words[] = value.toString().split(",");
				if (words.length > 18) {
					context.write(new Text(words[18].trim()), new Text(IMPRESSION_PREFIX + SEPERATOR + value.toString()));
				}
			} else {
				context.write(new Text(""), value);
			}
		}
	}

	private static class ClickMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String words[] = value.toString().split(",");
			if (words.length > 18) {
				context.write(new Text(words[18].trim()), new Text(CLICK_PREFIX + SEPERATOR + value.toString()));
			} else {
				context.write(new Text(""), new Text("1"));
			}
		}
	}

	private static class ImpressionClickReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) {
			try {
				Boolean iClickPresent = false;
				Boolean isImpressionPresent = false;
				String impressionData = "";
				Iterator<Text> iterator = values.iterator();
				while (iterator.hasNext()) {
					String output = iterator.next().toString();
					if (output.indexOf(IMPRESSION_PREFIX) != -1) {
						isImpressionPresent = true;
						impressionData = output.replace(IMPRESSION_PREFIX, "");
					}
					if (output.indexOf(CLICK_PREFIX) != -1) {
						iClickPresent = true;
					}
				}

				if (iClickPresent && isImpressionPresent) {
					context.write(key, new Text(impressionData + ",1"));
				}else if(isImpressionPresent){
					context.write(key, new Text(impressionData + ",0"));
				}
			} catch (Exception exception) {
				exception.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		try {

			long startTime = System.currentTimeMillis();

			Configuration conf = new Configuration();
//			conf.set("mapreduce.output.fileoutputformat.compress", "true");
//			conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
//			conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
//			conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
			Job job = Job.getInstance(conf, "IMPRESSION_CLICK_COMBINE_JOB");
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setReducerClass(ImpressionClickReducer.class);

			FileInputFormat.setInputDirRecursive(job, true);

			// FileInputFormat.addInputPath(job, new Path(args[0]));
			// job.setMapperClass(ImpressionMapper.class);

			Path p = new Path(args[2]);
			FileSystem fs = FileSystem.get(conf);
			fs.exists(p);
			fs.delete(p, true);

			/**
			 * Here directory of impressions will be present
			 */
			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ImpressionMapper.class);
			/**
			 * Here directory of clicks will be present
			 */
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ClickMapper.class);

			FileOutputFormat.setOutputPath(job, new Path(args[2]));

			job.setNumReduceTasks(10);
			
			job.setPartitionerClass(TrackerPartitioner.class);

			job.waitForCompletion(true);
			System.out.println("Time taken : " + (System.currentTimeMillis() - startTime) / 1000);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
