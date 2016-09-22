package com.hadoop.intellipaat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.collect.Lists;

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
				if (key.toString().length() != 0) {
					List<Text> myList = Lists.newArrayList(values);
					System.out.println("#######"+myList.get(0).toString()+"++++++++++"+myList.get(1).toString());
					if (myList.size() == 2) {
						if (myList.get(0).toString().indexOf(IMPRESSION_PREFIX) != -1 && myList.get(1).toString().indexOf(CLICK_PREFIX) != -1) {
							String line = myList.get(0).toString().split(SEPERATOR)[1] + ",1";
							context.write(key, new Text(line));
						} else if (myList.get(1).toString().indexOf(IMPRESSION_PREFIX) != -1
								&& myList.get(0).toString().indexOf(CLICK_PREFIX) != -1) {
							String line = myList.get(1).toString().split(SEPERATOR)[1] + ",1";
							context.write(key, new Text(line));
						}
					}
				}
			} catch (Exception exception) {
				exception.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			// conf.set("mapreduce.output.fileoutputformat.compress", "true");
			// conf.set("mapreduce.output.fileoutputformat.compress.codec",
			// "org.apache.hadoop.io.compress.GzipCodec");
			// conf.set("mapreduce.map.output.compress.codec",
			// "org.apache.hadoop.io.compress.SnappyCodec");
			// conf.set("mapreduce.output.fileoutputformat.compress.type",
			// "BLOCK");
			Job job = Job.getInstance(conf, "IMPRESSION_CLICK_COMBINE_JOB");

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.setInputDirRecursive(job, true);

			// FileInputFormat.addInputPath(job, new Path(args[0]));
			// job.setMapperClass(ImpressionMapper.class);

			/**
			 * Here directory of impressions will be present
			 */
			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ImpressionMapper.class);
			/**
			 * Here directory of clicks will be present
			 */
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ClickMapper.class);

			FileOutputFormat.setOutputPath(job, new Path(args[2]));

			job.setReducerClass(ImpressionClickReducer.class);

			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
