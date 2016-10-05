package com.hadoop.intellipaat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This job will combine click and impression on TrackerId
 * 
 * For merging : hadoop fs -cat /user/output/* > /tmp/final.txt Number of files
 * created will be equal to number of reducers.
 * 
 * dfs.permissions == false
 * 
 * hadoop distcp /apps/ReporterBackup/Impression/2016* /apps/
 * 
 * hadoop dfs -Ddfs.replication=1 -put Impression* /apps/
 * 
 * @author raghunandangupta
 *
 */

public class GzipReadTestJob extends Configured implements Tool {

	public static final String IMPRESSION_PREFIX = "IMPRESSION_PREFIX~";
	public static final String CLICK_PREFIX = "CLICK_PREFIX~";

	public static class TrackerPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return (key.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}

	}

	private static String[] HEADERS = new String[] { "accountId", "brand", "campaignId", "inverseTimestamp", "supc", "category", "pagetype", "site",
			"sellerCode", "amount", "publisherRevenue", "pog", "device_id", "email", "user_id", "adType", "url", "cookieId", "trackerId",
			"creativeId", "timestamp", "relevancy_score", "relevancy_category", "ref_tag", "offer_price", "rating", "discount", "sdplus",
			"no_of_rating", "created_time", "normalized_rating", "os", "browser", "city", "state", "country" };

	private static class ImpressionMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// Excluding header
			if (!(value.toString().indexOf(HEADERS[0]) != -1)) {
				String words[] = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				if (words.length >= 32) {
					context.write(new Text(words[18].trim()), new Text(IMPRESSION_PREFIX + value.toString()));
				}
			}
		}

	}

	private static class ClickMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String words[] = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			if (words.length > 18) {
				context.write(new Text(words[18].trim()), new Text(CLICK_PREFIX + value.toString()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new GzipReadTestJob(), args);
		System.exit(1);
	}

	private static int runMRJobs(String[] args) {
		int result = -1;
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.fileoutputformat.compress", "true");
		conf.set("mapreduce.output.fileoutputformat.compress.codec",
				"org.apache.hadoop.io.compress.DefaultCodec,com.hadoop.intellipaat.SplittableGzipCodec,org.apache.hadoop.io.compress.BZip2Codec");
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
		conf.set("dfs.replication", "1");
		conf.set("mapreduce.reduce.java.opts", "-Xmx3072m");
		conf.set("mapreduce.map.java.opts", "-Xmx2048m");
		conf.set("mapreduce.map.memory.mb", "3072");
		conf.set("mapreduce.reduce.memory.mb", "4096");
		conf.set("mapreduce.map.cpu.vcores", "8");
		conf.set("mapreduce.reduce.cpu.vcores", "8");
		// conf.set("mapreduce.job.running.map.limit", "200");
		// conf.set("mapreduce.job.running.reduce.limit", "100");
		conf.set("mapreduce.job.jvm.numtasks", "-1");
		conf.set("mapreduce.task.timeout", "0");
		// conf.set("mapreduce.task.io.sort.factor", "64");
		// conf.set("mapreduce.task.io.sort.mb", "640");
		// conf.set("dfs.namenode.handler.count", "32");
		// conf.set("dfs.datanode.handler.count", "32");
		conf.set("io.file.buffer.size", "65536");
		conf.set("mapred.child.java.opts", "-Xmx200m -XX:+UseConcMarkSweepGC");
		conf.set("mapreduce.input.fileinputformat.split.minsize", "33554432");
		conf.set("mapreduce.map.speculative", "true");

		ControlledJob mrJob1 = null;
		Job firstJob = null;
		try {
			deleteDirectory(args[2], conf);
			mrJob1 = new ControlledJob(conf);
			mrJob1.setJobName("IMPRESSION_CLICK_COMBINE_JOB");
			firstJob = mrJob1.getJob();
			result += firstMapReduceJob(args, firstJob);
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("Job Finished=============");

		JobControl jobControl = new JobControl("Click-Impression-aggregator");
		jobControl.addJob(mrJob1);
		jobControl.run();
		return result;
	}

	private static int firstMapReduceJob(String[] args, Job job) throws IOException, InterruptedException, ClassNotFoundException {

		long startTime = System.currentTimeMillis();
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(GzipReadTestJob.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputDirRecursive(job, true);
		/**
		 * Here directory of impressions will be present
		 */
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ImpressionMapper.class);
		/**
		 * Here directory of clicks will be present
		 */
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ClickMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setPartitionerClass(TrackerPartitioner.class);

		System.out.println("Time taken : " + (System.currentTimeMillis() - startTime) / 1000);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	private static void deleteDirectory(String args, Configuration conf) throws IOException {
		Path p = new Path(args);
		FileSystem fs = FileSystem.get(conf);
		fs.exists(p);
		fs.delete(p, true);
	}

	@Override
	public int run(String[] args) throws Exception {
		return runMRJobs(args);
	}
}
