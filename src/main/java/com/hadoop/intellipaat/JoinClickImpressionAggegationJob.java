package com.hadoop.intellipaat;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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

public class JoinClickImpressionAggegationJob extends Configured implements Tool {

	public static final String IMPRESSION_PREFIX = "IMPRESSION_PREFIX~";
	public static final String CLICK_PREFIX = "CLICK_PREFIX~";
	private static final Random random = new Random();

	public static class ClickNonClickPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			if (key.toString().equals("1")) {
				return numPartitions - 1;
			} else {
				return random.nextInt(numPartitions - 1);
			}
		}
	}

	private static class ImpressionClickMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String splits[] = value.toString().split("\t");
			context.write(new Text(splits[0].trim()), new Text(splits[1].trim()));
		}
	}

	private static class ImpressionAndClickReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text text : values) {
				context.write(key, text);
			}
		}
	}
	
	
	
	private static void abc(){
		try {
			Runtime.getRuntime().exec("hadoop -version");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		abc();
//		ToolRunner.run(new Configuration(), new JoinClickImpressionAggegationJob(), args);
//		System.exit(1);
	}

	private static int runMRJobs(String[] args) {
		int result = -1;
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.fileoutputformat.compress", "true");
		conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
		conf.set("dfs.replication", "1");
		conf.set("mapreduce.reduce.java.opts", "-Xmx9g");
		conf.set("mapreduce.map.java.opts", "-Xmx9g");
		conf.set("mapreduce.map.memory.mb", "10240");
		conf.set("mapreduce.reduce.memory.mb", "10240");
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
		conf.set("mapred.child.java.opts", "-Xmx15g -XX:+UseConcMarkSweepGC");
		conf.set("mapreduce.input.fileinputformat.split.minsize", "33554432");
		conf.set("mapreduce.map.speculative", "true");

		System.out.println("Second Job Started=============");

		ControlledJob mrJob2 = null;
		try {
			mrJob2 = new ControlledJob(conf);
			deleteDirectory(args[1], conf);
			mrJob2.setJobName("IMPRESSION_CLICK_COMBINE_JOB1");
			Job job2 = mrJob2.getJob();
			result += secondMapReduceJob(args, job2);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Second Job Finished=============");

		JobControl jobControl = new JobControl("Click-Impression-aggregator");
		jobControl.addJob(mrJob2);
		JobHandler.handleRun(jobControl);
		return result;
	}

	private static int secondMapReduceJob(String[] args, Job job2) throws IOException, InterruptedException, ClassNotFoundException {
		long startTime = System.currentTimeMillis();

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setJarByClass(JoinClickImpressionAggegationJob.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setReducerClass(ImpressionAndClickReducer.class);

		FileInputFormat.setInputDirRecursive(job2, true);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		job2.setMapperClass(ImpressionClickMapper.class);

		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.setNumReduceTasks(80); // 3/2 * core
		job2.setPartitionerClass(ClickNonClickPartitioner.class);
		System.out.println("Time taken : " + (System.currentTimeMillis() - startTime) / 1000);
		return job2.waitForCompletion(true) ? 0 : 1;
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
