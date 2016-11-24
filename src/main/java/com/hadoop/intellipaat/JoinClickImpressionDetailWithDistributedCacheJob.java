package com.hadoop.intellipaat;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
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

public class JoinClickImpressionDetailWithDistributedCacheJob extends Configured implements Tool {

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

	private static class ImpressionMapper extends Mapper<LongWritable, Text, Text, Text> {

		private final Text KEY = new Text();
		private final Text VALUE = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// Excluding header
			if (!(value.toString().indexOf(LibsvmConvertor.HEADERS[0]) != -1)) {
				String words[] = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				if (words.length >= 32) {
					String record = new LibsvmConvertor().convertToLibsvm(value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1));
					if (record.trim().length() > 0) {
						VALUE.set(record);
						if (clicksTrackerIdMap.containsKey(words[18].trim().toLowerCase())) {
							KEY.set("1");
						} else {
							KEY.set("0");
						}
						context.write(KEY, VALUE);
					}
				}
			}
		}

		private static Map<String, Byte> clicksTrackerIdMap = new HashMap<String, Byte>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			URI[] paths = context.getCacheArchives();
			if (paths != null) {
				for (URI path : paths) {
					loadDeliveryStatusCodes(path.toString(), context.getConfiguration());
				}
			}
			super.setup(context);
		}

		private void loadDeliveryStatusCodes(String file, Configuration conf) {
			System.out.println("File name : " + file);
			String strRead;
			BufferedReader br = null;
			try {
				FileSystem fileSystem = FileSystem.get(conf);
				FSDataInputStream open = fileSystem.open(new Path(file));
				br = new BufferedReader(new InputStreamReader(new GZIPInputStream(open)));
				while ((strRead = br.readLine()) != null) {
					String splitarray[] = strRead.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
					if (splitarray.length > 18) {
						clicksTrackerIdMap.put(splitarray[18].trim().toLowerCase(), Byte.MAX_VALUE);
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if (br != null)
						br.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Here file will be committed in libsvm format.
	 *
	 */
	private static class ImpressionAndClickReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text text : values) {
				context.write(key, text);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new JoinClickImpressionDetailWithDistributedCacheJob(), args);
		System.exit(1);
	}

	private static int runMRJobs(String[] args) {
		int result = -1;
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.fileoutputformat.compress", "true");
		conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
		conf.set("mapreduce.map.output.compress", "true");
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

		System.out.println("First Job STARTED=============");

		ControlledJob mrJob1 = null;
		Job firstJob = null;
		try {
			if (args.length != 0)
				deleteDirectory(args[2], conf);
			mrJob1 = new ControlledJob(conf);
			mrJob1.setJobName("IMPRESSION_CLICK_COMBINE_JOB");
			firstJob = mrJob1.getJob();
			result += firstMapReduceJob(args, firstJob);
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("First Job Finished=============");

		JobControl jobControl = new JobControl("Click-Impression-aggregator");
		jobControl.addJob(mrJob1);
		JobHandler.handleRun(jobControl);
		return result;
	}

	private static int firstMapReduceJob(String[] args, Job job)
			throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

		long startTime = System.currentTimeMillis();
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(JoinClickImpressionDetailWithDistributedCacheJob.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setReducerClass(ImpressionAndClickReducer.class);

		if (args.length >= 3) {
			FileInputFormat.setInputDirRecursive(job, true);
			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ImpressionMapper.class);
			FileSystem fileSystem = FileSystem.get(job.getConfiguration());
			RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path(args[1]), true);
			while (files.hasNext()) {
				job.addCacheArchive(files.next().getPath().toUri());
			}
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
		} else {
			FileInputFormat.setInputDirRecursive(job, true);
			MultipleInputs.addInputPath(job, new Path("/home/raghunandangupta/Downloads/click/Impression_30.0.4.34_8_2016-09-27.csv.gz"),
					TextInputFormat.class, ImpressionMapper.class);

			FileSystem fileSystem = FileSystem.get(job.getConfiguration());
			RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/home/raghunandangupta/Downloads/click"), true);
			while (files.hasNext()) {
				job.addCacheArchive(files.next().getPath().toUri());
			}
			FileOutputFormat.setOutputPath(job, new Path("/home/raghunandangupta/Downloads/click/otput"));
		}

		job.setNumReduceTasks(80);

		job.setPartitionerClass(ClickNonClickPartitioner.class);

		System.out.println("Time taken : " + (System.currentTimeMillis() - startTime) / 1000);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	private static void deleteDirectory(String args, Configuration conf) throws IOException {
		try {
			Path p = new Path(args);
			FileSystem fs = FileSystem.get(conf);
			fs.exists(p);
			fs.delete(p, true);
		} catch (IllegalArgumentException e) {
			System.out.println("File does not exists.");
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		return runMRJobs(args);
	}
}
