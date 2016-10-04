package com.hadoop.intellipaat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Iterator;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.hash.Hashing;

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
 * hadoop dfs -Ddfs.replication=1 -put  Impression* /apps/
 * 
 * @author raghunandangupta
 *
 */

public class JoinClickImpressionDetailJob extends Configured implements Tool {

	public static final String IMPRESSION_PREFIX = "IMPRESSION_PREFIX~";
	public static final String CLICK_PREFIX = "CLICK_PREFIX~";
	private static final String SPACE = " ";
	private static final Random random = new Random();

	public static class TrackerPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return (key.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}

	}

	public static class ClickNonClickPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			if (key.toString().equals("1")) {
				return numPartitions - 1;
			} else {
				return random.nextInt(7);
			}
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
			} else {
				context.write(new Text(""), value);
			}
		}

		@Override
		public void run(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			setup(context);
			while (true) {
				try {
					if (!context.nextKeyValue()) {
						break;
					} else {
						map(context.getCurrentKey(), context.getCurrentValue(), context);
					}
				} catch (Exception sre) {
					System.out.println("Mapper : "+sre+" "+System.currentTimeMillis());
					break;
				}
			}
			cleanup(context);
		}
	}

	private static class ClickMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String words[] = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			if (words.length > 18) {
				context.write(new Text(words[18].trim()), new Text(CLICK_PREFIX + value.toString()));
			} else {
				context.write(new Text(""), new Text("1"));
			}
		}
	}

	/**
	 * Here file will be committed in libsvm format.
	 *
	 */
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
						impressionData = output.replace(IMPRESSION_PREFIX, "").toLowerCase();
					}
					if (output.indexOf(CLICK_PREFIX) != -1) {
						iClickPresent = true;
					}
				}

				String record = convertToLibsvm(impressionData.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1));
				if (record.trim().length() > 0) {
					if (iClickPresent && isImpressionPresent) {
						context.write(new Text("1"), new Text(record));
					} else if (isImpressionPresent) {
						context.write(new Text("0"), new Text(record));
					}
				}
			} catch (Exception exception) {
				exception.printStackTrace();
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

	private static String convertToLibsvm(String[] words) {
		StringBuilder sb = new StringBuilder();
		try {
			// Return if ad type is not product and price is zero
			if (!(words[24].trim().length() > 0 && Float.parseFloat(words[24].trim()) > 0) || !("product".equalsIgnoreCase(words[15].trim()))) {
				return "";
			}

			sb.append("1:" + ((Float) Float.parseFloat(words[0])).intValue()).append(SPACE) // Account
																							// Id
					.append("2:" + hashCode(words[1])).append(SPACE) // Brand Id
					.append("3:" + hashCode(words[5])).append(SPACE); // category

			// Page Type
			byte[] pageTypes = convertPageTypeToBytes(words[6].trim());
			if (pageTypes != null) {
				sb.append("4:" + pageTypes[0]).append(SPACE).append("5:" + pageTypes[1]).append(SPACE).append("6:" + pageTypes[2]).append(SPACE);
			}

			// Site Ids
			byte[] siteTypes = convertSiteIdToBytes(words[7].trim());
			if (siteTypes != null) {
				sb.append("7:" + pageTypes[0]).append(SPACE).append("8:" + pageTypes[1]).append(SPACE).append("9:" + pageTypes[2]).append(SPACE);
			}

			sb.append("10:" + hashCode(words[8].trim())).append(SPACE) // Seller
																		// Code
					.append("11:" + hashCode(words[11].trim())).append(SPACE) // PogId
					.append("12:" + hashCode(words[12].trim())).append(SPACE) // device
																				// Id
					.append("13:" + hashCode(words[13].trim())).append(SPACE); // Email_id

			if (words[20].trim().length() > 0) {
				int[] timeStamp = convertToDay_Month_Year(words[20].trim());
				if (timeStamp != null) {
					sb.append("14:" + timeStamp[0]).append(SPACE).append("15:" + timeStamp[1]).append(SPACE);
				}
			}

			if (words[21].trim().length() > 0) {
				sb.append("16:" + ((Float) Float.parseFloat(words[21].trim())).intValue()).append(SPACE); // Relevant
																											// score
			}

			if (words[22].trim().length() > 0) {
				sb.append("17:" + hashCode(words[22])).append(SPACE); // Relevant
																		// category
			}

			sb.append("18:" + ((Float) Float.parseFloat(words[24].trim())).intValue()).append(SPACE); // price

			if (words[25].trim().length() > 0) {
				sb.append("19:" + ((Float) Float.parseFloat(words[25].trim())).intValue()).append(SPACE); // rating
			}

			if (words[26].trim().length() > 0) {
				sb.append("20:" + ((Float) Float.parseFloat(words[26].trim())).intValue()).append(SPACE); // discount
			}

			// Sd plus
			if (words[27].trim().length() != 0) {
				boolean sdPlus = Boolean.parseBoolean(words[27].trim());
				if (sdPlus) {
					sb.append("21:" + "1").append(SPACE).append("22:" + "0").append(SPACE);
				} else {
					sb.append("21:" + "0").append(SPACE).append("22:" + "1").append(SPACE);
				}
			}

			// OS
			if (words[31].trim().length() > 0) {
				sb.append("23:" + hashCode(words[31].trim())).append(SPACE);
			}

			// Browser
			if (words[32].trim().length() > 0) {
				sb.append("24:" + hashCode(words[32].trim())).append(SPACE);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
		return sb.toString();
	}

	private static final byte[] UNKNOWN = { 0, 0, 0 };
	private static final byte[] SLP = { 1, 0, 0 };
	private static final byte[] CLP = { 0, 1, 0 };
	private static final byte[] PDP = { 0, 0, 1 };

	private static final byte[] WEB = { 1, 0, 0 };
	private static final byte[] WAP = { 0, 1, 0 };
	private static final byte[] APP = { 0, 0, 1 };
	private static final byte[] GENERIC = { 0, 0, 0 };

	private static int[] convertToDay_Month_Year(String timeStamp) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(Long.parseLong(timeStamp));
		return new int[] { cal.get(Calendar.HOUR), cal.get(Calendar.DAY_OF_WEEK) };
	}

	private static byte[] convertPageTypeToBytes(String siteId) {
		byte[] value = null;
		switch (siteId) {
		case "slp":
			value = SLP;
			break;
		case "clp":
			value = CLP;
			break;
		case "pdp":
			value = PDP;
			break;
		default:
			value = UNKNOWN;
		}
		return value;
	}

	private static byte[] convertSiteIdToBytes(String siteId) {
		byte[] value = null;
		switch (siteId) {
		case "101":
			value = WEB;
			break;
		case "102":
			value = WAP;
			break;
		case "103":
		case "104":
		case "105":
			value = APP;
			break;
		default:
			value = GENERIC;
		}
		return value;
	}

	private static String hashCode(String value) {
		return Math.abs(Hashing.murmur3_32().hashString(value, StandardCharsets.UTF_8).hashCode()) + "";
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new JoinClickImpressionDetailJob(), args);
		System.exit(1);
	}

	private static int runMRJobs(String[] args) {
		int result = -1;
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.fileoutputformat.compress", "true");
		conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
		conf.set("dfs.replication", "1");
		conf.set("mapreduce.reduce.java.opts", "-Xmx3072m");
		conf.set("mapreduce.map.java.opts", "-Xmx2048m");
		conf.set("mapreduce.map.memory.mb", "3072");
		conf.set("mapreduce.reduce.memory.mb", "4096");
		conf.set("mapreduce.map.cpu.vcores", "8");
		conf.set("mapreduce.reduce.cpu.vcores", "8");
//		conf.set("mapreduce.job.running.map.limit", "200");
//		conf.set("mapreduce.job.running.reduce.limit", "100");
		conf.set("mapreduce.job.jvm.numtasks", "-1");
		conf.set("mapreduce.task.timeout", "0");
		conf.set("mapreduce.task.io.sort.factor", "64");
		conf.set("mapreduce.task.io.sort.mb", "640");
		conf.set("dfs.namenode.handler.count", "32");	
		conf.set("dfs.datanode.handler.count", "32");
		conf.set("io.file.buffer.size", "65536");
		conf.set("mapred.child.java.opts", "-Xmx200m -XX:+UseConcMarkSweepGC");
		
		

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

		System.out.println("First Job Finished=============");

		System.out.println("Second Job Started=============");

		ControlledJob mrJob2 = null;
		try {
			mrJob2 = new ControlledJob(conf);
			deleteDirectory(args[3], conf);
			mrJob2.addDependingJob(mrJob1);
			mrJob2.setJobName("IMPRESSION_CLICK_COMBINE_JOB1");
			Job job2 = mrJob2.getJob();
			result += secondMapReduceJob(args, job2);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Second Job Finished=============");

		JobControl jobControl = new JobControl("Click-Impression-aggregator");
		jobControl.addJob(mrJob1);
		jobControl.addJob(mrJob2);
		jobControl.run();
		return result;
	}

	private static int secondMapReduceJob(String[] args, Job job2) throws IOException, InterruptedException, ClassNotFoundException {
		long startTime = System.currentTimeMillis();

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setJarByClass(JoinClickImpressionDetailJob.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setReducerClass(ImpressionAndClickReducer.class);

		FileInputFormat.setInputDirRecursive(job2, true);
		FileInputFormat.addInputPath(job2, new Path(args[2]));
		job2.setMapperClass(ImpressionClickMapper.class);

		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		job2.setNumReduceTasks(20); // 3/2 * core
		job2.setPartitionerClass(ClickNonClickPartitioner.class);
		System.out.println("Time taken : " + (System.currentTimeMillis() - startTime) / 1000);
		return job2.waitForCompletion(true) ? 0 : 1;
	}

	private static int firstMapReduceJob(String[] args, Job job) throws IOException, InterruptedException, ClassNotFoundException {

		long startTime = System.currentTimeMillis();
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(JoinClickImpressionDetailJob.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setReducerClass(ImpressionClickReducer.class);

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

		job.setNumReduceTasks(20);

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
