package com.hadoop.intellipaat;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.collect.Lists;

//https://www.linkedin.com/pulse/distributed-cache-hadoop-examples-gaurav-singh
//http://kickstarthadoop.blogspot.in/2011_09_01_archive.html
public class UserSMSDeliveryJob {

	private static final String USER = "user";
	private static final String USER_SMS = "user-sms";

	private static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String words[] = value.toString().split(",");
			context.write(new Text(words[0].trim()), new Text(USER + "~" + words[1].trim()));
		}

	}

	private static class UserSmsCodeMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String words[] = value.toString().split(",");
			context.write(new Text(words[0].trim()), new Text(USER_SMS + "~" + words[1].trim()));
		}

	}

	private static class UserNameSmsStatusCodeReducer extends Reducer<Text, Text, Text, Text> {

		private static Map<String, String> deliveryCodesMap = new HashMap<String, String>();

		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			URI[] paths = context.getCacheFiles();
			if (paths.length > 0) {
				loadDeliveryStatusCodes(paths[0].toString());
			}
			super.setup(context);
		}

		private void loadDeliveryStatusCodes(String file) {
			String strRead;
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader("./some"));
				while ((strRead = reader.readLine()) != null) {
					String splitarray[] = strRead.split(",");
					deliveryCodesMap.put(splitarray[0].trim(), splitarray[1].trim());
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					reader.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			List<Text> myList = Lists.newArrayList(values);
			String text1[] = myList.get(0).toString().split("~");
			String text2[] = myList.get(1).toString().split("~");

			System.out.println(text1[0] + " " + text1[1] + " " + text2[0] + " " + text2[1]);

			String userName = "";
			String smsStatus = "";
			if (text1[0].equalsIgnoreCase(USER)) {
				userName = text1[1].trim();
			} else if (text2[0].equalsIgnoreCase(USER)) {
				userName = text2[1].trim();
			}

			if (text1[0].equalsIgnoreCase(USER_SMS)) {
				smsStatus = deliveryCodesMap.get(text1[1].trim());
			} else if (text2[0].equalsIgnoreCase(USER_SMS)) {
				smsStatus = deliveryCodesMap.get(text2[1].trim());
			}

			context.write(new Text(userName), new Text(smsStatus));
		}
	}

	public static void main(String[] args) {

		try {
			Configuration conf = new Configuration();

			Job job = Job.getInstance(conf, "USER_SMS_FILES");

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			if (args.length == 4) {
				MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserMapper.class);
				MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UserSmsCodeMapper.class);
				job.addCacheFile(new URI(args[2]));
				Path out = new Path(args[3]);
				FileOutputFormat.setOutputPath(job, out);
				out.getFileSystem(conf).deleteOnExit(out);
			} else {
				MultipleInputs.addInputPath(job, new Path("/home/raghunandangupta/gitPro/hadoop-demo/inputfiles/smsdata/UserDetails.txt"),
						TextInputFormat.class, UserMapper.class);
				MultipleInputs.addInputPath(job, new Path("/home/raghunandangupta/gitPro/hadoop-demo/inputfiles/smsdata/DeliveryDetails.txt"),
						TextInputFormat.class, UserSmsCodeMapper.class);
				FileOutputFormat.setOutputPath(job, new Path("/home/raghunandangupta/gitPro/hadoop-demo/inputfiles/smsdata/output21"));
				job.addCacheFile(new Path("/home/raghunandangupta/gitPro/hadoop-demo/inputfiles/smsdata/DeliveryStatusCodes.txt").toUri());
			}
			job.setReducerClass(UserNameSmsStatusCodeReducer.class);
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
