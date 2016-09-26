package com.hadoop.skillspeed.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This will tell us which words are present in which files.
 * @author raghunandangupta
 *
 */
public class InvertedIndexJob {

	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			
			String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
			String words[] = line.split(",");
			for (String word : words) {
				context.write(new Text(word), new Text(fileName));
			}
		}
	}

	private static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text text, Iterable<Text> counts, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (Text file : counts) {
				sb.append(file.toString()+" ");
			}
			context.write(text, new Text(sb.toString()));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "WORDCOUNT");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputDirRecursive(job, true);
		if(args.length == 2){
			Path p = new Path(args[1]);
			FileSystem fs = FileSystem.get(conf);
			fs.exists(p);
			fs.delete(p, true);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		}else{
			FileInputFormat.addInputPath(job, new Path("/home/raghunandangupta/gitPro/hadoop-demo/inputfiles/smsdata"));
			FileOutputFormat.setOutputPath(job, new Path("/home/raghunandangupta/Desktop/CSV/ALLCSV/op"));
		}
		job.waitForCompletion(true);
	}
}
