package com.hadoop.intellipaat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

public class WordCountMultipleFiles {

	private static class MapperOne extends Mapper<LongWritable, Text, Text, CountWordPerFile> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CountWordPerFile>.Context context)
				throws IOException, InterruptedException {
			try {
				String fileName = "customer";
				String line = value.toString();
				String words[] = line.split(",");
				for (String word : words) {
					context.write(new Text(word), new CountWordPerFile(fileName, 1));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static class MapperTwo extends Mapper<LongWritable, Text, Text, CountWordPerFile> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CountWordPerFile>.Context context)
				throws IOException, InterruptedException {
			try {
				String fileName = "delivery";
				String line = value.toString();
				String words[] = line.split(",");
				for (String word : words) {
					context.write(new Text(word), new CountWordPerFile(fileName, 1));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static class Reduce extends Reducer<Text, CountWordPerFile, Text, Iterable<CountWordPerFile>> {

		@Override
		protected void reduce(Text text, Iterable<CountWordPerFile> counts,
				Reducer<Text, CountWordPerFile, Text, Iterable<CountWordPerFile>>.Context context) throws IOException, InterruptedException {
			try {
				List<CountWordPerFile> list = new ArrayList<CountWordPerFile>();
				for (CountWordPerFile count : counts) {
					CountWordPerFile countWordPerFile = new CountWordPerFile(count.getFileName(), count.getCount());
					if (list.contains(countWordPerFile)) {
						countWordPerFile.setCount(countWordPerFile.getCount() + list.get(list.indexOf(countWordPerFile)).getCount());
						list.remove(countWordPerFile);
					}
					list.add(countWordPerFile);
				}
				context.write(text, list);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {

		try {
			Configuration conf = new Configuration();

			Job job = Job.getInstance(conf, "WORDCOUNT_MULTIPLE_FILES");

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(CountWordPerFile.class);

			job.setMapperClass(MapperOne.class);
			job.setMapperClass(MapperTwo.class);
			job.setReducerClass(Reduce.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			if (args.length == 3) {
				MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperOne.class);
				MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperTwo.class);
				FileOutputFormat.setOutputPath(job, new Path(args[2]));
				Path out = new Path(args[2]);
				out.getFileSystem(conf).deleteOnExit(out);
			} else {
				MultipleInputs.addInputPath(job, new Path("/home/raghunandangupta/gitPro/hadoop-demo/inputfiles/multiplefile/customer.txt"),
						TextInputFormat.class, MapperOne.class);
				MultipleInputs.addInputPath(job, new Path("/home/raghunandangupta/gitPro/hadoop-demo/inputfiles/multiplefile/delivery.txt"),
						TextInputFormat.class, MapperTwo.class);
				FileOutputFormat.setOutputPath(job, new Path("/home/raghunandangupta/gitPro/hadoop-demo/inputfiles/multiplefile/output1"));
			}
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
