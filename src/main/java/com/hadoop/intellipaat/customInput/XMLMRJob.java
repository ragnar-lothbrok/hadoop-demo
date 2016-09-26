package com.hadoop.intellipaat.customInput;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class XMLMRJob {

	private static class XMLMapper extends Mapper<LongWritable, Text, Text, XMLWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, XMLWritable>.Context context)
				throws IOException, InterruptedException {
			XMLWritable xmlWritable = new XMLWritable();
			String id = "";
			try {
				XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(new ByteArrayInputStream(value.toString().getBytes()));
				String currentElement = "";
				while (reader.hasNext()) {
					int code = reader.next();
					switch (code) {
					case XMLStreamConstants.START_ELEMENT: // START_ELEMENT:
						currentElement = reader.getLocalName();
						break;
					case XMLStreamConstants.CHARACTERS:
						if (currentElement.equals("id") && reader.getText().trim().length() > 0) {
							xmlWritable.setId(reader.getText());
							id = reader.getText();
						} else if (currentElement.equals("first_name") && reader.getText().trim().length() > 0) {
							xmlWritable.setFirst_name(reader.getText());
						} else if (currentElement.equals("last_name") && reader.getText().trim().length() > 0) {
							xmlWritable.setLast_name(reader.getText());
						} else if (currentElement.equals("email") && reader.getText().trim().length() > 0) {
							xmlWritable.setEmail(reader.getText());
						} else if (currentElement.equals("gender") && reader.getText().trim().length() > 0) {
							xmlWritable.setGender(reader.getText());
						} else if (currentElement.equals("ip_address") && reader.getText().trim().length() > 0) {
							xmlWritable.setIp_address(reader.getText());
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			context.write(new Text(id), xmlWritable);
		}

	}

	private static class XMLReducer extends Reducer<Text, XMLWritable, Text, Text> {

		@Override
		protected void reduce(Text arg0, Iterable<XMLWritable> arg1, Reducer<Text, XMLWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String output = "";

			for (XMLWritable obj : arg1) {
				output = "|" + obj.toCSV();
			}
			context.write(arg0, new Text(output));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		conf.set("startTag", "<record>");
		conf.set("endTag", "</record>");

		Job job = Job.getInstance(conf, "XMLParseJob");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(XMLWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(XMLMapper.class);
		job.setReducerClass(XMLReducer.class);

		job.setInputFormatClass(XMLInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		if (args.length == 2) {
			Path p = new Path(args[1]);
			FileSystem fs = FileSystem.get(conf);
			fs.exists(p);
			fs.delete(p, true);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		} else {
			System.exit(0);
		}

		job.waitForCompletion(true);
	}
}
