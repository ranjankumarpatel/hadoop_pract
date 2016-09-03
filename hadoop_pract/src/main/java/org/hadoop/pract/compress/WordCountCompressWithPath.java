package org.hadoop.pract.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hadoop.pract.latest.WordMapperLatest;
import org.hadoop.pract.latest.WordReducerLatest;

public class WordCountCompressWithPath extends Configured implements Tool {

	@Override
	public int run(final String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Please give input and output directories.");
			return -1;
		}

		final Configuration conf = new Configuration();
		conf.setBoolean("mapred.compress.map.output", true);
		conf.setClass("mapred.map.output.compression.codec", GzipCodec.class,
				CompressionCodec.class);

		final Job job = new Job(conf);
		job.setJarByClass(WordCountCompressWithPath.class);
		job.setJobName("Word Count Latest");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(WordMapperLatest.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		job.setCombinerClass(WordReducerLatest.class);
		job.setReducerClass(WordReducerLatest.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(final String args[]) throws Exception {

		final int exitCode = ToolRunner.run(new WordCountCompressWithPath(),
				args);
		System.exit(exitCode);
	}

}
