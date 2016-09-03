package org.hadoop.pract.latest;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountLatest extends Configured implements Tool {

	@Override
	public int run(final String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Please give input and output directories.");
			return -1;
		}
		// final JobConf conf = new JobConf(WordCount.class);
		final Job job = new Job();
		job.setJarByClass(WordCountLatest.class);
		job.setJobName("Word Count Latest");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(WordMapperLatest.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(WordReducerLatest.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(final String args[]) throws Exception {

		final int exitCode = ToolRunner.run(new WordCountLatest(), args);
		System.exit(exitCode);
	}

}
