package org.hadoop.pract.basic;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class WordCount extends Configured implements Tool {
	@Override
	public int run(final String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Please give input and output directories.");
			return -1;
		}
		final JobConf conf = new JobConf(WordCount.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.setMapperClass(WordMapper.class);
		conf.setReducerClass(WordReducer.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		JobClient.runJob(conf);
		return 0;
	}

	static class WordMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(final LongWritable key, final Text value,
				final OutputCollector<Text, IntWritable> output,
				final Reporter r) throws IOException {
			final String s = value.toString();
			String[] arrayOfString;
			final int j = (arrayOfString = s.split(" ")).length;
			for (int i = 0; i < j; i++) {
				final String word = arrayOfString[i];
				if (word.length() > 0) {
					output.collect(new Text(word), new IntWritable(1));
				}
			}
		}
	}

	static class WordReducer extends MapReduceBase implements
	Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(final Text key, final Iterator<IntWritable> values,
				final OutputCollector<Text, IntWritable> output,
				final Reporter r) throws IOException {
			int count = 0;
			while (values.hasNext()) {
				final IntWritable i = values.next();
				count += i.get();
			}
			output.collect(key, new IntWritable(count));
		}
	}

	public static void main(final String[] args) throws Exception {
		final int exitCode = ToolRunner.run(new WordCount(), args);
		System.exit(exitCode);
	}
}
