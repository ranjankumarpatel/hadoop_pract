package org.hadoop.pract.latest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordMapperLatest extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(final LongWritable key, final Text value,
			final Context context) throws IOException, InterruptedException {
		final String s = value.toString();
		for (final String word : s.split(" ")) {
			if (!word.isEmpty()) {
				context.write(new Text(word), new IntWritable(1));
			}
		}

	}

}
