package org.hadoop.pract.latest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordReducerLatest extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(final Text key, final Iterable<IntWritable> values,
			final Context context) throws IOException, InterruptedException {

		int count = 0;
		for (final IntWritable intWritable : values) {

			count += intWritable.get();
		}
		context.write(key, new IntWritable(count));
	}

}
