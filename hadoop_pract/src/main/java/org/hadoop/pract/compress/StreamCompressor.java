package org.hadoop.pract.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * echo "Text" | hadoop StreamCompressor org.apache.hadoop.io.compress.GzipCodec
 * | gunzip -
 * 
 * @author RANJAN
 *
 */
public class StreamCompressor {

	public static void main(final String[] args) throws Exception {
		final String codecClassname = args[0];
		final Class<?> codecClass = Class.forName(codecClassname);
		final Configuration conf = new Configuration();
		final CompressionCodec codec = (CompressionCodec) ReflectionUtils
				.newInstance(codecClass, conf);
		final CompressionOutputStream out = codec
				.createOutputStream(System.out);
		IOUtils.copyBytes(System.in, out, 4096, false);
		out.finish();
	}
}
