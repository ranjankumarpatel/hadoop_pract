package org.hadoop.pract.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 *
 */
public class FileCopyWithProgress {
	public static void main(final String[] args) throws Exception {
		final String localSrc = args[0];
		final String dst = args[1];
		final InputStream in = new BufferedInputStream(new FileInputStream(
				localSrc));
		final Configuration conf = new Configuration();
		final FileSystem fs = FileSystem.get(URI.create(dst), conf);
		final OutputStream out = fs.create(new Path(dst), new Progressable() {
			@Override
			public void progress() {
				System.out.print(".");
			}
		});
		IOUtils.copyBytes(in, out, 4096, true);
	}
}
