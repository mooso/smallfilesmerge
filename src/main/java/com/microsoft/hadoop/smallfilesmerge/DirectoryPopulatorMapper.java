package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class DirectoryPopulatorMapper extends
		Mapper<IntWritable, Text, NullWritable, NullWritable> {

	@Override
	protected void map(IntWritable key, Text value, Context context) throws IOException,
			InterruptedException {
		Path outputFile = new Path(value.toString());
		FileSystem fs = outputFile.getFileSystem(context.getConfiguration());
		OutputStream out = fs.create(outputFile);
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
			writer.write("Dummy content");
			writer.newLine();
			writer.close();
		} finally {
			out.close();
		}
	}
}
