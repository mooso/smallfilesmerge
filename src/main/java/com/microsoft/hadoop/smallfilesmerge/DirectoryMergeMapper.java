package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;
import java.nio.charset.Charset;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class DirectoryMergeMapper extends Mapper<IntWritable, Text, NullWritable, Text> {
	private Text outputValue = new Text();
	private InputTransformer inputTransformer;

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Class<? extends InputTransformer> transformerClass =
				CombineDirectoryConfiguration.getInputTransformerClass(context.getConfiguration());
		try {
			inputTransformer = transformerClass.newInstance();
		} catch (InstantiationException e) {
			throw new IllegalArgumentException("Error while instantiating transformer.", e);
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException("Error while instantiating transformer.", e);
		}
	};

	@Override
	protected void map(IntWritable key, Text value, final Context context)
			throws IOException, InterruptedException {
		if (value.toString().startsWith(DirectoryFileNameRecordReader.TAKE_PREVIOUS_PREFIX)) {
			writePreviousMapOutput(value, context);
			return;
		}
		Path inputFile = new Path(value.toString());
		FileSystem fs = inputFile.getFileSystem(context.getConfiguration());
		FSDataInputStream inputStream = fs.open(inputFile);
		try {
			inputTransformer.TransformInput(inputStream, new InputTransformer.OutputConsumer() {
				@Override
				public void Consume(String output) throws IOException, InterruptedException {
					outputValue.set(output);
					context.write(NullWritable.get(), outputValue);
				}
			});
		} finally {
			inputStream.close();
		}
	}

	private void writePreviousMapOutput(Text value, Context context)
			throws IOException, InterruptedException {
		Path previousFile = new Path(value.toString().substring(
				DirectoryFileNameRecordReader.TAKE_PREVIOUS_PREFIX.length()));
		FileSystem previousFs = previousFile.getFileSystem(context.getConfiguration());
		InputStream previousStream = previousFs.open(previousFile);
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(previousStream, Charset.forName("UTF-8")));
		String currentLine;
		while ((currentLine = reader.readLine()) != null) {
			outputValue.set(currentLine);
			context.write(NullWritable.get(), outputValue);
			context.progress();
		}
		reader.close();
		previousStream.close();
	}
}
