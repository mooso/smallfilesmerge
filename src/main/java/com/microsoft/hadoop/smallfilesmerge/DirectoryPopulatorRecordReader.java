package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class DirectoryPopulatorRecordReader extends RecordReader<IntWritable, Text> {
	private IntWritable currentIndex;
	private int total;
	private Path directory;

	@Override
	public void close() throws IOException {
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return currentIndex;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return new Text(new Path(directory, "F" + currentIndex.get()).toString());
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float)currentIndex.get() / (float)total;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		directory = ((DirectoryPopulatorInputSplit)split).getDirectory();
		currentIndex = new IntWritable(-1);
		total = DirectoryPopulatorConfiguration.getNumFilesPerDirectory(context.getConfiguration());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		currentIndex.set(currentIndex.get() + 1);
		return currentIndex.get() < total;
	}
}
