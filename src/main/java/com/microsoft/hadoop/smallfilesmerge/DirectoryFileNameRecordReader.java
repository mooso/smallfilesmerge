package com.microsoft.hadoop.smallfilesmerge;

import java.io.IOException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class DirectoryFileNameRecordReader
	extends RecordReader<IntWritable, Text> {
	private FileStatus[] allFiles;
	private int currentLocation;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Path myDir = ((CombineDirectoryInputSplit)split).getDirectoryPath();
		allFiles = myDir.getFileSystem(context.getConfiguration()).listStatus(myDir);
		currentLocation = -1;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return new IntWritable(currentLocation);
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return new Text(allFiles[currentLocation].getPath().toString());
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float)currentLocation / (float)allFiles.length;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		currentLocation++;
		return currentLocation < allFiles.length;
	}

}
