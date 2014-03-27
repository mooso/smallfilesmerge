package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class DirectoryPopulatorInputFormat extends InputFormat<IntWritable, Text> {

	@Override
	public RecordReader<IntWritable, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new DirectoryPopulatorRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		int numDirectories = DirectoryPopulatorConfiguration.getNumDirectories(context.getConfiguration());
		Path[] outputPaths = DirectoryPopulatorConfiguration.getOutputDirectoryPaths(context.getConfiguration());
		int currentOutputPathIndex = 0;
		ArrayList<InputSplit> ret = new ArrayList<InputSplit>();
		for (int i = 0; i < numDirectories; i++) {
			ret.add(new DirectoryPopulatorInputSplit(new Path(outputPaths[currentOutputPathIndex], "Dir" + i)));
			currentOutputPathIndex = (currentOutputPathIndex + 1) % outputPaths.length;
		}
		return ret;
	}
}
