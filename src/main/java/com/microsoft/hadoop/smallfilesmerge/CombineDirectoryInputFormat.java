package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class CombineDirectoryInputFormat extends InputFormat<IntWritable, Text> {
	@Override
	public RecordReader<IntWritable, Text> createRecordReader(
			InputSplit split,
			TaskAttemptContext context)
					throws IOException, InterruptedException {
		return new DirectoryFileNameRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context)
			throws IOException,
			InterruptedException {
		Configuration job = context.getConfiguration();
		ArrayList<InputSplit> ret = new ArrayList<InputSplit>();
		Path inputPath = CombineDirectoryConfiguration.getInputDirectoryPath(job);
		FileStatus[] list = inputPath.getFileSystem(job).listStatus(inputPath);
		for (FileStatus current : list) {
			if (current.isDir()) {
				ret.add(new CombineDirectoryInputSplit(current.getPath().toString()));
			}
		}
		return ret;
	}
}