package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class DirectoryFileNameRecordReader
	extends RecordReader<IntWritable, Text> {
	private ArrayList<FileStatus> allFiles;
	private int currentLocation;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Path myDir = ((CombineDirectoryInputSplit)split).getDirectoryPath();
		FileSystem fs = myDir.getFileSystem(context.getConfiguration());
		FileStatus[] obtained = fs.listStatus(myDir);
		int nameHashSlot = ((CombineDirectoryInputSplit)split).getNameHashSlot();
		int numNameHashSlots =
				CombineDirectoryConfiguration.getNumNameHashSplits(context.getConfiguration());
		allFiles = new ArrayList<FileStatus>(obtained.length + 1024);
		for (FileStatus current : obtained) {
			addAllFiles(fs, current, nameHashSlot, numNameHashSlots);
		}
		currentLocation = -1;
	}
	
	private void addAllFiles(FileSystem fs, FileStatus source,
			int nameHashSlot, int numNameHashSlots) throws IOException {
		if (source.isDir()) {
			for (FileStatus child : fs.listStatus(source.getPath())) {
				addAllFiles(fs, child, nameHashSlot, numNameHashSlots);
			}
		} else {
			if (numNameHashSlots == 1 ||
					(source.getPath().getName().hashCode() % numNameHashSlots) == nameHashSlot) {
				allFiles.add(source);
			}
		}
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
		return new Text(allFiles.get(currentLocation).getPath().toString());
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float)currentLocation / (float)allFiles.size();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		currentLocation++;
		return currentLocation < allFiles.size();
	}

}
