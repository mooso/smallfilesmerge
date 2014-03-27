package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class DirectoryPopulatorInputSplit extends InputSplit implements Writable {
	private String directory;
	
	public DirectoryPopulatorInputSplit() {}
	
	public DirectoryPopulatorInputSplit(Path directory) {
		this.directory = directory.toString();
	}
	
	public Path getDirectory() {
		return new Path(directory);
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		 // It's nowhere...
		 return new String[] { "localhost" };
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		directory = Text.readString(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		Text.writeString(output, directory);
	}
}
