package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class CombineDirectoryInputSplit extends InputSplit implements Writable {
	private String directoryPath;

	public CombineDirectoryInputSplit() {}
	
	public CombineDirectoryInputSplit(String directoryPath) {
		this.directoryPath = directoryPath;
	}
	
	public Path getDirectoryPath() {
		return new Path(directoryPath);
	}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		return 0;
	}
	
	@Override
	public String[] getLocations() throws IOException {
	 // We don't know where the blocks will be, so return "localhost"
	 return new String[] { "localhost" };
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, directoryPath);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		directoryPath = Text.readString(in);
	}
}
