package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class CombineDirectoryInputSplit extends InputSplit implements Writable {
	private String directoryPath;
	private int nameHashSlot;

	public CombineDirectoryInputSplit() {}
	
	public CombineDirectoryInputSplit(String directoryPath, int nameHashSlot) {
		this.directoryPath = directoryPath;
		this.nameHashSlot = nameHashSlot;
	}
	
	public Path getDirectoryPath() {
		return new Path(directoryPath);
	}

	public int getNameHashSlot() {
		return nameHashSlot;
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
		out.writeUTF(directoryPath);
		out.writeInt(nameHashSlot);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		directoryPath = in.readUTF();
		nameHashSlot = in.readInt();
	}
}
