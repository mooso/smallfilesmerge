package com.microsoft.hadoop.smallfilesmerge;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class CombineDirectoryConfiguration {
	public static enum Keys {
		INPUT_DIRECTORY_PATHS("combine.directory.input.paths"),
		NAME_HASH_SPLIT_COUNT("combine.directory.input.num.hash.names");
	
		private final String key;
	
		Keys(String key) {
			this.key = key;
		}
	
		public String getKey() {
			return key;
		}
	}

	public static Path[] getInputDirectoryPath(Configuration conf) {
		return Utils.stringToPaths(conf.get(Keys.INPUT_DIRECTORY_PATHS.getKey()));
	}
	
	public static int getNumNameHashSplits(Configuration conf) {
		return conf.getInt(Keys.NAME_HASH_SPLIT_COUNT.getKey(), 1);
	}
	
	public static void configureInputPaths(Configuration conf, Path[] inputPaths) {
		conf.set(Keys.INPUT_DIRECTORY_PATHS.getKey(), Utils.pathsToString(inputPaths));
	}
	
	public static void setNumNameHashSplits(Configuration conf, int numNameHashSplits) {
		conf.setInt(Keys.NAME_HASH_SPLIT_COUNT.getKey(), numNameHashSplits);
	}
}
