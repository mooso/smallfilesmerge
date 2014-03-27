package com.microsoft.hadoop.smallfilesmerge;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class CombineDirectoryConfiguration {
	public static enum Keys {
		INPUT_DIRECTORY_PATH("combine.directory.input.path");
	
		private final String key;
	
		Keys(String key) {
			this.key = key;
		}
	
		public String getKey() {
			return key;
		}
	}

	public static Path getInputDirectoryPath(Configuration conf) {
		return new Path(conf.get(Keys.INPUT_DIRECTORY_PATH.getKey()));
	}
	
	public static void configureInputPath(Configuration conf, Path inputPath) {
		conf.set(Keys.INPUT_DIRECTORY_PATH.getKey(), inputPath.toString());
	}
}
