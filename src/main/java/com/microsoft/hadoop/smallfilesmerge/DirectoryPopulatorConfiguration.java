package com.microsoft.hadoop.smallfilesmerge;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class DirectoryPopulatorConfiguration {
	public static enum Keys {
		OUTPUT_DIRECTORY_PATHS("directory.populator.input.paths"),
		NUM_DIRECTORIES("directory.populator.num.directories"),
		NUM_FILES_PER_DIRECTORY("directory.populator.num.files");

		private final String key;

		Keys(String key) {
			this.key = key;
		}

		public String getKey() {
			return key;
		}
	}

	public static Path[] getOutputDirectoryPaths(Configuration conf) {
		String confValue = conf.get(Keys.OUTPUT_DIRECTORY_PATHS.getKey());
		return Utils.stringToPaths(confValue);
	}

	public static int getNumDirectories(Configuration conf) {
		return conf.getInt(Keys.NUM_DIRECTORIES.getKey(), 1);
	}

	public static int getNumFilesPerDirectory(Configuration conf) {
		return conf.getInt(Keys.NUM_FILES_PER_DIRECTORY.getKey(), 1);
	}

	public static void configure(Configuration conf, Path[] outputPaths,
			int numDirectories, int numFilesPerDirectory) {
		conf.set(Keys.OUTPUT_DIRECTORY_PATHS.getKey(), Utils.pathsToString(outputPaths));
		conf.setInt(Keys.NUM_DIRECTORIES.getKey(), numDirectories);
		conf.setInt(Keys.NUM_FILES_PER_DIRECTORY.getKey(), numFilesPerDirectory);
	}

}
