package com.microsoft.hadoop.smallfilesmerge;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

/**
 * The configuration helper/keys for the merger. 
 */
public class CombineDirectoryConfiguration {
	public static enum Keys {
		INPUT_DIRECTORY_PATHS("combine.directory.input.paths"),
		NAME_HASH_SPLIT_COUNT("combine.directory.input.num.hash.names"),
		INPUT_DIRECTORY_ACCOUNT_KEY("combine.directory.account.key"),
		PREVIOUS_JOB_ATTEMPT_OUTPUT("combine.directory.previous.job.attempt.output"),
		INPUT_TRANSFORMER_CLASS("combine.directory.input.transformer");

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

	public static String getAccountExplicitKey(Configuration conf) {
		return conf.get(Keys.INPUT_DIRECTORY_ACCOUNT_KEY.getKey());
	}

	public static int getNumNameHashSplits(Configuration conf) {
		return conf.getInt(Keys.NAME_HASH_SPLIT_COUNT.getKey(), 10);
	}

	public static Class<? extends InputTransformer> getInputTransformerClass(Configuration conf) {
		return conf.getClass(Keys.INPUT_TRANSFORMER_CLASS.getKey(), IdentityInputTransformer.class,
				InputTransformer.class);
	}

	public static Path getPreviousJobAttemptOutput(Configuration conf) {
		String path = conf.get(Keys.PREVIOUS_JOB_ATTEMPT_OUTPUT.getKey());
		if (path != null) {
			return new Path(path);
		} else {
			return null;
		}
	}

	public static void configureInputPaths(Configuration conf, Path[] inputPaths) {
		conf.set(Keys.INPUT_DIRECTORY_PATHS.getKey(), Utils.pathsToString(inputPaths));
	}

	public static void setNumNameHashSplits(Configuration conf, int numNameHashSplits) {
		conf.setInt(Keys.NAME_HASH_SPLIT_COUNT.getKey(), numNameHashSplits);
	}
}
