package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class MultiFileOutputFormat<K, V> extends OutputFormat<K, V> {
	private FileOutputFormat<K, V> innerFormat = new TextOutputFormat<K, V>();
	private static final String FILE_OUTPUT_DIR = "mapred.output.dir";
	
	public static final String OUTPUT_DIRS = "multi.file.out.dirs";
	
	private Path[] getOutputDirs(Configuration conf) {
		String confValue = conf.get(OUTPUT_DIRS);
		return confValue == null ? null : Utils.stringToPaths(confValue);
	}
	
	public static void setOutputDirs(Job job, Path[] outputDirs) {
		job.getConfiguration().set(OUTPUT_DIRS, Utils.pathsToString(outputDirs));
	}

	@Override
	public void checkOutputSpecs(JobContext jobContext) throws IOException,
			InterruptedException {
		Path[] outputDirs = getOutputDirs(jobContext.getConfiguration());
		if (outputDirs == null) {
      throw new InvalidJobConfException("Output directories not set.");
		}
		for (Path dir : outputDirs) {
			jobContext.getConfiguration().set(FILE_OUTPUT_DIR, dir.toString());
			innerFormat.checkOutputSpecs(jobContext);
		}
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext taskContext)
			throws IOException, InterruptedException {
		setChosenOutputDir(taskContext);
		return innerFormat.getOutputCommitter(taskContext);
	}

	private void setChosenOutputDir(TaskAttemptContext taskContext) {
		int taskId = taskContext.getTaskAttemptID().getTaskID().getId();
		Path[] outputDirs = getOutputDirs(taskContext.getConfiguration());
		int chosenId = taskId % outputDirs.length;
		taskContext.getConfiguration().set(FILE_OUTPUT_DIR, outputDirs[chosenId].toString());
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext taskContext)
			throws IOException, InterruptedException {
		setChosenOutputDir(taskContext);
		return innerFormat.getRecordWriter(taskContext);
	}

}
