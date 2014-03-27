package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class FileMergerByDirectory extends Configured implements Tool {
	
	public static class DirectoryMergeMapper extends Mapper<IntWritable, Text, NullWritable, Text> {
		private Text outputValue = new Text();

		@Override
		protected void map(IntWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Path inputFile = new Path(value.toString());
			System.out.println("Processing file " + inputFile.toString());
			FileSystem fs = inputFile.getFileSystem(context.getConfiguration());
			FSDataInputStream inputStream = fs.open(inputFile);
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
				String line;
				while ((line = reader.readLine()) != null) {
					outputValue.set(line);
					context.write(NullWritable.get(), outputValue);
				}
				reader.close();
			} finally {
				inputStream.close();
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			writeUsage();
			return 1;
		}
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		if (args.length >= 3 && args[2].trim().equalsIgnoreCase("-popInput")) {
			if (args.length < 5) {
				writeUsage();
				return 1;
			}
			int numDirs = Integer.parseInt(args[3]), numFiles = Integer.parseInt(args[4]);
			System.out.printf("Populating %d directories, each with %d files\n",
					numDirs, numFiles);
			inputPath.getFileSystem(getConf()).delete(inputPath, true);
			Job popJob = configurePopulationJob(inputPath, numDirs, numFiles);
			popJob.submit();
			if (popJob.waitForCompletion(true)) {
				System.out.println("Done populating!");
			} else {
				return 2;
			}
		}
		outputPath.getFileSystem(getConf()).delete(outputPath, true);
		Job job = configureJob(inputPath, outputPath);
		job.submit();
		if (job.waitForCompletion(true)) {
			System.out.println("Done with everything!");
		} else {
			return 2;
		}
		return 0;
	}

	private void writeUsage() {
		System.out.printf(
				"Usage: hadoop jar <jarPath> %s <inputPath> <outputPath> [-popInput numDirs numFiles]\n",
				getClass().getName());
	}

	private Job configurePopulationJob(Path outputPath, int numDirs, int numFiles) throws IOException {
		Job job = new Job(getConf());
		DirectoryPopulatorConfiguration.configure(job.getConfiguration(), outputPath, numDirs, numFiles);
		job.setJarByClass(getClass());
		job.setMapperClass(DirectoryPopulatorMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(DirectoryPopulatorInputFormat.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(NullOutputFormat.class);
		return job;
	}

	private Job configureJob(Path inputPath, Path outputPath) throws IOException {
		Job job = new Job(getConf());
		CombineDirectoryConfiguration.configureInputPath(job.getConfiguration(), inputPath);
		job.setJarByClass(getClass());
		job.setMapperClass(DirectoryMergeMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(CombineDirectoryInputFormat.class);
		job.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new FileMergerByDirectory(), args);
	}
}
