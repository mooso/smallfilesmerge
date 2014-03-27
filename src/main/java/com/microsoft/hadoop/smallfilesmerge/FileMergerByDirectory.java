package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FileMergerByDirectory extends Configured implements Tool {
	
	public static class DirectoryMergeMapper extends Mapper<IntWritable, Text, IntWritable, Text> {
		private Text outputValue = new Text();

		@Override
		protected void map(IntWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Path inputDir = new Path(value.toString());
			FileSystem fs = inputDir.getFileSystem(context.getConfiguration());
			for (FileStatus currentFile : fs.listStatus(inputDir)) {
				FSDataInputStream inputStream = fs.open(currentFile.getPath());
				try {
					BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
					String line;
					while ((line = reader.readLine()) != null) {
						outputValue.set(line);
						context.write(key, outputValue);
					}
				} finally {
					inputStream.close();
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.printf(
					"Usage: hadoop jar <jarPath> %s <inputPath> <outputPath> [-popInput]\n",
					getClass().getName());
			return 1;
		}
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		if (args.length >= 3 && args[2].trim().equalsIgnoreCase("-popInput")) {
			int numDirs = 2, numFiles = 5, numLines = 3;
			System.out.printf("Populating %d directories, each with %d files, each with %d lines",
					numDirs, numFiles, numLines);
			FileSystem fs = inputPath.getFileSystem(getConf());
			for (int dirIndex = 0; dirIndex < numDirs; dirIndex++) {
				Path newDir = new Path(inputPath, "dir" + dirIndex);
				fs.delete(newDir, true);
				fs.mkdirs(newDir);
				for (int fileIndex = 0; fileIndex < numFiles; fileIndex++) {
					Path newFile = new Path(newDir, "file" + fileIndex);
					FSDataOutputStream outputStream = fs.create(newFile);
					try {
						BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
						for (int lineIndex = 0; lineIndex < numLines; lineIndex++) {
							writer.write("F" + fileIndex + "D" + dirIndex + "L" + lineIndex);
						}
					} finally {
						outputStream.close();
					}
				}
			}
			System.out.printf("Done!");
		}
		outputPath.getFileSystem(getConf()).delete(outputPath, true);
		Job job = configureJob(inputPath, outputPath);
		job.submit();
		if (job.waitForCompletion(true)) {
			System.out.println("Done!");
		} else {
			return 2;
		}
		return 0;
	}

	private Job configureJob(Path inputPath, Path outputPath) throws IOException {
		Job job = new Job(getConf());
		CombineDirectoryConfiguration.configureInputPath(job.getConfiguration(), inputPath);
		job.setJarByClass(getClass());
		job.setMapperClass(DirectoryMergeMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(CombineDirectoryInputFormat.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new FileMergerByDirectory(), args);
	}
}
