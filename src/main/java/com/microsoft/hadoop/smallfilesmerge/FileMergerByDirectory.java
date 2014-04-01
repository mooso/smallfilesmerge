package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.w3c.dom.Document;

public class FileMergerByDirectory extends Configured implements Tool {
	
	public static class DirectoryMergeMapper extends Mapper<IntWritable, Text, NullWritable, Text> {
		private Text outputValue = new Text();
		private DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			

		@Override
		protected void map(IntWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Path inputFile = new Path(value.toString());
			//System.out.println("Processing file " + inputFile.toString());
			FileSystem fs = inputFile.getFileSystem(context.getConfiguration());
			FSDataInputStream inputStream = fs.open(inputFile);
			try {
				DocumentBuilder docBuilder;
				try {
					docBuilder = docBuilderFactory.newDocumentBuilder();
				} catch (ParserConfigurationException e) {
					e.printStackTrace();
					return;
				}
				Document doc;
				try {
					doc = docBuilder.parse(inputStream);
				} catch (Exception ex) {
					System.err.println("Encountered parse error - skipping: " + inputFile.toString());
					return;
				}
				outputValue.set(doc.getDocumentElement().toString());
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
		Path[] inputPaths = Utils.stringToPaths(args[0]);
		Path[] outputPaths = Utils.stringToPaths(args[1]);
		if (args.length >= 3 && args[2].trim().equalsIgnoreCase("-popInput")) {
			if (args.length < 5) {
				writeUsage();
				return 1;
			}
			int numDirs = Integer.parseInt(args[3]), numFiles = Integer.parseInt(args[4]);
			System.out.printf("Populating %d directories, each with %d files\n",
					numDirs, numFiles);
			deleteAll(inputPaths);
			Job popJob = configurePopulationJob(inputPaths, numDirs, numFiles);
			long startPopTime = System.currentTimeMillis();
			popJob.submit();
			if (popJob.waitForCompletion(true)) {
				System.out.printf("Done populating - took %d seconds.\n",
						(System.currentTimeMillis() - startPopTime) / 1000);
			} else {
				return 2;
			}
		}
		deleteAll(outputPaths);
		Job job = configureJob(inputPaths, outputPaths);
		long startMergeTime = System.currentTimeMillis();
		job.submit();
		if (job.waitForCompletion(true)) {
			System.out.printf("Done merging - took %d seconds.\n",
					(System.currentTimeMillis() - startMergeTime) / 1000);
		} else {
			return 2;
		}
		return 0;
	}

	private void deleteAll(Path[] paths) throws IOException {
		for (Path path : paths) {
			path.getFileSystem(getConf()).delete(path, true);
		}
	}

	private void writeUsage() {
		System.out.printf(
				"Usage: hadoop jar <jarPath> %s <inputPath> <outputPath> [-popInput numDirs numFiles]\n",
				getClass().getName());
	}

	private Job configurePopulationJob(Path[] outputPaths, int numDirs, int numFiles) throws IOException {
		Job job = new Job(getConf());
		DirectoryPopulatorConfiguration.configure(job.getConfiguration(), outputPaths, numDirs, numFiles);
		job.setJarByClass(getClass());
		job.setMapperClass(DirectoryPopulatorMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(DirectoryPopulatorInputFormat.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(NullOutputFormat.class);
		return job;
	}

	private Job configureJob(Path[] inputPaths, Path[] outputPaths) throws IOException {
		Job job = new Job(getConf());
		CombineDirectoryConfiguration.configureInputPaths(job.getConfiguration(), inputPaths);
		job.setJarByClass(getClass());
		job.setMapperClass(DirectoryMergeMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(CombineDirectoryInputFormat.class);
		job.setNumReduceTasks(0);
		MultiFileOutputFormat.setOutputDirs(job, outputPaths);
		job.setOutputFormatClass(MultiFileOutputFormat.class);
		return job;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new FileMergerByDirectory(), args);
	}
}
