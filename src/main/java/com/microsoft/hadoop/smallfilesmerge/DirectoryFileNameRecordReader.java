package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.microsoft.windowsazure.services.blob.client.*;
import com.microsoft.windowsazure.services.core.storage.*;

import static com.microsoft.hadoop.smallfilesmerge.CombineDirectoryConfiguration.*;

public class DirectoryFileNameRecordReader
	extends RecordReader<IntWritable, Text> {
	private int nameHashSlot;
	private int numNameHashSlots;
	private Path myDir;
	public static final String TAKE_PREVIOUS_PREFIX = "TakePrevious:";
	private InnerReader readerUsed;

	private boolean doesMatchNameHash(String name) {
		return numNameHashSlots == 1 ||
				Math.abs(name.hashCode() % numNameHashSlots) == nameHashSlot;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		myDir = ((CombineDirectoryInputSplit)split).getDirectoryPath();
		nameHashSlot = ((CombineDirectoryInputSplit)split).getNameHashSlot();
		numNameHashSlots = getNumNameHashSplits(context.getConfiguration());
		InnerReader[] potentialReaders = new InnerReader[] {
				new TakeFromPreviousAttemptReader(),
				new UseStorageApiReader(),
				new UseFileSystemReader()
		};
		for (InnerReader currentReader : potentialReaders) {
			if (currentReader.initialize(context)) {
				readerUsed = currentReader;
				break;
			}
		}
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return new IntWritable(readerUsed.getCurrentKey());
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return new Text(readerUsed.getCurrentValue());
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return readerUsed.getProgress();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return readerUsed.nextKeyValue();
	}

	private abstract class InnerReader {
		public abstract boolean initialize(TaskAttemptContext context) throws IOException;
		public abstract int getCurrentKey();
		public abstract String getCurrentValue();
		public abstract boolean nextKeyValue() throws IOException, InterruptedException;
		public abstract float getProgress();
	}

	private class TakeFromPreviousAttemptReader extends InnerReader {
		private Path previousOutputPath;
		private boolean previousPathGiven = false;

		/**
		 * Checks if we should skip this task attempt, and instead get the output from
		 * a previous job's output whose task ID is the same as us (assumes tasks with
		 * same ID have the same output).
		 * @param context
		 * @return
		 * @throws IOException
		 */
		@Override
		public boolean initialize(TaskAttemptContext context) throws IOException {
			Path previousJobAttemptPath = getPreviousJobAttemptOutput(context.getConfiguration());
			if (previousJobAttemptPath == null) {
				return false;
			}
			FileSystem mapOutputFs = previousJobAttemptPath.getFileSystem(context.getConfiguration());
			FileStatus[] mapOutputs = mapOutputFs.listStatus(previousJobAttemptPath);
			for (FileStatus currentOutput : mapOutputs) {
				// Assuming task output names of the form part-m-01234.
				String[] parts = currentOutput.getPath().getName().split("-");
				if (parts.length == 3) {
					try {
						int currentOutputId = Integer.parseInt(parts[2]);
						if (currentOutputId == context.getTaskAttemptID().getTaskID().getId()) {
							previousOutputPath = currentOutput.getPath();
							System.out.println("Skipping and taking the previous attempt's output from " +
									previousOutputPath + " instead.");
							return true;
						}
					} catch (NumberFormatException ex) {
						// Ignore.
					}
				}
			}
			return false;
		}

		@Override
		public int getCurrentKey() {
			return 0;
		}

		@Override
		public String getCurrentValue() {
			// Taking-previous-path mode.
			return TAKE_PREVIOUS_PREFIX + previousOutputPath.toString();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (previousPathGiven) {
				return false;
			} else {
				previousPathGiven = true;
				return true;
			}
		}

		@Override
		public float getProgress() {
			return previousPathGiven ? 1.0f : 0.5f;
		}
	}

	private class UseFileSystemReader extends InnerReader {
		private int currentLocation;
		private ArrayList<FileStatus> allFiles;
		private FileSystem fs;

		@Override
		public boolean initialize(TaskAttemptContext context) throws IOException {
			fs = myDir.getFileSystem(context.getConfiguration());
			FileStatus[] obtained = fs.listStatus(myDir);
			allFiles = new ArrayList<FileStatus>(obtained.length + 1024);
			for (FileStatus current : obtained) {
				addAllFiles(fs, current);
			}
			currentLocation = -1;
			return true;
		}

		private void addAllFiles(FileSystem fs, FileStatus source) throws IOException {
			if (source.isDir()) {
				for (FileStatus child : fs.listStatus(source.getPath())) {
					addAllFiles(fs, child);
				}
			} else {
				if (doesMatchNameHash(source.getPath().getName())) {
					allFiles.add(source);
				}
			}
		}

		@Override
		public int getCurrentKey() {
			return currentLocation;
		}

		@Override
		public String getCurrentValue() {
			return allFiles.get(currentLocation).getPath().toString();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			currentLocation++;
			return currentLocation < allFiles.size();
		}

		@Override
		public float getProgress() {
			return (float)currentLocation / (float)allFiles.size();
		}
	}

	private class UseStorageApiReader extends InnerReader {
		private Path currentPath;
		private Iterator<ListBlobItem> blobs;
		private int currentLocation;

		@Override
		public boolean initialize(TaskAttemptContext context) throws IOException {
			String accountKey = getAccountExplicitKey(context.getConfiguration());
			if (accountKey == null) {
				return false;
			}
			String[] authComponents = myDir.toUri().getAuthority().split("@");
			String accountName = authComponents[1].split("\\.")[0];
			String containerName = authComponents[0];
			StorageCredentials creds = new StorageCredentialsAccountAndKey(accountName,
					accountKey);
			try {
				CloudStorageAccount account = new CloudStorageAccount(creds);
				CloudBlobClient client = account.createCloudBlobClient();
				CloudBlobContainer container = client.getContainerReference(containerName);
				blobs = container.listBlobs(myDir.toUri().getPath().substring(1) + "/", true,
						EnumSet.noneOf(BlobListingDetails.class), null, null).iterator();
				currentLocation = -1;
			} catch (URISyntaxException e) {
				throw new IOException(e);
			} catch (StorageException e) {
				throw new IOException(e);
			}
			return true;
		}

		@Override
		public int getCurrentKey() {
			return currentLocation;
		}

		@Override
		public String getCurrentValue() {
			return currentPath.toString();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			while (blobs.hasNext()) {
				ListBlobItem currentBlob = blobs.next();
				if (doesBlobMatchNameHash(currentBlob) && getBlobLength(currentBlob) > 0) {
					String[] pathComponents = currentBlob.getUri().getPath().split("/");
					String pathWithoutContainer =
							currentBlob.getUri().getPath().substring(pathComponents[1].length() + 1);
					currentPath = new Path(myDir.toUri().getScheme(), myDir.toUri().getAuthority(),
							pathWithoutContainer);
					currentLocation++;
					return true;
				}
			}
			return false;
		}

		private boolean doesBlobMatchNameHash(ListBlobItem blob) {
			String[] pathComponents = blob.getUri().getPath().split("/");
			return doesMatchNameHash(pathComponents[pathComponents.length - 1]);
		}

		private long getBlobLength(ListBlobItem blob) {
			if (!CloudBlob.class.isInstance(blob)) {
				return 0;
			}
			CloudBlob asCloudBlob = (CloudBlob)blob;
			return asCloudBlob.getProperties().getLength();
		}

		@Override
		public float getProgress() {
			if (!blobs.hasNext()) {
				return 1.0f;
			} else {
				return 0.5f;
			}
		}
	}
}
