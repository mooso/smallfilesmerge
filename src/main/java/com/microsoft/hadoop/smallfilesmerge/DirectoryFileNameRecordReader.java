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
	private ArrayList<FileStatus> allFiles;
	private int currentLocation;
	private int nameHashSlot;
	private int numNameHashSlots;
	private Iterator<ListBlobItem> blobs;
	private Path currentPath;
	private Path myDir;
	private FileSystem fs;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		myDir = ((CombineDirectoryInputSplit)split).getDirectoryPath();
		nameHashSlot = ((CombineDirectoryInputSplit)split).getNameHashSlot();
		numNameHashSlots = getNumNameHashSplits(context.getConfiguration());
		fs = myDir.getFileSystem(context.getConfiguration());
		String accountKey = getAccountExplicitKey(context.getConfiguration());
		if (accountKey != null) {
			addUsingStorageApi(accountKey);
		} else {
			addUsingFileSystem();
			currentLocation = -1;
		}
	}

	private void addUsingFileSystem() throws IOException {
		FileStatus[] obtained = fs.listStatus(myDir);
		allFiles = new ArrayList<FileStatus>(obtained.length + 1024);
		for (FileStatus current : obtained) {
			addAllFiles(fs, current);
		}
	}

	private void addUsingStorageApi(String accountKey) throws IOException {
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
		} catch (URISyntaxException e) {
			throw new IOException(e);
		} catch (StorageException e) {
			throw new IOException(e);
		}
	}
	
	private void addAllFiles(FileSystem fs, FileStatus source) throws IOException {
		if (source.isDir()) {
			for (FileStatus child : fs.listStatus(source.getPath())) {
				addAllFiles(fs, child);
			}
		} else {
			if (doesMatchNameHash(source)) {
				allFiles.add(source);
			}
		}
	}

	private boolean doesMatchNameHash(FileStatus source) {
		return doesMatchNameHash(source.getPath().getName());
	}

	private boolean doesMatchNameHash(ListBlobItem blob) {
		String[] pathComponents = blob.getUri().getPath().split("/");
		return doesMatchNameHash(pathComponents[pathComponents.length - 1]);
	}

	private boolean doesMatchNameHash(String name) {
		return numNameHashSlots == 1 ||
				Math.abs(name.hashCode() % numNameHashSlots) == nameHashSlot;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return new IntWritable(currentLocation);
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		if (allFiles != null) {
			return new Text(allFiles.get(currentLocation).getPath().toString());
		} else {
			return new Text(currentPath.toString());
		}
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (allFiles != null) {
			return (float)currentLocation / (float)allFiles.size();
		} else {
			if (!blobs.hasNext()) {
				return 1.0f;
			} else {
				return 0.5f;
			}
		}
	}

	private static long getBlobLength(ListBlobItem blob) {
		if (!CloudBlob.class.isInstance(blob)) {
			return 0;
		}
		CloudBlob asCloudBlob = (CloudBlob)blob;
		return asCloudBlob.getProperties().getLength();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (allFiles != null) {
			currentLocation++;
			return currentLocation < allFiles.size();
		} else {
			while (blobs.hasNext()) {
				ListBlobItem currentBlob = blobs.next();
				if (doesMatchNameHash(currentBlob) && getBlobLength(currentBlob) > 0) {
					String[] pathComponents = currentBlob.getUri().getPath().split("/");
					String pathWithoutContainer =
							currentBlob.getUri().getPath().substring(pathComponents[1].length() + 1);
					currentPath = new Path(myDir.toUri().getScheme(), myDir.toUri().getAuthority(),
							pathWithoutContainer);
					return true;
				}
			}
			return false;
		}
	}

}
