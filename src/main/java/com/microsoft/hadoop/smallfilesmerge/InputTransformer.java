package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;

/**
 * A transformer for the input files as we merge them into the output files. 
 */
public abstract class InputTransformer {
	public abstract void TransformInput(InputStream inputStream, OutputConsumer consumer)
		throws IOException, InterruptedException;

	public static interface OutputConsumer {
		public void Consume(String output) throws IOException, InterruptedException;
	}
}
