package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;

public class IdentityInputTransformer extends InputTransformer {

	@Override
	public void TransformInput(InputStream inputStream, OutputConsumer consumer) throws IOException, InterruptedException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		String line;
		while ((line = reader.readLine()) != null) {
			consumer.Consume(line);
		}
	}
}
