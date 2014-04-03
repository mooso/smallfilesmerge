package com.microsoft.hadoop.smallfilesmerge;

import java.io.*;

import javax.xml.stream.*;

/**
 * An input transformer that validates XML input files as it's merging them.
 */
public class XmlInputTransformer extends InputTransformer {
	private XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
	private XMLInputFactory inputFactory = XMLInputFactory.newInstance();

	@Override
	public void TransformInput(InputStream inputStream, OutputConsumer consumer)
			throws IOException, InterruptedException {
		StringWriter stringWriter = new StringWriter();
		try {
			XMLEventReader reader = inputFactory.createXMLEventReader(inputStream);
			XMLEventWriter writer = outputFactory.createXMLEventWriter(stringWriter);
			writer.add(reader);
			writer.close();
		} catch (XMLStreamException e) {
			System.err.println("Encountered parse error - skipping.");
			e.printStackTrace();
			return;
		} catch (ArrayIndexOutOfBoundsException e) {
			System.err.println("Encountered parse error due to JVM XML parser bug - skipping.");
			e.printStackTrace();
			return;
		}
		consumer.Consume(stringWriter.toString());
	}

}
