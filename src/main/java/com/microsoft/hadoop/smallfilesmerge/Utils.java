package com.microsoft.hadoop.smallfilesmerge;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;

class Utils {

	public static Path[] stringToPaths(String input) {
		String[] splitString = StringUtils.split(input);
		Path[] ret = new Path[splitString.length];
		for (int i = 0; i < splitString.length; i++) {
			ret[i] = new Path(splitString[i]);
		}
		return ret;
	}

	public static String pathsToString(Path[] paths) {
		StringBuilder pathsBuilder = new StringBuilder();
		boolean needComma = false;
		for (Path current : paths) {
			if (needComma) {
				pathsBuilder.append(',');
			}
			pathsBuilder.append(current.toString());
			needComma = true;
		}
		return pathsBuilder.toString();
	}
}
