package com.etl.util;

public class SchemaLoader {

	public static String[] loadSchema(String fileSchema) {

		String[] cols = fileSchema.split(",");

		for (int i = 0; i < cols.length; i++) {
			cols[i] = cols[i].trim();
		}

		return cols;

	}
}
