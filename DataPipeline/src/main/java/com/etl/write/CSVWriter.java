package com.etl.write;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.etl.pojo.OutputComponent;

public class CSVWriter {

	public void write(Dataset<Row> dataset, OutputComponent out) {

		dataset.write().mode(SaveMode.Overwrite).option("delimiter", out.getFile_delimiter())
				.csv(out.getFile_path() + File.separator + out.getEntity_name());
	}

}
