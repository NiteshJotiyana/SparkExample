package com.etl.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.etl.pojo.InputComponent;
import com.etl.util.SchemaLoader;

public class CSVReader {

	public Dataset<Row> read(SparkSession session, InputComponent in) {
		System.out.println("loading component id : " + in.getSequence_id());
		Dataset<Row> dataset = session.read().option("delimiter", in.getFile_delimiter()).csv(in.getFile_path())
				.toDF(SchemaLoader.loadSchema(in.getFile_schema()));
		return dataset;

	}

}
