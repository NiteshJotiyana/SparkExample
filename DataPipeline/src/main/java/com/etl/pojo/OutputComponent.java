package com.etl.pojo;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.etl.write.CSVWriter;

public class OutputComponent extends Component {
	private String entity_name;

	private String input_type;

	private String file_format;

	private String file_delimiter;

	private String file_path;

	private String file_schema;

	public void setEntity_name(String entity_name) {
		this.entity_name = entity_name;
	}

	public String getEntity_name() {
		return this.entity_name;
	}

	public void setInput_type(String input_type) {
		this.input_type = input_type;
	}

	public String getInput_type() {
		return this.input_type;
	}

	public void setFile_format(String file_format) {
		this.file_format = file_format;
	}

	public String getFile_format() {
		return this.file_format;
	}

	public void setFile_delimiter(String file_delimiter) {
		this.file_delimiter = file_delimiter;
	}

	public String getFile_delimiter() {
		return this.file_delimiter;
	}

	public void setFile_path(String file_path) {
		this.file_path = file_path;
	}

	public String getFile_path() {
		return this.file_path;
	}

	public void setFile_schema(String file_schema) {
		this.file_schema = file_schema;
	}

	public String getFile_schema() {
		return this.file_schema;
	}

	@Override
	public void execute(SparkSession session, Map<String, Dataset<Row>> map) {

		System.out.println("loading component id : " + this.getSequence_id());
		Dataset<Row> dataset = map.get(this.getInput_type());

		CSVWriter csvWriter = new CSVWriter();
		csvWriter.write(dataset, this);
	}

}
