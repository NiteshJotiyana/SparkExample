package com.etl.pojo;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.etl.transformation.SelectTransformation;

public class SelectTransformationComponent extends Component {
	private String entity_name;

	private String input_entity;

	private String trans_type;

	private List<String> columns;

	public void setEntity_name(String entity_name) {
		this.entity_name = entity_name;
	}

	public String getEntity_name() {
		return this.entity_name;
	}

	public void setInput_entity(String input_entity) {
		this.input_entity = input_entity;
	}

	public String getInput_entity() {
		return this.input_entity;
	}

	public void setTrans_type(String trans_type) {
		this.trans_type = trans_type;
	}

	public String getTrans_type() {
		return this.trans_type;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public List<String> getColumns() {
		return this.columns;
	}
	
	

	@Override
	public void execute(SparkSession session, Map<String, Dataset<Row>> map) {

		System.out.println("loading component id : " + this.getSequence_id());
		SelectTransformation st = new SelectTransformation();
		Dataset<Row> dataset = st.select(this, map);

		map.put(this.getEntity_name(), dataset);

	}
}
