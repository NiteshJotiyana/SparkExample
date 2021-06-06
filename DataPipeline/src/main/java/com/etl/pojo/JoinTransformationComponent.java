package com.etl.pojo;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.etl.transformation.JoinTransformation;

public class JoinTransformationComponent extends Component {
	private String entity_name;

	private String trans_type;

	private List<String> entities;

	private List<String> join_col;

	public void setEntity_name(String entity_name) {
		this.entity_name = entity_name;
	}

	public String getEntity_name() {
		return this.entity_name;
	}

	public void setTrans_type(String trans_type) {
		this.trans_type = trans_type;
	}

	public String getTrans_type() {
		return this.trans_type;
	}

	public void setEntities(List<String> entities) {
		this.entities = entities;
	}

	public List<String> getEntities() {
		return this.entities;
	}

	public void setJoin_col(List<String> join_col) {
		this.join_col = join_col;
	}

	public List<String> getJoin_col() {
		return this.join_col;
	}

	@Override
	public void execute(SparkSession session, Map<String, Dataset<Row>> map) {
		System.out.println("loading component id : " + this.getSequence_id());

		JoinTransformation jt = new JoinTransformation();
		Dataset<Row> dataset = jt.join(this, map);

		map.put(this.getEntity_name(), dataset);

	}

}
