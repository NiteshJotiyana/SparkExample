package com.etl.pojo;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class Component implements Comparable<Component> {

	int sequence_id;

	public abstract void execute(SparkSession session, Map<String, Dataset<Row>> map);

	@Override
	public int compareTo(Component o) {

		return new Integer(this.sequence_id).compareTo(o.sequence_id);
	}

	public int getSequence_id() {
		return sequence_id;
	}

	public void setSequence_id(int sequence_id) {
		this.sequence_id = sequence_id;
	}

}
