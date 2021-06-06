package com.etl.transformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.etl.pojo.JoinTransformationComponent;
import com.etl.pojo.SelectTransformationComponent;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class SelectTransformation {

	public Dataset<Row> select(SelectTransformationComponent component, Map<String, Dataset<Row>> H) {

		Dataset<Row> d1 = H.get(component.getInput_entity());
		
		Dataset<Row> selected = d1.select(convertListToSeq(component.getColumns()));

		return selected;

	}

	public Seq<Column> convertListToSeq(List<String> inputList) {

		List<Column> list = new ArrayList<Column>();

		for (String s : inputList) {
			list.add(new Column(s));
		}
		return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
	}

}
