package com.etl.transformation;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.etl.pojo.JoinTransformationComponent;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class JoinTransformation {

	public Dataset<Row> join(JoinTransformationComponent component, Map<String, Dataset<Row>> H) {

		Dataset<Row> d1 = H.get(component.getEntities().get(0));
		Dataset<Row> d2 = H.get(component.getEntities().get(1));

		String col1 = component.getJoin_col().get(0);
		String col2 = component.getJoin_col().get(1);
		Dataset<Row> joined = d1.join(d2, d1.col(col1).equalTo(d2.col(col2))).drop(d1.col(col1));

		return joined;

	}

	public Seq<String> convertListToSeq(List<String> inputList) {

		return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}
}
