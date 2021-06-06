package com.etl.transformation;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.etl.pojo.FilterTransformationComponent;

public class FilterTransformation {

	public Dataset<Row> filter(FilterTransformationComponent component, Map<String, Dataset<Row>> H) {

		Dataset<Row> d1 = H.get(component.getInput_entity());

		Dataset<Row> filtered = d1.filter(component.getExpr());

		return filtered;

	}

}
