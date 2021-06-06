package com.etl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.etl.pojo.Component;
import com.etl.pojo.WorkFlow;
import com.etl.util.WorkflowLoader;

public class TransformationRunnerGeneric {

	public static void main(String[] args) {

		SparkSession session = SparkSession.builder().appName("SparkDemo").master("local").getOrCreate();

		Map<String, Dataset<Row>> datasetMap = new HashMap<String, Dataset<Row>>();

		// Load work Flow
		String workFlowFile = "/Users/nitesh/Work/Git/SparkProjects/ETLDemo/conf/workflow1.json";
		WorkFlow workFlow = WorkflowLoader.loadWorkFlow(workFlowFile);

		List<Component> list = WorkflowLoader.getComponents(workFlow);
		for (Component c : list) {
			c.execute(session, datasetMap);
		}

		session.close();
	}
}