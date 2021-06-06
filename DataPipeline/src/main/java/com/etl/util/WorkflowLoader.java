package com.etl.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.etl.pojo.Component;
import com.etl.pojo.WorkFlow;
import com.fasterxml.jackson.databind.ObjectMapper;

public class WorkflowLoader {

	public static void main(String[] args) {
		new WorkflowLoader();
	}

	public WorkflowLoader() {
		loadWorkFlow("/Users/nitesh/Work/Git/SparkProjects/ETLDemo/conf/workflow1.json");
	}

	public static WorkFlow loadWorkFlow(String workflowFile) {

		WorkFlow workFlow = null;
		try {
			ObjectMapper mapper = new ObjectMapper();

			workFlow = mapper.readValue(new File(workflowFile), WorkFlow.class);
			System.out.println(workFlow.getWorkflow_id() + " " + workFlow.getWorkflow_name());

		} catch (IOException e) {
			e.printStackTrace();
		}
		return workFlow;
	}

	public static List<Component> getComponents(WorkFlow workFlow) {
		List<Component> components = new ArrayList<Component>();

		for (Component c : workFlow.getInput()) {
			components.add(c);
		}
		for (Component c : workFlow.getJoin_transformation()) {
			components.add(c);
		}

		for (Component c : workFlow.getOutput()) {
			components.add(c);
		}
		for (Component c : workFlow.getSelect_transformation()) {
			components.add(c);
		}
		for (Component c : workFlow.getFilter_transformation()) {
			components.add(c);
		}
		Collections.sort(components);

		for (Component c : components) {
			System.out.println("c id : " + c.getSequence_id());
		}
		return components;

	}

}