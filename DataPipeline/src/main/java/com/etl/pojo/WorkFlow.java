package com.etl.pojo;

import java.util.List;

public class WorkFlow {

	private String workflow_id;

	private String workflow_name;

	private List<InputComponent> input;

	private List<OutputComponent> output;

	private List<JoinTransformationComponent> join_transformation;

	private List<SelectTransformationComponent> select_transformation;

	private List<FilterTransformationComponent> filter_transformation;

	public void setWorkflow_id(String workflow_id) {
		this.workflow_id = workflow_id;
	}

	public String getWorkflow_id() {
		return this.workflow_id;
	}

	public void setWorkflow_name(String workflow_name) {
		this.workflow_name = workflow_name;
	}

	public String getWorkflow_name() {
		return this.workflow_name;
	}

	public void setInput(List<InputComponent> input) {
		this.input = input;
	}

	public List<InputComponent> getInput() {
		return this.input;
	}

	public void setOutput(List<OutputComponent> output) {
		this.output = output;
	}

	public List<OutputComponent> getOutput() {
		return this.output;
	}

	public List<JoinTransformationComponent> getJoin_transformation() {
		return join_transformation;
	}

	public void setJoin_transformation(List<JoinTransformationComponent> join_transformation) {
		this.join_transformation = join_transformation;
	}

	public List<SelectTransformationComponent> getSelect_transformation() {
		return select_transformation;
	}

	public void setSelect_transformation(List<SelectTransformationComponent> select_transformation) {
		this.select_transformation = select_transformation;
	}

	public List<FilterTransformationComponent> getFilter_transformation() {
		return filter_transformation;
	}

	public void setFilter_transformation(List<FilterTransformationComponent> filter_transformation) {
		this.filter_transformation = filter_transformation;
	}

}
