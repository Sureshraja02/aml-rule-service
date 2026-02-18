package com.aml.srv.core.efrmsrv.ruleengine;

import java.util.List;

public class AMLRule {

	private List<Schema> schema;
	private List<Func> func;
	private String ruleName;
	
	

	public String getRuleName() {
		return ruleName;
	}

	public void setRuleName(String ruleName) {
		this.ruleName = ruleName;
	}

	public List<Schema> getSchema() {
		return schema;
	}

	public void setSchema(List<Schema> schema) {
		this.schema = schema;
	}

	public List<Func> getFunc() {
		return func;
	}

	public void setFunc(List<Func> func) {
		this.func = func;
	}

}
