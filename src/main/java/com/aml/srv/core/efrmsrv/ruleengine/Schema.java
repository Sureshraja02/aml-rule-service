package com.aml.srv.core.efrmsrv.ruleengine;

public class Schema {

	private String tag;
	private String value;
	private String condition;
	private String type;
	private String joinexpression;

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getJoinexpression() {
		return joinexpression;
	}

	public void setJoinexpression(String joinexpression) {
		this.joinexpression = joinexpression;
	}

}
