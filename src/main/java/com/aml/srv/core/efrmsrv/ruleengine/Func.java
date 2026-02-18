package com.aml.srv.core.efrmsrv.ruleengine;

public class Func {

	private String fact;
	private String value;
	private Integer lookback;
	private String condition;
	private String units;
	private String joinexpression;
	private String field;
	private String expression;
	private String tag;
	private String operator;
	private Range factrange;
	private String range;

	public Range getFactrange() {
		return factrange;
	}

	public void setFactrange(Range factrange) {
		this.factrange = factrange;
	}

	public String getRange() {
		return range;
	}

	public void setRange(String range) {
		this.range = range;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getFact() {
		return fact;
	}

	public void setFact(String fact) {
		this.fact = fact;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Integer getLookback() {
		return lookback;
	}

	public void setLookback(Integer lookback) {
		this.lookback = lookback;
	}

	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

	public String getUnits() {
		return units;
	}

	public void setUnits(String units) {
		this.units = units;
	}

	public String getJoinexpression() {
		return joinexpression;
	}

	public void setJoinexpression(String joinexpression) {
		this.joinexpression = joinexpression;
	}

}
