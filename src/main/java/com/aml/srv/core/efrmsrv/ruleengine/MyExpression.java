package com.aml.srv.core.efrmsrv.ruleengine;

public enum MyExpression {

	equals("=="), AND("&&"), greaterthanequals(">="), greatherthan(">"), lesserthanequals("<="), lesserthan("<");

	private String action;

	MyExpression(String action) {
		this.action = action;
	}

	public String getAction() {
		return action;
	}

}
