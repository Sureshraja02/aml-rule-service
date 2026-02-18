package com.aml.srv.core.efrmsrv.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "NGP_NORMALIZED_RULE_TB")
public class NormalizedTblEntity {

	@Column(name = "ID")
	@Id
	private String id;

	@Column(name = "RULE_NAME")
	private String ruleName;

	@Column(name = "PRIORITY")
	private Integer priority;

	@Column(name = "RULE_DESCRIPTION")
	private String ruleDescription;

	@Column(name = "PAYLOAD")
	private String payload;

	@Column(name = "OFFSET_UNIT")
	private String offsetUnit;

	@Column(name = "OFFSET_VALUE")
	private int offsetValue;

	@Column(name = "ALERT_CATEGORY")
	private String alertCategory;

	@Column(name = "TXN_MODE")
	private String transactionMode;
	
	@Column(name = "STATUS")
	private String status;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getRuleName() {
		return ruleName;
	}

	public void setRuleName(String ruleName) {
		this.ruleName = ruleName;
	}

	public Integer getPriority() {
		return priority;
	}

	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	public String getRuleDescription() {
		return ruleDescription;
	}

	public void setRuleDescription(String ruleDescription) {
		this.ruleDescription = ruleDescription;
	}

	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
	}

	public String getOffsetUnit() {
		return offsetUnit;
	}

	public void setOffsetUnit(String offsetUnit) {
		this.offsetUnit = offsetUnit;
	}

	public int getOffsetValue() {
		return offsetValue;
	}

	public void setOffsetValue(int offsetValue) {
		this.offsetValue = offsetValue;
	}

	public String getAlertCategory() {
		return alertCategory;
	}

	public void setAlertCategory(String alertCategory) {
		this.alertCategory = alertCategory;
	}

	public String getTransactionMode() {
		return transactionMode;
	}

	public void setTransactionMode(String transactionMode) {
		this.transactionMode = transactionMode;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}