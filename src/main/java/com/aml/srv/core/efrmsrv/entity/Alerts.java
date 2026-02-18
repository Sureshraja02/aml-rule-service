package com.aml.srv.core.efrmsrv.entity;

import java.sql.Timestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_Alerts")
public class Alerts {

	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "ID")
	private Integer id;
	
	@Column(name = "ALERT_ID")
	private String alertId;

	@Column(name = "ALERT_NAME")
	private String alertName;

	@Column(name = "ALERT_DESC")
	private String alertDesc;

	@Column(name = "TRANSACTION_ID")
	private String transactionId;

	@Column(name = "ALERT_DT")
	private Timestamp alertDT;

	@Column(name = "ALERT_ASSIGNEE")
	private String alertAssignee;

	@Column(name = "RULE_ID")
	private String ruleId;

	@Column(name = "CUST_ID")
	private String custId;

	@Column(name = "ALERT_PARENT_ID")
	private String alertParentId;

	@Column(name = "ALERT_STATUS")
	private String alertStatus;

	@Column(name = "MODIFIED_DT")
	private Timestamp modifiedDt;

	@Column(name = "ACCNO")
	private String accNo;

	@Column(name = "ALERT_RANGE")
	private String alertRange;

	@Column(name = "RISK_CATEGORY")
	private String riskCategory;

	@Column(name = "FACT_VALUE")
	private String factValue;

	@Column(name = "FACT_NAME")
	private String factName;

	@Column(name = "THRESHOLD_VALUE")
	private String thresholdValue;
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getAlertId() {
		return alertId;
	}

	public void setAlertId(String alertId) {
		this.alertId = alertId;
	}

	public String getAlertName() {
		return alertName;
	}

	public void setAlertName(String alertName) {
		this.alertName = alertName;
	}

	public String getAlertDesc() {
		return alertDesc;
	}

	public void setAlertDesc(String alertDesc) {
		this.alertDesc = alertDesc;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public Timestamp getAlertDT() {
		return alertDT;
	}

	public void setAlertDT(Timestamp alertDT) {
		this.alertDT = alertDT;
	}

	public String getAlertAssignee() {
		return alertAssignee;
	}

	public void setAlertAssignee(String alertAssignee) {
		this.alertAssignee = alertAssignee;
	}

	public String getRuleId() {
		return ruleId;
	}

	public void setRuleId(String ruleId) {
		this.ruleId = ruleId;
	}

	public String getCustId() {
		return custId;
	}

	public void setCustId(String custId) {
		this.custId = custId;
	}

	public String getAlertParentId() {
		return alertParentId;
	}

	public void setAlertParentId(String alertParentId) {
		this.alertParentId = alertParentId;
	}

	public String getAlertStatus() {
		return alertStatus;
	}

	public void setAlertStatus(String alertStatus) {
		this.alertStatus = alertStatus;
	}

	public Timestamp getModifiedDt() {
		return modifiedDt;
	}

	public void setModifiedDt(Timestamp modifiedDt) {
		this.modifiedDt = modifiedDt;
	}

	public String getAccNo() {
		return accNo;
	}

	public void setAccNo(String accNo) {
		this.accNo = accNo;
	}

	public String getAlertRange() {
		return alertRange;
	}

	public void setAlertRange(String alertRange) {
		this.alertRange = alertRange;
	}

	public String getRiskCategory() {
		return riskCategory;
	}

	public void setRiskCategory(String riskCategory) {
		this.riskCategory = riskCategory;
	}

	public String getFactValue() {
		return factValue;
	}

	public void setFactValue(String factValue) {
		this.factValue = factValue;
	}

	public String getFactName() {
		return factName;
	}

	public void setFactName(String factName) {
		this.factName = factName;
	}

	public String getThresholdValue() {
		return thresholdValue;
	}

	public void setThresholdValue(String thresholdValue) {
		this.thresholdValue = thresholdValue;
	}

}
