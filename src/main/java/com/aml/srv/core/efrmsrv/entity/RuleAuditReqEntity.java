package com.aml.srv.core.efrmsrv.entity;

import java.io.Serializable;
import java.sql.Timestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_RULE_AUDIT_REQ")
public class RuleAuditReqEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name = "RA_ID", length = 255, nullable = false)
	private String raId;

	@Column(name = "req_id", length = 255)
	private String reqId;

	@Column(name = "customer_id", length = 255)
	private String customerId;

	@Column(name = "account_no", length = 255)
	private String accountNo;

	@Column(name = "RULE_ID", length = 255)
	private String ruleId;

	@Column(name = "transaction_mode", length = 255)
	private String transactionMode;

	@Column(name = "transaction_time")
	private String transactionTime;

	@Column(name = "TXN_TYPE", length = 255)
	private String txnType;

	@Column(name = "FACT_NAME", length = 255)
	private String factName;

	@Column(name = "FACT_FIELD", length = 255)
	private String factField;

	@Column(name = "CATE_ID", length = 255)
	private String cateId;

	@Column(name = "account_holder_type", length = 255)
	private String accountHolderType;

	@Column(name = "account_status", length = 255)
	private String accountStatus;

	@Column(name = "RULE_VALUE", length = 255)
	private String ruleValue;

	@Column(name = "req_date_time")
	private Timestamp reqDateTime;

	@Column(name = "NumberOfDays", length = 255)
	private String numberOfDays;

	@Column(name = "NumberOfhours", length = 255)
	private String numberOfHours;

	@Column(name = "NumberOfMonths", length = 255)
	private String numberOfMonths;

	public String getRaId() {
		return raId;
	}

	public void setRaId(String raId) {
		this.raId = raId;
	}

	public String getReqId() {
		return reqId;
	}

	public void setReqId(String reqId) {
		this.reqId = reqId;
	}

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public String getAccountNo() {
		return accountNo;
	}

	public void setAccountNo(String accountNo) {
		this.accountNo = accountNo;
	}

	public String getRuleId() {
		return ruleId;
	}

	public void setRuleId(String ruleId) {
		this.ruleId = ruleId;
	}

	public String getTransactionMode() {
		return transactionMode;
	}

	public void setTransactionMode(String transactionMode) {
		this.transactionMode = transactionMode;
	}

	public String getTransactionTime() {
		return transactionTime;
	}

	public void setTransactionTime(String transactionTime) {
		this.transactionTime = transactionTime;
	}

	public String getTxnType() {
		return txnType;
	}

	public void setTxnType(String txnType) {
		this.txnType = txnType;
	}

	public String getFactName() {
		return factName;
	}

	public void setFactName(String factName) {
		this.factName = factName;
	}

	public String getFactField() {
		return factField;
	}

	public void setFactField(String factField) {
		this.factField = factField;
	}

	public String getCateId() {
		return cateId;
	}

	public void setCateId(String cateId) {
		this.cateId = cateId;
	}

	public String getAccountHolderType() {
		return accountHolderType;
	}

	public void setAccountHolderType(String accountHolderType) {
		this.accountHolderType = accountHolderType;
	}

	public String getAccountStatus() {
		return accountStatus;
	}

	public void setAccountStatus(String accountStatus) {
		this.accountStatus = accountStatus;
	}

	public String getRuleValue() {
		return ruleValue;
	}

	public void setRuleValue(String ruleValue) {
		this.ruleValue = ruleValue;
	}

	public Timestamp getReqDateTime() {
		return reqDateTime;
	}

	public void setReqDateTime(Timestamp reqDateTime) {
		this.reqDateTime = reqDateTime;
	}

	public String getNumberOfDays() {
		return numberOfDays;
	}

	public void setNumberOfDays(String numberOfDays) {
		this.numberOfDays = numberOfDays;
	}

	public String getNumberOfHours() {
		return numberOfHours;
	}

	public void setNumberOfHours(String numberOfHours) {
		this.numberOfHours = numberOfHours;
	}

	public String getNumberOfMonths() {
		return numberOfMonths;
	}

	public void setNumberOfMonths(String numberOfMonths) {
		this.numberOfMonths = numberOfMonths;
	}
	
}