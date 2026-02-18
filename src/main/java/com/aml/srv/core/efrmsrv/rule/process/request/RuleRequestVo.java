package com.aml.srv.core.efrmsrv.rule.process.request;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class RuleRequestVo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@JsonProperty("reqId")
	private String reqId;
	
	@JsonProperty("customerId")
	private String customerId;
	
	@JsonProperty("ruleId")
	private String ruleId;
	
	@JsonProperty("accountNo")
	private String accountNo;
	
	@JsonProperty("transactionMode") //Cashor Non Cash
	private String transactionMode;
	
	@JsonProperty("txnId")
	private String txnId;
	
	@JsonProperty("txnType") //D or W 
	private String txnType;    
	
	@JsonProperty("txn_time")
	private String txn_time;
	
	@JsonProperty("Factset")
	private List<Factset> factSet;

	@JsonProperty("reqId")
	public String getReqId() {
		return reqId;
	}
	
	@JsonProperty("reqId")
	public void setReqId(String reqId) {
		this.reqId = reqId;
	}

	@JsonProperty("customerId")
	public String getCustomerId() {
		return customerId;
	}

	@JsonProperty("customerId")
	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	@JsonProperty("ruleId")
	public String getRuleId() {
		return ruleId;
	}

	@JsonProperty("ruleId")
	public void setRuleId(String ruleId) {
		this.ruleId = ruleId;
	}

	@JsonProperty("accountNo")
	public String getAccountNo() {
		return accountNo;
	}

	@JsonProperty("accountNo")
	public void setAccountNo(String accountNo) {
		this.accountNo = accountNo;
	}

	@JsonProperty("transactionMode")
	public String getTransactionMode() {
		return transactionMode;
	}

	@JsonProperty("transactionMode")
	public void setTransactionMode(String transactionMode) {
		this.transactionMode = transactionMode;
	}

	@JsonProperty("txnType")
	public String getTxnType() {
		return txnType;
	}

	@JsonProperty("txnType")
	public void setTxnType(String txnType) {
		this.txnType = txnType;
	}

	@JsonProperty("txn_time")
	public String getTxn_time() {
		return txn_time;
	}

	@JsonProperty("txn_time")
	public void setTxn_time(String txn_time) {
		this.txn_time = txn_time;
	}

	@JsonProperty("factSet")
	public List<Factset> getFactSet() {
		return factSet;
	}

	@JsonProperty("factSet")
	public void setFactSet(List<Factset> factSet) {
		this.factSet = factSet;
	}

	@JsonProperty("txnId")
	public String getTxnId() {
		return txnId;
	}

	@JsonProperty("txnId")
	public void setTxnId(String txnId) {
		this.txnId = txnId;
	}
	

}