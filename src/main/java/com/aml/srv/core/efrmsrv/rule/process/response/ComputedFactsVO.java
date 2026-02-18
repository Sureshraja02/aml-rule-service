package com.aml.srv.core.efrmsrv.rule.process.response;

import java.io.Serializable;
import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 
 * @author : E5554365 (Prabakaran.R)
 * @Project : aml-srv
 * @year : 2025
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ComputedFactsVO implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@JsonProperty("fact")
	private String fact;

	@JsonProperty("value")
	private BigDecimal value;
	
	@JsonProperty("ruleType")
	private String ruleType;
	
	@JsonProperty("transDate")
	private String transDate;
	
	@JsonProperty("strValue")
	private String strValue;
	
	@JsonProperty("strType")
	private String strType;
	
	@JsonProperty("acc_open_date")
	private String acc_open_date;
	
	@JsonProperty("acc_Re_date")
	private String acc_Re_date;
	
	@JsonProperty("fieldTag")
	private String fieldTag;
	
	
	@JsonProperty("accountStatus")
	private String accountStatus;
	
	@JsonProperty("depositeLocation")
	private String depositeLocation;
	
	@JsonProperty("perCentValue")
	private String perCentValue;
	
	@JsonProperty("transactionId")
	private String transactionId;

	@JsonProperty("transactionId")
	public String getTransactionId() {
		return transactionId;
	}

	@JsonProperty("transactionId")
	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	@JsonProperty("depositeLocation")
	public String getDepositeLocation() {
		return depositeLocation;
	}

	@JsonProperty("depositeLocation")
	public void setDepositeLocation(String depositeLocation) {
		this.depositeLocation = depositeLocation;
	}

	@JsonProperty("fact")
	public String getFact() {
		return fact;
	}

	@JsonProperty("fact")
	public void setFact(String fact) {
		this.fact = fact;
	}

	@JsonProperty("value")
	public BigDecimal getValue() {
		return value;
	}

	@JsonProperty("value")
	public void setValue(BigDecimal value) {
		this.value = value;
	}

	@JsonProperty("ruleType")
	public String getRuleType() {
		return ruleType;
	}

	@JsonProperty("ruleType")
	public void setRuleType(String ruleType) {
		this.ruleType = ruleType;
	}

	@JsonProperty("transDate")
	public String getTransDate() {
		return transDate;
	}

	@JsonProperty("transDate")
	public void setTransDate(String transDate) {
		this.transDate = transDate;
	}
	@JsonProperty("strValue")
	public String getStrValue() {
		return strValue;
	}
	@JsonProperty("strValue")
	public void setStrValue(String strValue) {
		this.strValue = strValue;
	}

	@JsonProperty("acc_open_date")
	public String getAcc_open_date() {
		return acc_open_date;
	}

	@JsonProperty("acc_open_date")
	public void setAcc_open_date(String acc_open_date) {
		this.acc_open_date = acc_open_date;
	}

	@JsonProperty("accountStatus")
	public String getAccountStatus() {
		return accountStatus;
	}

	@JsonProperty("accountStatus")
	public void setAccountStatus(String accountStatus) {
		this.accountStatus = accountStatus;
	}

	@JsonProperty("perCentValue")
	public String getPerCentValue() {
		return perCentValue;
	}

	@JsonProperty("perCentValue")
	public void setPerCentValue(String perCentValue) {
		this.perCentValue = perCentValue;
	}

	@JsonProperty("acc_Re_date")
	public String getAcc_Re_date() {
		return acc_Re_date;
	}

	@JsonProperty("acc_Re_date")
	public void setAcc_Re_date(String acc_Re_date) {
		this.acc_Re_date = acc_Re_date;
	}

	@JsonProperty("strType")
	public String getStrType() {
		return strType;
	}

	@JsonProperty("strType")
	public void setStrType(String strType) {
		this.strType = strType;
	}

	@JsonProperty("fieldTag")
	public String getFieldTag() {
		return fieldTag;
	}

	@JsonProperty("fieldTag")
	public void setFieldTag(String fieldTag) {
		this.fieldTag = fieldTag;
	}
	
	

}
