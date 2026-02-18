package com.aml.srv.core.efrmsrv.rule.process.response;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class RuleResposeDetailsVO implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@JsonProperty("reqId")
	private String reqId;
	
	@JsonProperty("ruleId")
	private String ruleId;
	
	@JsonProperty("accountType")
	private String accountType;
	
	@JsonProperty("accountStatus")
	private String accountStatus;
	
	@JsonProperty("computedFacts")
	private List<ComputedFactsVO> computedFacts;

	@JsonProperty("reqId")
	public String getReqId() {
		return reqId;
	}

	@JsonProperty("reqId")
	public void setReqId(String reqId) {
		this.reqId = reqId;
	}

	@JsonProperty("ruleId")
	public String getRuleId() {
		return ruleId;
	}

	@JsonProperty("ruleId")
	public void setRuleId(String ruleId) {
		this.ruleId = ruleId;
	}

	
	

	@JsonProperty("accountStatus")
	public String getAccountStatus() {
		return accountStatus;
	}
	@JsonProperty("accountHolderType")
	public String getAccountType() {
		return accountType;
	}

	@JsonProperty("accountHolderType")
	public void setAccountType(String accountType) {
		this.accountType = accountType;
	}

	@JsonProperty("accountStatus")
	public void setAccountStatus(String accountStatus) {
		this.accountStatus = accountStatus;
	}

	@JsonProperty("computedFacts")
	public List<ComputedFactsVO> getComputedFacts() {
		return computedFacts;
	}

	@JsonProperty("computedFacts")
	public void setComputedFacts(List<ComputedFactsVO> computedFacts) {
		this.computedFacts = computedFacts;
	}
	
}
