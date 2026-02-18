package com.aml.srv.core.efrmsrv.rule.process.response;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class RuleResponseVo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@JsonProperty("ruleResponse")
	List<RuleResposeDetailsVO> ruleResponse;

	@JsonProperty("ruleResponse")
	public List<RuleResposeDetailsVO> getRuleResponse() {
		return ruleResponse;
	}

	@JsonProperty("ruleResponse")
	public void setRuleResponse(List<RuleResposeDetailsVO> ruleResponse) {
		this.ruleResponse = ruleResponse;
	}
	
}
