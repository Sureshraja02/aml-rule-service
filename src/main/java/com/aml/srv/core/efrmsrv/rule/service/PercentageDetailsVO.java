package com.aml.srv.core.efrmsrv.rule.service;

import java.io.Serializable;
import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PercentageDetailsVO implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@JsonProperty("reqId")
	private String reqId;
	
	@JsonProperty("noOfTimes")
	private Long noOfTimes;
	
	@JsonProperty("totalValue")
	private BigDecimal totalValue;

	@JsonProperty("reqId")
	public String getReqId() {
		return reqId;
	}

	@JsonProperty("reqId")
	public void setReqId(String reqId) {
		this.reqId = reqId;
	}

	@JsonProperty("noOfTimes")
	public Long getNoOfTimes() {
		return noOfTimes;
	}

	@JsonProperty("noOfTimes")
	public void setNoOfTimes(Long noOfTimes) {
		this.noOfTimes = noOfTimes;
	}

	@JsonProperty("totalValue")
	public BigDecimal getTotalValue() {
		return totalValue;
	}

	@JsonProperty("totalValue")
	public void setTotalValue(BigDecimal totalValue) {
		this.totalValue = totalValue;
	}

}
