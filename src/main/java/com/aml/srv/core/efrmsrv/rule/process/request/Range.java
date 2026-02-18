package com.aml.srv.core.efrmsrv.rule.process.request;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Range implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@JsonProperty("min")
	private String min;
	
	@JsonProperty("max")
	private String max;
	

	@JsonProperty("min")
	public String getMin() {
	return min;
	}

	@JsonProperty("min")
	public void setMin(String min) {
	this.min = min;
	}

	@JsonProperty("max")
	public String getMax() {
	return max;
	}

	@JsonProperty("max")
	public void setMax(String max) {
	this.max = max;
	}


}
