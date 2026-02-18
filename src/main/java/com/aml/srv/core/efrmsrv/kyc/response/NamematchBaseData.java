package com.aml.srv.core.efrmsrv.kyc.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
public class NamematchBaseData {

	private String firstNamePercentage;		
	private String lastNamePercentage;
	
	public String getFirstNamePercentage() {
		return firstNamePercentage;
	}
	public void setFirstNamePercentage(String firstNamePercentage) {
		this.firstNamePercentage = firstNamePercentage;
	}
	public String getLastNamePercentage() {
		return lastNamePercentage;
	}
	public void setLastNamePercentage(String lastNamePercentage) {
		this.lastNamePercentage = lastNamePercentage;
	}
}
