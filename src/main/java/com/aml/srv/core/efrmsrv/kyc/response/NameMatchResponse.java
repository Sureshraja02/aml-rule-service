package com.aml.srv.core.efrmsrv.kyc.response;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
public class NameMatchResponse {

	public List<NamematchBaseData> getNamematchBaseData() {
		return namematchBaseData;
	}
	public void setNamematchBaseData(List<NamematchBaseData> namematchBaseData) {
		this.namematchBaseData = namematchBaseData;
	}
	private String reqid;
	
	private String custId;
	
	private String firstName;
	
	private String lastName;
	
	private String dob;
	
	private String pan;
	
	private String mobile;	
	
	private String email;
	
	
	
	private double firstnamePercentage;
	
	private boolean soundMatch;
	
	private List<NamematchBaseData> namematchBaseData;

	public String getReqid() {
		return reqid;
	}
	public void setReqid(String reqid) {
		this.reqid = reqid;
	}
	public double getFirstnamePercentage() {
		return firstnamePercentage;
	}
	public void setFirstnamePercentage(double firstnamePercentage) {
		this.firstnamePercentage = firstnamePercentage;
	}
	public boolean isSoundMatch() {
		return soundMatch;
	}
	public void setSoundMatch(boolean soundMatch) {
		this.soundMatch = soundMatch;
	}
	public String getCustId() {
		return custId;
	}
	public void setCustId(String custId) {
		this.custId = custId;
	}
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public String getDob() {
		return dob;
	}
	public void setDob(String dob) {
		this.dob = dob;
	}
	public String getPan() {
		return pan;
	}
	public void setPan(String pan) {
		this.pan = pan;
	}
	public String getMobile() {
		return mobile;
	}
	public void setMobile(String mobile) {
		this.mobile = mobile;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	
	
	
}
