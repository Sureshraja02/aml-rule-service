package com.aml.srv.core.efrmsrv.entity;

import java.io.Serializable;
import java.sql.Timestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_RULE_AUDIT_RES")
public class RuleAuditRespEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
    @Column(name = "RA_RES_ID", length = 255, nullable = false)
    private String raResId;

    @Column(name = "REQ_ID", length = 255)
    private String reqId;

    @Column(name = "FACT_NAME", length = 255)
    private String factName;

    @Column(name = "FACT_FIELD", length = 255)
    private String factField;

    @Column(name = "RULE_VALUE", length = 255)
    private String ruleValue;

    @Column(name = "ACCOUNT_NO", length = 255)
    private String accountNo;

    @Column(name = "ACCOUNT_HOLDER_TYPE", length = 255)
    private String accountHolderType;

    @Column(name = "ACCOUNT_STATUS", length = 255)
    private String accountStatus;

    @Column(name = "ACCOUNT_OPEN_DATE")
    private Timestamp accountOpenDate;

    @Column(name = "ACCOUNT_REACT_DATE")
    private Timestamp accountReactDate;

    @Column(name = "RULE_ID", length = 255)
    private String ruleId;

    @Column(name = "LINE_OF_BUSINESS", length = 255)
    private String lineOfBusiness;

    @Column(name = "CREATED_DATE")
    private Timestamp createdDate;

	public String getRaResId() {
		return raResId;
	}

	public void setRaResId(String raResId) {
		this.raResId = raResId;
	}

	public String getReqId() {
		return reqId;
	}

	public void setReqId(String reqId) {
		this.reqId = reqId;
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

	public String getRuleValue() {
		return ruleValue;
	}

	public void setRuleValue(String ruleValue) {
		this.ruleValue = ruleValue;
	}

	public String getAccountNo() {
		return accountNo;
	}

	public void setAccountNo(String accountNo) {
		this.accountNo = accountNo;
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

	public Timestamp getAccountOpenDate() {
		return accountOpenDate;
	}

	public void setAccountOpenDate(Timestamp accountOpenDate) {
		this.accountOpenDate = accountOpenDate;
	}

	public Timestamp getAccountReactDate() {
		return accountReactDate;
	}

	public void setAccountReactDate(Timestamp accountReactDate) {
		this.accountReactDate = accountReactDate;
	}

	public String getRuleId() {
		return ruleId;
	}

	public void setRuleId(String ruleId) {
		this.ruleId = ruleId;
	}

	public String getLineOfBusiness() {
		return lineOfBusiness;
	}

	public void setLineOfBusiness(String lineOfBusiness) {
		this.lineOfBusiness = lineOfBusiness;
	}

	public Timestamp getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Timestamp createdDate) {
		this.createdDate = createdDate;
	}
        
}
