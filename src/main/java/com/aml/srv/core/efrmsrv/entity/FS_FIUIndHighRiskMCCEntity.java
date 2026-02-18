package com.aml.srv.core.efrmsrv.entity;

import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * 
 * @author : E5554365 (Prabakaran.R)
 * @Project : aml-srv
 * @year : 2025
 */
@Entity
@Table(name = "FS_FIU_IND_HIGHRISK_MCC")
public class FS_FIUIndHighRiskMCCEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "ID")
	private Integer id;

	@Column(name = "MCC_CODE")
	private String mccCode;

	@Column(name = "CATEGORY")
	private String category;

	@Column(name = "DESCRIPTION")
	private String description;

	@Column(name = "RISK_TIRE")
	private String riskTire;

	@Column(name = "AML_KYC_IMPACT")
	private String amlKycImpact;

	@Column(name = "FIU_REPORT_OBLIGATION")
	private String fiuReportObligation;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getMccCode() {
		return mccCode;
	}

	public void setMccCode(String mccCode) {
		this.mccCode = mccCode;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getRiskTire() {
		return riskTire;
	}

	public void setRiskTire(String riskTire) {
		this.riskTire = riskTire;
	}

	public String getAmlKycImpact() {
		return amlKycImpact;
	}

	public void setAmlKycImpact(String amlKycImpact) {
		this.amlKycImpact = amlKycImpact;
	}

	public String getFiuReportObligation() {
		return fiuReportObligation;
	}

	public void setFiuReportObligation(String fiuReportObligation) {
		this.fiuReportObligation = fiuReportObligation;
	}

}
