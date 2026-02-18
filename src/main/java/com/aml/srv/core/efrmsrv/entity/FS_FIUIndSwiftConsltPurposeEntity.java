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
@Table(name = "FS_FIU_IND_SWIFT_CONSLT_PURPOSE")
public class FS_FIUIndSwiftConsltPurposeEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "ID")
	private Integer id;

	@Column(name = "PURPOSE_CODE")
	private String purposeCode;

	@Column(name = "DESCRIPTION")
	private String description;

	@Column(name = "RISK_LEVEL")
	private String riskLevel;

	@Column(name = "FIU_REPORTING_REQMET")
	private String fiuReportingReqmet;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getPurposeCode() {
		return purposeCode;
	}

	public void setPurposeCode(String purposeCode) {
		this.purposeCode = purposeCode;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getRiskLevel() {
		return riskLevel;
	}

	public void setRiskLevel(String riskLevel) {
		this.riskLevel = riskLevel;
	}

	public String getFiuReportingReqmet() {
		return fiuReportingReqmet;
	}

	public void setFiuReportingReqmet(String fiuReportingReqmet) {
		this.fiuReportingReqmet = fiuReportingReqmet;
	}

}
