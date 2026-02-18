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
@Table(name = "FS_FIU_IND_FCRA_COMPLIANCE")
public class FS_FIUIndFCRAComplianceEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "ID")
	private Integer id;

	@Column(name = "NGO_NAME")
	private String ngoName;

	@Column(name = "STATE")
	private String state;

	@Column(name = "FCRA_REG_NUMBER")
	private String fcraRegNumber;

	@Column(name = "VALIDITY_PERIOD")
	private String validityPeriod;

	@Column(name = "COMPLIANCE_STATUS")
	private String complianceStatus;

	@Column(name = "FIU_RISK_INDICATOR")
	private String fiuRiskIndicator;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getNgoName() {
		return ngoName;
	}

	public void setNgoName(String ngoName) {
		this.ngoName = ngoName;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getFcraRegNumber() {
		return fcraRegNumber;
	}

	public void setFcraRegNumber(String fcraRegNumber) {
		this.fcraRegNumber = fcraRegNumber;
	}

	public String getValidityPeriod() {
		return validityPeriod;
	}

	public void setValidityPeriod(String validityPeriod) {
		this.validityPeriod = validityPeriod;
	}

	public String getComplianceStatus() {
		return complianceStatus;
	}

	public void setComplianceStatus(String complianceStatus) {
		this.complianceStatus = complianceStatus;
	}

	public String getFiuRiskIndicator() {
		return fiuRiskIndicator;
	}

	public void setFiuRiskIndicator(String fiuRiskIndicator) {
		this.fiuRiskIndicator = fiuRiskIndicator;
	}

}
