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
@Table(name = "FS_FIU_IND_TERROR_LOCATIONS")
public class FS_FIUIndTerrorLocationEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "ID")
	private Integer id;

	@Column(name = "LOCATION")
	private String location;

	@Column(name = "RISK_LEVEL")
	private String riskLevel;

	@Column(name = "REASON_HIGH_RISK")
	private String reasonHisgRisk;

	@Column(name = "FATF_STATUS")
	private String fatfStatus;

	@Column(name = "FIU_COMPLIANCE_REQMET")
	private String fiuComplianceReqmet;

	@Column(name = "country_code")
	private String country_Code;
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getRiskLevel() {
		return riskLevel;
	}

	public void setRiskLevel(String riskLevel) {
		this.riskLevel = riskLevel;
	}

	public String getReasonHisgRisk() {
		return reasonHisgRisk;
	}

	public void setReasonHisgRisk(String reasonHisgRisk) {
		this.reasonHisgRisk = reasonHisgRisk;
	}

	public String getFatfStatus() {
		return fatfStatus;
	}

	public void setFatfStatus(String fatfStatus) {
		this.fatfStatus = fatfStatus;
	}

	public String getFiuComplianceReqmet() {
		return fiuComplianceReqmet;
	}

	public void setFiuComplianceReqmet(String fiuComplianceReqmet) {
		this.fiuComplianceReqmet = fiuComplianceReqmet;
	}

	public String getCountry_Code() {
		return country_Code;
	}

	public void setCountry_Code(String country_Code) {
		this.country_Code = country_Code;
	}

	
}
