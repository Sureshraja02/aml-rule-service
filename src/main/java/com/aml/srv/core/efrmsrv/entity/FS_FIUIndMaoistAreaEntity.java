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
@Table(name = "FS_FIU_IND_MAOIST_AREA")
public class FS_FIUIndMaoistAreaEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "ID")
	private Integer id;

	@Column(name = "DISTRICT")
	private String district;

	@Column(name = "STATE")
	private String state;

	@Column(name = "RISK_LEVEL")
	private String riskLevel;

	@Column(name = "REASON_HIGH_RISK")
	private String reasonHighRish;

	@Column(name = "FIU_COMPLIANCE_REQMET")
	private String fiuComplianceReqmet;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getDistrict() {
		return district;
	}

	public void setDistrict(String district) {
		this.district = district;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getRiskLevel() {
		return riskLevel;
	}

	public void setRiskLevel(String riskLevel) {
		this.riskLevel = riskLevel;
	}

	public String getReasonHighRish() {
		return reasonHighRish;
	}

	public void setReasonHighRish(String reasonHighRish) {
		this.reasonHighRish = reasonHighRish;
	}

	public String getFiuComplianceReqmet() {
		return fiuComplianceReqmet;
	}

	public void setFiuComplianceReqmet(String fiuComplianceReqmet) {
		this.fiuComplianceReqmet = fiuComplianceReqmet;
	}

}
