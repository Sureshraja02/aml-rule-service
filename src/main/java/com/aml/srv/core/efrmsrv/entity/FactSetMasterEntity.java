package com.aml.srv.core.efrmsrv.entity;

import java.io.Serializable;
import java.sql.Timestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_FACTSET_MASTER")
public class FactSetMasterEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name = "FACT_ID", nullable = true)
	private String factId;

	@Column(name = "FACT_NAME", nullable = true)
	private String factName;

	@Column(name = "FACT_DATATYPE", nullable = true)
	private String factDataType;

	@Column(name = "FACT_TYPE", nullable = true)
	private String factType;

	@Column(name = "FACT_DESC", nullable = true)
	private String factDesc;

	@Column(name = "CREATED_DATE", nullable = true)
	private Timestamp createdDate;

	@Column(name = "UPDATE_DATE", nullable = true)
	private Timestamp updatedDate;

	public String getFactId() {
		return factId;
	}

	public void setFactId(String factId) {
		this.factId = factId;
	}

	public String getFactName() {
		return factName;
	}

	public void setFactName(String factName) {
		this.factName = factName;
	}

	public String getFactDesc() {
		return factDesc;
	}

	public void setFactDesc(String factDesc) {
		this.factDesc = factDesc;
	}

	public Timestamp getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Timestamp createdDate) {
		this.createdDate = createdDate;
	}

	public Timestamp getUpdatedDate() {
		return updatedDate;
	}

	public void setUpdatedDate(Timestamp updatedDate) {
		this.updatedDate = updatedDate;
	}

	public String getFactDataType() {
		return factDataType;
	}

	public void setFactDataType(String factDataType) {
		this.factDataType = factDataType;
	}

	public String getFactType() {
		return factType;
	}

	public void setFactType(String factType) {
		this.factType = factType;
	}

}