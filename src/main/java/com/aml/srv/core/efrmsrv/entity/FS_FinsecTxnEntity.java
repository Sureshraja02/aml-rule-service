package com.aml.srv.core.efrmsrv.entity;

import java.sql.Timestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;


@Entity
@Table(name = "FS_FINSEC_TXN")
public class FS_FinsecTxnEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "ID")
	private Integer id;

	@Column(name = "TRANSACTIONID")
	private String transactionId;

	@Column(name = "STR", columnDefinition = "integer default 0")
	private Integer str = 0;

	@Column(name = "CTR", columnDefinition = "integer default 0")
	private Integer ctr = 0;

	@Column(name = "NTR", columnDefinition = "integer default 0")
	private Integer ntr = 0;

	@Column(name = "CBWTR", columnDefinition = "integer default 0")
	private Integer cbwtr = 0;

	@Column(name = "CFTR", columnDefinition = "integer default 0")
	private Integer cftr = 0;

	@Column(name = "OTHER", columnDefinition = "integer default 0")
	private Integer other = 0;

	@Column(name = "TRANS_SCORE", columnDefinition = "integer default 0")
	private Integer transScore = 0;

	@Column(name = "CREATED_DATE")
	private Timestamp createdDate;

	@Column(name = "MODIFY_DATE")
	private Timestamp modifyDate;

	@Version 
	@Column(name = "VERSION")
	private Integer version;
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public Integer getStr() {
		return str;
	}

	public void setStr(Integer str) {
		this.str = str;
	}

	public Integer getCtr() {
		return ctr;
	}

	public void setCtr(Integer ctr) {
		this.ctr = ctr;
	}

	public Integer getNtr() {
		return ntr;
	}

	public void setNtr(Integer ntr) {
		this.ntr = ntr;
	}

	public Integer getCbwtr() {
		return cbwtr;
	}

	public void setCbwtr(Integer cbwtr) {
		this.cbwtr = cbwtr;
	}

	public Integer getCftr() {
		return cftr;
	}

	public void setCftr(Integer cftr) {
		this.cftr = cftr;
	}

	public Integer getOther() {
		return other;
	}

	public void setOther(Integer other) {
		this.other = other;
	}

	public Timestamp getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Timestamp createdDate) {
		this.createdDate = createdDate;
	}

	public Timestamp getModifyDate() {
		return modifyDate;
	}

	public void setModifyDate(Timestamp modifyDate) {
		this.modifyDate = modifyDate;
	}

	public Integer getTransScore() {
		return transScore;
	}

	public void setTransScore(Integer transScore) {
		this.transScore = transScore;
	}

}