package com.aml.srv.core.efrmsrv.entity;

import java.io.Serializable;
import java.sql.Timestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_RULE_CATEGORIES")//, schema = "amlschema"
public class RuleCategoriesEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	@Id
    @Column(name = "CATE_ID", length = 255, nullable = false)
    private String cateId;

    @Column(name = "CATE_NAME", length = 255)
    private String cateName;

    @Column(name = "CATE_TYPE", length = 255)
    private String cateType;

    @Column(name = "CREATED_DATE")
    private Timestamp createdDate;

    @Column(name = "UPDATE_DATE")
    private Timestamp updateDate;

	public String getCateId() {
		return cateId;
	}

	public void setCateId(String cateId) {
		this.cateId = cateId;
	}

	public String getCateName() {
		return cateName;
	}

	public void setCateName(String cateName) {
		this.cateName = cateName;
	}

	public String getCateType() {
		return cateType;
	}

	public void setCateType(String cateType) {
		this.cateType = cateType;
	}

	public Timestamp getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Timestamp createdDate) {
		this.createdDate = createdDate;
	}

	public Timestamp getUpdateDate() {
		return updateDate;
	}

	public void setUpdateDate(Timestamp updateDate) {
		this.updateDate = updateDate;
	}
	
}
