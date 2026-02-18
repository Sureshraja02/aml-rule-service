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
@Table(name = "FS_UNSCR_LIST")
public class FS_UNSCRListEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "ID")
	private Integer id;

	@Column(name = "UNSCR_ID")
	private String unscrId;

	@Column(name = "NAME")
	private String name;

	@Column(name = "TITLE")
	private String title;

	@Column(name = "DESIGNATION")
	private String designation;

	@Column(name = "DOB")
	private String dob;

	@Column(name = "POB")
	private String pob;

	@Column(name = "NATIONALITY")
	private String nationality;

	@Column(name = "PASSPORT_NO")
	private String passportNo;

	@Column(name = "NATIONAL_ID")
	private String nationalId;

	@Column(name = "ADDRESS")
	private String address;

	@Column(name = "LISTED_ON")
	private String listedOn;

	@Column(name = "AMENDMENTS")
	private String amendments;

	@Column(name = "OTHER_INFORMATION")
	private String otherInformation;

	@Column(name = "INTERPOL_Link")
	private String interpolLink;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getUnscrId() {
		return unscrId;
	}

	public void setUnscrId(String unscrId) {
		this.unscrId = unscrId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getDesignation() {
		return designation;
	}

	public void setDesignation(String designation) {
		this.designation = designation;
	}

	public String getDob() {
		return dob;
	}

	public void setDob(String dob) {
		this.dob = dob;
	}

	public String getPob() {
		return pob;
	}

	public void setPob(String pob) {
		this.pob = pob;
	}

	public String getNationality() {
		return nationality;
	}

	public void setNationality(String nationality) {
		this.nationality = nationality;
	}

	public String getPassportNo() {
		return passportNo;
	}

	public void setPassportNo(String passportNo) {
		this.passportNo = passportNo;
	}

	public String getNationalId() {
		return nationalId;
	}

	public void setNationalId(String nationalId) {
		this.nationalId = nationalId;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getListedOn() {
		return listedOn;
	}

	public void setListedOn(String listedOn) {
		this.listedOn = listedOn;
	}

	public String getAmendments() {
		return amendments;
	}

	public void setAmendments(String amendments) {
		this.amendments = amendments;
	}

	public String getOtherInformation() {
		return otherInformation;
	}

	public void setOtherInformation(String otherInformation) {
		this.otherInformation = otherInformation;
	}

	public String getInterpolLink() {
		return interpolLink;
	}

	public void setInterpolLink(String interpolLink) {
		this.interpolLink = interpolLink;
	}

}
