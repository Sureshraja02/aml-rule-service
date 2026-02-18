package com.aml.srv.core.efrmsrv.entity;


import jakarta.persistence.*;

@Entity
@Table(name = "FS_BRANCH")
public class BranchDetailsEntity {

    @Id
    @Column(name = "BRANCHCODE")
    private String branchCode;   // VARCHAR → String (Primary Key)

    @Column(name = "BRANCHNAME")
    private String branchName;

    @Column(name = "BRANCHAREA")
    private Long branchArea;

    @Column(name = "BANKCODE")
    private String bankCode;

    @Column(name = "BRANCHMANAGERNAME")
    private String branchManagerName;

    @Column(name = "CLHOUSECODE")
    private Long clHouseCode;

    @Column(name = "UPDATETIMESTAMP")
    private String updateTimestamp;

    @Column(name = "SECURITYID")
    private String securityId;

    @Column(name = "BRANCHEMAIL")
    private String branchEmail;

    @Column(name = "BRANCHFAX")
    private String branchFax;

    @Column(name = "BRANCHTELEPHONE")
    private String branchTelephone;

    @Column(name = "BRANCHPINCODE")
    private Long branchPincode;

    @Column(name = "BRANCHADDR5")
    private String branchAddr5;

    @Column(name = "BRANCHADDR4")
    private String branchAddr4;

    @Column(name = "BRANCHADDR2")
    private String branchAddr2;

    @Column(name = "BRANCHADDR1")
    private String branchAddr1;

    @Column(name = "BRANCHADDR3")
    private Long branchAddr3;

    @Column(name = "BSRCODE")
    private String bsrCode;

    @Column(name = "CATEGORYCODE")
    private Long categoryCode;

    @Column(name = "CLEARINGAREA")
    private String clearingArea;

    @Column(name = "REGIONCODE")
    private Long regionCode;

    @Column(name = "REMARKS")
    private String remarks;

    @Column(name = "WEBSITE")
    private String website;

    @Column(name = "COUNTRY")
    private String country;

    @Column(name = "ISSENSITIVE")
    private String isSensitive;

    @Column(name = "CATEGORY")
    private String category;

    @Column(name = "CITY")
    private Long city;

    @Column(name = "STATE")
    private String state;

    @Column(name = "UIDISSUEDBYFIU")
    private String uidIssuedByFiu;

    @Column(name = "ZONECODE")
    private Long zoneCode;

    @Column(name = "HIGHMLRISK")
    private String highMlRisk;

	public String getBranchCode() {
		return branchCode;
	}

	public void setBranchCode(String branchCode) {
		this.branchCode = branchCode;
	}

	public String getBranchName() {
		return branchName;
	}

	public void setBranchName(String branchName) {
		this.branchName = branchName;
	}

	public Long getBranchArea() {
		return branchArea;
	}

	public void setBranchArea(Long branchArea) {
		this.branchArea = branchArea;
	}

	public String getBankCode() {
		return bankCode;
	}

	public void setBankCode(String bankCode) {
		this.bankCode = bankCode;
	}

	public String getBranchManagerName() {
		return branchManagerName;
	}

	public void setBranchManagerName(String branchManagerName) {
		this.branchManagerName = branchManagerName;
	}

	public Long getClHouseCode() {
		return clHouseCode;
	}

	public void setClHouseCode(Long clHouseCode) {
		this.clHouseCode = clHouseCode;
	}

	public String getUpdateTimestamp() {
		return updateTimestamp;
	}

	public void setUpdateTimestamp(String updateTimestamp) {
		this.updateTimestamp = updateTimestamp;
	}

	public String getSecurityId() {
		return securityId;
	}

	public void setSecurityId(String securityId) {
		this.securityId = securityId;
	}

	public String getBranchEmail() {
		return branchEmail;
	}

	public void setBranchEmail(String branchEmail) {
		this.branchEmail = branchEmail;
	}

	public String getBranchFax() {
		return branchFax;
	}

	public void setBranchFax(String branchFax) {
		this.branchFax = branchFax;
	}

	public String getBranchTelephone() {
		return branchTelephone;
	}

	public void setBranchTelephone(String branchTelephone) {
		this.branchTelephone = branchTelephone;
	}

	public Long getBranchPincode() {
		return branchPincode;
	}

	public void setBranchPincode(Long branchPincode) {
		this.branchPincode = branchPincode;
	}

	public String getBranchAddr5() {
		return branchAddr5;
	}

	public void setBranchAddr5(String branchAddr5) {
		this.branchAddr5 = branchAddr5;
	}

	public String getBranchAddr4() {
		return branchAddr4;
	}

	public void setBranchAddr4(String branchAddr4) {
		this.branchAddr4 = branchAddr4;
	}

	public String getBranchAddr2() {
		return branchAddr2;
	}

	public void setBranchAddr2(String branchAddr2) {
		this.branchAddr2 = branchAddr2;
	}

	public String getBranchAddr1() {
		return branchAddr1;
	}

	public void setBranchAddr1(String branchAddr1) {
		this.branchAddr1 = branchAddr1;
	}

	public Long getBranchAddr3() {
		return branchAddr3;
	}

	public void setBranchAddr3(Long branchAddr3) {
		this.branchAddr3 = branchAddr3;
	}

	public String getBsrCode() {
		return bsrCode;
	}

	public void setBsrCode(String bsrCode) {
		this.bsrCode = bsrCode;
	}

	public Long getCategoryCode() {
		return categoryCode;
	}

	public void setCategoryCode(Long categoryCode) {
		this.categoryCode = categoryCode;
	}

	public String getClearingArea() {
		return clearingArea;
	}

	public void setClearingArea(String clearingArea) {
		this.clearingArea = clearingArea;
	}

	public Long getRegionCode() {
		return regionCode;
	}

	public void setRegionCode(Long regionCode) {
		this.regionCode = regionCode;
	}

	public String getRemarks() {
		return remarks;
	}

	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	public String getWebsite() {
		return website;
	}

	public void setWebsite(String website) {
		this.website = website;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getIsSensitive() {
		return isSensitive;
	}

	public void setIsSensitive(String isSensitive) {
		this.isSensitive = isSensitive;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public Long getCity() {
		return city;
	}

	public void setCity(Long city) {
		this.city = city;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getUidIssuedByFiu() {
		return uidIssuedByFiu;
	}

	public void setUidIssuedByFiu(String uidIssuedByFiu) {
		this.uidIssuedByFiu = uidIssuedByFiu;
	}

	public Long getZoneCode() {
		return zoneCode;
	}

	public void setZoneCode(Long zoneCode) {
		this.zoneCode = zoneCode;
	}

	public String getHighMlRisk() {
		return highMlRisk;
	}

	public void setHighMlRisk(String highMlRisk) {
		this.highMlRisk = highMlRisk;
	}

  
}

