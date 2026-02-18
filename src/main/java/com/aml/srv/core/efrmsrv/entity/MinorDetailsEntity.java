package com.aml.srv.core.efrmsrv.entity;



import jakarta.persistence.*;

@Entity
@Table(name = "FS_MINOR")
public class MinorDetailsEntity {

    @Id
    @Column(name = "CUSTOMERID")
    private String customerId;   // VARCHAR → String (Primary Key)

    @Column(name = "DATEOFBIRTH")
    private String dateOfBirth;  // VARCHAR → String (can be LocalDate if stored as date)

    @Column(name = "GUARDIANRELATION")
    private String guardianRelation;

    @Column(name = "GUARDIANNAME")
    private String guardianName;

    @Column(name = "GUARDIANADDRESSLINE1")
    private String guardianAddressLine1;

    @Column(name = "GUARDIANADDRESSLINE2")
    private String guardianAddressLine2;

    @Column(name = "GUARDIANCITY")
    private String guardianCity;

    @Column(name = "GUARDIANSTATE")
    private String guardianState;

    @Column(name = "GUARDIANCOUNTRY")
    private String guardianCountry;

    @Column(name = "GUARDIANPINCODE")
    private Long guardianPincode;   // BIGINT → Long

    @Column(name = "GUARDIANPHONENO")
    private String guardianPhoneNo;

    @Column(name = "UPDATETIMESTAMP")
    private String updateTimestamp; // VARCHAR → String (can be LocalDateTime if stored as date/time)

    @Column(name = "SECURITYID")
    private String securityId;

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public String getDateOfBirth() {
		return dateOfBirth;
	}

	public void setDateOfBirth(String dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
	}

	public String getGuardianRelation() {
		return guardianRelation;
	}

	public void setGuardianRelation(String guardianRelation) {
		this.guardianRelation = guardianRelation;
	}

	public String getGuardianName() {
		return guardianName;
	}

	public void setGuardianName(String guardianName) {
		this.guardianName = guardianName;
	}

	public String getGuardianAddressLine1() {
		return guardianAddressLine1;
	}

	public void setGuardianAddressLine1(String guardianAddressLine1) {
		this.guardianAddressLine1 = guardianAddressLine1;
	}

	public String getGuardianAddressLine2() {
		return guardianAddressLine2;
	}

	public void setGuardianAddressLine2(String guardianAddressLine2) {
		this.guardianAddressLine2 = guardianAddressLine2;
	}

	public String getGuardianCity() {
		return guardianCity;
	}

	public void setGuardianCity(String guardianCity) {
		this.guardianCity = guardianCity;
	}

	public String getGuardianState() {
		return guardianState;
	}

	public void setGuardianState(String guardianState) {
		this.guardianState = guardianState;
	}

	public String getGuardianCountry() {
		return guardianCountry;
	}

	public void setGuardianCountry(String guardianCountry) {
		this.guardianCountry = guardianCountry;
	}

	public Long getGuardianPincode() {
		return guardianPincode;
	}

	public void setGuardianPincode(Long guardianPincode) {
		this.guardianPincode = guardianPincode;
	}

	public String getGuardianPhoneNo() {
		return guardianPhoneNo;
	}

	public void setGuardianPhoneNo(String guardianPhoneNo) {
		this.guardianPhoneNo = guardianPhoneNo;
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

  
}
