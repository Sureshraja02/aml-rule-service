package com.aml.srv.core.efrmsrv.entity;


import java.sql.Timestamp;

import jakarta.persistence.*;

@Entity
@Table(name = "FS_NOM")
public class NomineeDetailsEntity {

    @Id
    @Column(name = "ACCOUNTNO")
    private String accountNo;   // VARCHAR → String (Primary Key)

    @Column(name = "NOMINEENAME")
    private String nomineeName;

    @Column(name = "NOM_DATEOFBIRTH")
    private String nomineeDateOfBirth;   // VARCHAR → String (can be LocalDate if stored as date)

    @Column(name = "NOM_RELATION")
    private Long nomineeRelation;   // BIGINT → Long

    @Column(name = "NOM_ADDRESSLINE1")
    private String nomineeAddressLine1;

    @Column(name = "NOM_ADDRESSLINE2")
    private String nomineeAddressLine2;

    @Column(name = "NOM_CITY")
    private String nomineeCity;

    @Column(name = "NOM_STATE")
    private String nomineeState;

    @Column(name = "NOM_COUNTRY")
    private String nomineeCountry;

    @Column(name = "NOM_PINCODE")
    private String nomineePincode;

    @Column(name = "NOM_PHONENO")
    private String nomineePhoneNo;

    @Column(name = "UPDATETIMESTAMP")
    private Timestamp updateTimestamp;   // VARCHAR → String (can be LocalDateTime if stored as date/time)

    @Column(name = "SECURITYID")
    private String securityId;

    @Column(name = "NOM_ADDRESSLINE3")
    private String nomineeAddressLine3;

	public String getAccountNo() {
		return accountNo;
	}

	public void setAccountNo(String accountNo) {
		this.accountNo = accountNo;
	}

	public String getNomineeName() {
		return nomineeName;
	}

	public void setNomineeName(String nomineeName) {
		this.nomineeName = nomineeName;
	}

	public String getNomineeDateOfBirth() {
		return nomineeDateOfBirth;
	}

	public void setNomineeDateOfBirth(String nomineeDateOfBirth) {
		this.nomineeDateOfBirth = nomineeDateOfBirth;
	}

	public Long getNomineeRelation() {
		return nomineeRelation;
	}

	public void setNomineeRelation(Long nomineeRelation) {
		this.nomineeRelation = nomineeRelation;
	}

	public String getNomineeAddressLine1() {
		return nomineeAddressLine1;
	}

	public void setNomineeAddressLine1(String nomineeAddressLine1) {
		this.nomineeAddressLine1 = nomineeAddressLine1;
	}

	public String getNomineeAddressLine2() {
		return nomineeAddressLine2;
	}

	public void setNomineeAddressLine2(String nomineeAddressLine2) {
		this.nomineeAddressLine2 = nomineeAddressLine2;
	}

	public String getNomineeCity() {
		return nomineeCity;
	}

	public void setNomineeCity(String nomineeCity) {
		this.nomineeCity = nomineeCity;
	}

	public String getNomineeState() {
		return nomineeState;
	}

	public void setNomineeState(String nomineeState) {
		this.nomineeState = nomineeState;
	}

	public String getNomineeCountry() {
		return nomineeCountry;
	}

	public void setNomineeCountry(String nomineeCountry) {
		this.nomineeCountry = nomineeCountry;
	}

	public String getNomineePincode() {
		return nomineePincode;
	}

	public void setNomineePincode(String nomineePincode) {
		this.nomineePincode = nomineePincode;
	}

	public String getNomineePhoneNo() {
		return nomineePhoneNo;
	}

	public void setNomineePhoneNo(String nomineePhoneNo) {
		this.nomineePhoneNo = nomineePhoneNo;
	}

	public Timestamp getUpdateTimestamp() {
		return updateTimestamp;
	}

	public void setUpdateTimestamp(Timestamp updateTimestamp) {
		this.updateTimestamp = updateTimestamp;
	}

	public String getSecurityId() {
		return securityId;
	}

	public void setSecurityId(String securityId) {
		this.securityId = securityId;
	}

	public String getNomineeAddressLine3() {
		return nomineeAddressLine3;
	}

	public void setNomineeAddressLine3(String nomineeAddressLine3) {
		this.nomineeAddressLine3 = nomineeAddressLine3;
	}

   
}
