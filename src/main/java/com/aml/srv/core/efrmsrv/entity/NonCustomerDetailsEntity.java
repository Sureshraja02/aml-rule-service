package com.aml.srv.core.efrmsrv.entity;


import java.sql.Timestamp;

import jakarta.persistence.*;

@Entity
@Table(name = "FS_NCUST")
public class NonCustomerDetailsEntity {

    @Id
    @Column(name = "REFNO")
    private String refNo;   // VARCHAR → String (Primary Key)

    @Column(name = "CUSTOMERNAME")
    private String customerName;

    @Column(name = "IDDOCUMENT")
    private String idDocument;

    @Column(name = "IDNUMBER")
    private String idNumber;

    @Column(name = "IDENTIFICATIONTYPE")
    private String identificationType;

    @Column(name = "ISSUINGAUTHORITY")
    private String issuingAuthority;

    @Column(name = "PLACEOFISSUE")
    private String placeOfIssue;

    @Column(name = "CUSTOMERTYPE")
    private String customerType;

    @Column(name = "PAN")
    private String pan;

    @Column(name = "GENDER")
    private String gender;

    @Column(name = "OCCUPATION")
    private String occupation;

    @Column(name = "NATUREOFBUSINESS")
    private String natureOfBusiness;

    @Column(name = "PLACEOFWORK")
    private String placeOfWork;

    @Column(name = "DATEOFBIRTH")
    private String dateOfBirth;

    @Column(name = "NATIONALITY")
    private String nationality;

    @Column(name = "EMAIL")
    private String email;

    @Column(name = "ADDRESS")
    private String address;

    @Column(name = "CITY")
    private String city;

    @Column(name = "STATECODE")
    private String stateCode;

    @Column(name = "PINCODE")
    private String pincode;

    @Column(name = "COUNTRYCODE")
    private String countryCode;

    @Column(name = "TELEPHONE")
    private String telephone;

    @Column(name = "MOBILE")
    private String mobile;

    @Column(name = "FAX")
    private String fax;

    @Column(name = "FATHERNAME")
    private String fatherName;

    @Column(name = "TRANSACTIONTYPE")
    private String transactionType;

    @Column(name = "TRANSACTIONDATE")
    private Timestamp transactionDate;

    @Column(name = "AMOUNT")
    private Long amount;   // BIGINT → Long

    @Column(name = "DEPOSITORWITHDRAWAL")
    private String depositorWithdrawal;

    @Column(name = "ORIGINALAMOUNT")
    private String originalAmount;

    @Column(name = "CURRENCYCODE")
    private String currencyCode;

    @Column(name = "CONVERSIONRATE")
    private String conversionRate;

    @Column(name = "PRODUCTCODE")
    private String productCode;

    @Column(name = "INSTRUMENTCODE")
    private String instrumentCode;

    @Column(name = "INSTRUMENTNO")
    private String instrumentNo;

    @Column(name = "COUNTERPARTYNAME")
    private String counterpartyName;

    @Column(name = "COUNTERACCOUNTNO")
    private String counterAccountNo;

    @Column(name = "COUNTERBANKCODE")
    private String counterBankCode;

    @Column(name = "COUNTERBANKNAME")
    private String counterBankName;

    @Column(name = "COUNTERSTATECODE")
    private String counterStateCode;

    @Column(name = "COUNTERCOUNTRYCODE")
    private String counterCountryCode;

    @Column(name = "BRANCHCODE")
    private Long branchCode;

    @Column(name = "POSTEDUSERID")
    private String postedUserId;

    @Column(name = "FOREIGNCURRENCYFLAG")
    private String foreignCurrencyFlag;

    @Column(name = "CHANNELTYPE")
    private String channelType;

    @Column(name = "ISREMITTANCE")
    private String isRemittance;

    @Column(name = "ISCASH")
    private String isCash;

    @Column(name = "PURPOSECODE")
    private String purposeCode;

    @Column(name = "PURPOSEOFTRANSACTION")
    private String purposeOfTransaction;

    @Column(name = "RISKRATING")
    private String riskRating;

    @Column(name = "UIN")
    private String uin;

    @Column(name = "PLACEOFBIRTH")
    private Timestamp placeOfBirth;

    @Column(name = "INSTRUMENTDATE")
    private Timestamp instrumentDate;

    @Column(name = "SECURITYID")
    private String securityId;

    @Column(name = "UPDATETIMESTAMP")
    private Timestamp updateTimestamp;

	public String getRefNo() {
		return refNo;
	}

	public void setRefNo(String refNo) {
		this.refNo = refNo;
	}

	public String getCustomerName() {
		return customerName;
	}

	public void setCustomerName(String customerName) {
		this.customerName = customerName;
	}

	public String getIdDocument() {
		return idDocument;
	}

	public void setIdDocument(String idDocument) {
		this.idDocument = idDocument;
	}

	public String getIdNumber() {
		return idNumber;
	}

	public void setIdNumber(String idNumber) {
		this.idNumber = idNumber;
	}

	public String getIdentificationType() {
		return identificationType;
	}

	public void setIdentificationType(String identificationType) {
		this.identificationType = identificationType;
	}

	public String getIssuingAuthority() {
		return issuingAuthority;
	}

	public void setIssuingAuthority(String issuingAuthority) {
		this.issuingAuthority = issuingAuthority;
	}

	public String getPlaceOfIssue() {
		return placeOfIssue;
	}

	public void setPlaceOfIssue(String placeOfIssue) {
		this.placeOfIssue = placeOfIssue;
	}

	public String getCustomerType() {
		return customerType;
	}

	public void setCustomerType(String customerType) {
		this.customerType = customerType;
	}

	public String getPan() {
		return pan;
	}

	public void setPan(String pan) {
		this.pan = pan;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getOccupation() {
		return occupation;
	}

	public void setOccupation(String occupation) {
		this.occupation = occupation;
	}

	public String getNatureOfBusiness() {
		return natureOfBusiness;
	}

	public void setNatureOfBusiness(String natureOfBusiness) {
		this.natureOfBusiness = natureOfBusiness;
	}

	public String getPlaceOfWork() {
		return placeOfWork;
	}

	public void setPlaceOfWork(String placeOfWork) {
		this.placeOfWork = placeOfWork;
	}

	public String getDateOfBirth() {
		return dateOfBirth;
	}

	public void setDateOfBirth(String dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
	}

	public String getNationality() {
		return nationality;
	}

	public void setNationality(String nationality) {
		this.nationality = nationality;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getStateCode() {
		return stateCode;
	}

	public void setStateCode(String stateCode) {
		this.stateCode = stateCode;
	}

	public String getPincode() {
		return pincode;
	}

	public void setPincode(String pincode) {
		this.pincode = pincode;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public String getTelephone() {
		return telephone;
	}

	public void setTelephone(String telephone) {
		this.telephone = telephone;
	}

	public String getMobile() {
		return mobile;
	}

	public void setMobile(String mobile) {
		this.mobile = mobile;
	}

	public String getFax() {
		return fax;
	}

	public void setFax(String fax) {
		this.fax = fax;
	}

	public String getFatherName() {
		return fatherName;
	}

	public void setFatherName(String fatherName) {
		this.fatherName = fatherName;
	}

	public String getTransactionType() {
		return transactionType;
	}

	public void setTransactionType(String transactionType) {
		this.transactionType = transactionType;
	}

	public Timestamp getTransactionDate() {
		return transactionDate;
	}

	public void setTransactionDate(Timestamp transactionDate) {
		this.transactionDate = transactionDate;
	}

	public Long getAmount() {
		return amount;
	}

	public void setAmount(Long amount) {
		this.amount = amount;
	}

	public String getDepositorWithdrawal() {
		return depositorWithdrawal;
	}

	public void setDepositorWithdrawal(String depositorWithdrawal) {
		this.depositorWithdrawal = depositorWithdrawal;
	}

	public String getOriginalAmount() {
		return originalAmount;
	}

	public void setOriginalAmount(String originalAmount) {
		this.originalAmount = originalAmount;
	}

	public String getCurrencyCode() {
		return currencyCode;
	}

	public void setCurrencyCode(String currencyCode) {
		this.currencyCode = currencyCode;
	}

	public String getConversionRate() {
		return conversionRate;
	}

	public void setConversionRate(String conversionRate) {
		this.conversionRate = conversionRate;
	}

	public String getProductCode() {
		return productCode;
	}

	public void setProductCode(String productCode) {
		this.productCode = productCode;
	}

	public String getInstrumentCode() {
		return instrumentCode;
	}

	public void setInstrumentCode(String instrumentCode) {
		this.instrumentCode = instrumentCode;
	}

	public String getInstrumentNo() {
		return instrumentNo;
	}

	public void setInstrumentNo(String instrumentNo) {
		this.instrumentNo = instrumentNo;
	}

	public String getCounterpartyName() {
		return counterpartyName;
	}

	public void setCounterpartyName(String counterpartyName) {
		this.counterpartyName = counterpartyName;
	}

	public String getCounterAccountNo() {
		return counterAccountNo;
	}

	public void setCounterAccountNo(String counterAccountNo) {
		this.counterAccountNo = counterAccountNo;
	}

	public String getCounterBankCode() {
		return counterBankCode;
	}

	public void setCounterBankCode(String counterBankCode) {
		this.counterBankCode = counterBankCode;
	}

	public String getCounterBankName() {
		return counterBankName;
	}

	public void setCounterBankName(String counterBankName) {
		this.counterBankName = counterBankName;
	}

	public String getCounterStateCode() {
		return counterStateCode;
	}

	public void setCounterStateCode(String counterStateCode) {
		this.counterStateCode = counterStateCode;
	}

	public String getCounterCountryCode() {
		return counterCountryCode;
	}

	public void setCounterCountryCode(String counterCountryCode) {
		this.counterCountryCode = counterCountryCode;
	}

	public Long getBranchCode() {
		return branchCode;
	}

	public void setBranchCode(Long branchCode) {
		this.branchCode = branchCode;
	}

	public String getPostedUserId() {
		return postedUserId;
	}

	public void setPostedUserId(String postedUserId) {
		this.postedUserId = postedUserId;
	}

	public String getForeignCurrencyFlag() {
		return foreignCurrencyFlag;
	}

	public void setForeignCurrencyFlag(String foreignCurrencyFlag) {
		this.foreignCurrencyFlag = foreignCurrencyFlag;
	}

	public String getChannelType() {
		return channelType;
	}

	public void setChannelType(String channelType) {
		this.channelType = channelType;
	}

	public String getIsRemittance() {
		return isRemittance;
	}

	public void setIsRemittance(String isRemittance) {
		this.isRemittance = isRemittance;
	}

	public String getIsCash() {
		return isCash;
	}

	public void setIsCash(String isCash) {
		this.isCash = isCash;
	}

	public String getPurposeCode() {
		return purposeCode;
	}

	public void setPurposeCode(String purposeCode) {
		this.purposeCode = purposeCode;
	}

	public String getPurposeOfTransaction() {
		return purposeOfTransaction;
	}

	public void setPurposeOfTransaction(String purposeOfTransaction) {
		this.purposeOfTransaction = purposeOfTransaction;
	}

	public String getRiskRating() {
		return riskRating;
	}

	public void setRiskRating(String riskRating) {
		this.riskRating = riskRating;
	}

	public String getUin() {
		return uin;
	}

	public void setUin(String uin) {
		this.uin = uin;
	}

	public Timestamp getPlaceOfBirth() {
		return placeOfBirth;
	}

	public void setPlaceOfBirth(Timestamp placeOfBirth) {
		this.placeOfBirth = placeOfBirth;
	}

	public Timestamp getInstrumentDate() {
		return instrumentDate;
	}

	public void setInstrumentDate(Timestamp instrumentDate) {
		this.instrumentDate = instrumentDate;
	}

	public String getSecurityId() {
		return securityId;
	}

	public void setSecurityId(String securityId) {
		this.securityId = securityId;
	}

	public Timestamp getUpdateTimestamp() {
		return updateTimestamp;
	}

	public void setUpdateTimestamp(Timestamp updateTimestamp) {
		this.updateTimestamp = updateTimestamp;
	}

    
}
