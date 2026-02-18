package com.aml.srv.core.efrmsrv.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_CBWT_TRN_REPORT")
public class CbwtDetailsEntity {

	@Id
	@Column(name = "REFNO", length = 50)
	private String refNo;

	@Column(name = "CUSTOMERID", length = 50)
	private String customerId;

	@Column(name = "ACCOUNTNO", length = 50)
	private String accountNo;

	@Column(name = "CUSTOMERNAME", length = 250)
	private String customerName;

	@Column(name = "IDDOCUMENT", length = 150)
	private String idDocument;

	@Column(name = "IDNUMBER", length = 150)
	private String idNumber;

	@Column(name = "IDENTIFICATIONTYPE", length = 50)
	private String identificationType;

	@Column(name = "ISSUINGAUTHORITY", length = 250)
	private String issuingAuthority;

	@Column(name = "PLACEOFISSUE", length = 250)
	private String placeOfIssue;

	@Column(name = "CUSTOMERTYPE", length = 150)
	private String customerType;

	@Column(name = "PAN", length = 50)
	private String pan;

	@Column(name = "UIN", length = 50)
	private String uin;

	@Column(name = "GENDER", length = 50)
	private String gender;

	@Column(name = "OCCUPATION", length = 250)
	private String occupation;

	@Column(name = "NATUREOFBUSINESS", length = 250)
	private String natureOfBusiness;

	@Column(name = "PLACEOFWORK", length = 250)
	private String placeOfWork;

	@Column(name = "DATEOFBIRTH", length = 20)
	private String dateOfBirth;

	@Column(name = "PLACEOFBIRTH", length = 250)
	private String placeOfBirth;

	@Column(name = "NATIONALITY", length = 150)
	private String nationality;

	@Column(name = "EMAIL", length = 150)
	private String email;

	@Column(name = "ADDRESS", length = 500)
	private String address;

	@Column(name = "CITY", length = 150)
	private String city;

	@Column(name = "STATECODE", length = 50)
	private String stateCode;

	@Column(name = "PINCODE")
	private Long pinCode;

	@Column(name = "COUNTRYCODE", length = 50)
	private String countryCode;

	@Column(name = "TELEPHONE")
	private Double telephone;

	@Column(name = "MOBILE", length = 50)
	private String mobile;

	@Column(name = "FAX")
	private Long fax;

	@Column(name = "FATHERORSPOUSENAME", length = 50)
	private String fatherOrSpouseName;

	@Column(name = "TRANSACTIONTYPE", length = 50)
	private String transactionType;

	@Column(name = "TRANSACTIONDATE", length = 50)
	private String transactionDate;

	@Column(name = "AMOUNT")
	private Long amount;

	@Column(name = "DEPOSITORWITHDRAWAL", length = 50)
	private String depositorWithdrawal;

	@Column(name = "ORIGINALAMOUNT")
	private Long originalAmount;

	@Column(name = "CURRENCYCODE", length = 50)
	private String currencyCode;

	@Column(name = "CONVERSIONRATE")
	private Double conversionRate;

	@Column(name = "PRODUCTCODE", length = 50)
	private String productCode;

	@Column(name = "INSTRUMENTCODE", length = 50)
	private String instrumentCode;

	@Column(name = "INSTRUMENTNO", length = 50)
	private String instrumentNo;

	@Column(name = "INSTRUMENTDATE", length = 50)
	private String instrumentDate;

	@Column(name = "COUNTERPARTYNAME", length = 50)
	private String counterPartyName;

	@Column(name = "COUNTERACCOUNTNO", length = 50)
	private String counterAccountNo;

	@Column(name = "COUNTERPARTYID", length = 50)
	private String counterPartyId;

	@Column(name = "COUNTERBANKCODE", length = 50)
	private String counterBankCode;

	@Column(name = "COUNTERBANKNAME", length = 50)
	private String counterBankName;

	@Column(name = "COUNTERSTATECODE", length = 50)
	private String counterStateCode;

	@Column(name = "COUNTERCOUNTRYCODE", length = 50)
	private String counterCountryCode;

	@Column(name = "BRANCHCODE")
	private Long branchCode;

	@Column(name = "POSTEDUSERID", length = 50)
	private String postedUserId;

	@Column(name = "FOREIGNCURRENCYFLAG", length = 50)
	private String foreignCurrencyFlag;

	@Column(name = "CHANNELTYPE", length = 50)
	private String channelType;

	@Column(name = "ISREMITTANCE", length = 50)
	private String isRemittance;

	@Column(name = "ISCASH", length = 50)
	private String isCash;

	@Column(name = "PURPOSECODE", length = 50)
	private String purposeCode;

	@Column(name = "PURPOSEOFTRANSACTION", length = 250)
	private String purposeOfTransaction;

	@Column(name = "RISKRATING")
	private Long riskRating;

	@Column(name = "TRNIDEN_FLAG", length = 50)
	private String trnIdenFlag;

	@Column(name = "COUNTERADDRESS", length = 250)
	private String counterAddress;

	@Column(name = "SENDERCORRESPONDENTBANK", length = 50)
	private String senderCorrespondentBank;

	@Column(name = "RECVERCORRESPONDENTBANK", length = 50)
	private String recverCorrespondentBank;

	@Column(name = "SENDR_CORSPNDNT_BNKCODE", length = 50)
	private String senderCorrespondentBankCode;

	@Column(name = "RECVER_CORSPNDNT_BNKCODE", length = 50)
	private String recverCorrespondentBankCode;

	@Column(name = "PAYMNT_INSTR_ISUE_INST_NAME", length = 250)
	private String paymentInstrIssueInstName;

	@Column(name = "INSTRMNT_ISSUE_INST_REFNO")
	private Long instrumentIssueInstRefNo;

	@Column(name = "SWIFT_REFNO", length = 150)
	private String swiftRefNo;

	@Column(name = "REMITR_OR_BENF_BIC_CODE", length = 50)
	private String remitrOrBenefBicCode;

	public String getRefNo() {
		return refNo;
	}

	public void setRefNo(String refNo) {
		this.refNo = refNo;
	}

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public String getAccountNo() {
		return accountNo;
	}

	public void setAccountNo(String accountNo) {
		this.accountNo = accountNo;
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

	public String getUin() {
		return uin;
	}

	public void setUin(String uin) {
		this.uin = uin;
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

	public String getPlaceOfBirth() {
		return placeOfBirth;
	}

	public void setPlaceOfBirth(String placeOfBirth) {
		this.placeOfBirth = placeOfBirth;
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

	public Long getPinCode() {
		return pinCode;
	}

	public void setPinCode(Long pinCode) {
		this.pinCode = pinCode;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public Double getTelephone() {
		return telephone;
	}

	public void setTelephone(Double telephone) {
		this.telephone = telephone;
	}

	public String getMobile() {
		return mobile;
	}

	public void setMobile(String mobile) {
		this.mobile = mobile;
	}

	public Long getFax() {
		return fax;
	}

	public void setFax(Long fax) {
		this.fax = fax;
	}

	public String getFatherOrSpouseName() {
		return fatherOrSpouseName;
	}

	public void setFatherOrSpouseName(String fatherOrSpouseName) {
		this.fatherOrSpouseName = fatherOrSpouseName;
	}

	public String getTransactionType() {
		return transactionType;
	}

	public void setTransactionType(String transactionType) {
		this.transactionType = transactionType;
	}

	public String getTransactionDate() {
		return transactionDate;
	}

	public void setTransactionDate(String transactionDate) {
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

	public Long getOriginalAmount() {
		return originalAmount;
	}

	public void setOriginalAmount(Long originalAmount) {
		this.originalAmount = originalAmount;
	}

	public String getCurrencyCode() {
		return currencyCode;
	}

	public void setCurrencyCode(String currencyCode) {
		this.currencyCode = currencyCode;
	}

	public Double getConversionRate() {
		return conversionRate;
	}

	public void setConversionRate(Double conversionRate) {
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

	public String getInstrumentDate() {
		return instrumentDate;
	}

	public void setInstrumentDate(String instrumentDate) {
		this.instrumentDate = instrumentDate;
	}

	public String getCounterPartyName() {
		return counterPartyName;
	}

	public void setCounterPartyName(String counterPartyName) {
		this.counterPartyName = counterPartyName;
	}

	public String getCounterAccountNo() {
		return counterAccountNo;
	}

	public void setCounterAccountNo(String counterAccountNo) {
		this.counterAccountNo = counterAccountNo;
	}

	public String getCounterPartyId() {
		return counterPartyId;
	}

	public void setCounterPartyId(String counterPartyId) {
		this.counterPartyId = counterPartyId;
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

	public Long getRiskRating() {
		return riskRating;
	}

	public void setRiskRating(Long riskRating) {
		this.riskRating = riskRating;
	}

	public String getTrnIdenFlag() {
		return trnIdenFlag;
	}

	public void setTrnIdenFlag(String trnIdenFlag) {
		this.trnIdenFlag = trnIdenFlag;
	}

	public String getCounterAddress() {
		return counterAddress;
	}

	public void setCounterAddress(String counterAddress) {
		this.counterAddress = counterAddress;
	}

	public String getSenderCorrespondentBank() {
		return senderCorrespondentBank;
	}

	public void setSenderCorrespondentBank(String senderCorrespondentBank) {
		this.senderCorrespondentBank = senderCorrespondentBank;
	}

	public String getRecverCorrespondentBank() {
		return recverCorrespondentBank;
	}

	public void setRecverCorrespondentBank(String recverCorrespondentBank) {
		this.recverCorrespondentBank = recverCorrespondentBank;
	}

	public String getSenderCorrespondentBankCode() {
		return senderCorrespondentBankCode;
	}

	public void setSenderCorrespondentBankCode(String senderCorrespondentBankCode) {
		this.senderCorrespondentBankCode = senderCorrespondentBankCode;
	}

	public String getRecverCorrespondentBankCode() {
		return recverCorrespondentBankCode;
	}

	public void setRecverCorrespondentBankCode(String recverCorrespondentBankCode) {
		this.recverCorrespondentBankCode = recverCorrespondentBankCode;
	}

	public String getPaymentInstrIssueInstName() {
		return paymentInstrIssueInstName;
	}

	public void setPaymentInstrIssueInstName(String paymentInstrIssueInstName) {
		this.paymentInstrIssueInstName = paymentInstrIssueInstName;
	}

	public Long getInstrumentIssueInstRefNo() {
		return instrumentIssueInstRefNo;
	}

	public void setInstrumentIssueInstRefNo(Long instrumentIssueInstRefNo) {
		this.instrumentIssueInstRefNo = instrumentIssueInstRefNo;
	}

	public String getSwiftRefNo() {
		return swiftRefNo;
	}

	public void setSwiftRefNo(String swiftRefNo) {
		this.swiftRefNo = swiftRefNo;
	}

	public String getRemitrOrBenefBicCode() {
		return remitrOrBenefBicCode;
	}

	public void setRemitrOrBenefBicCode(String remitrOrBenefBicCode) {
		this.remitrOrBenefBicCode = remitrOrBenefBicCode;
	}

}
