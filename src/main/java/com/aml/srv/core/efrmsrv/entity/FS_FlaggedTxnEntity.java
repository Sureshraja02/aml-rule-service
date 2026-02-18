package com.aml.srv.core.efrmsrv.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_FLAGGED_TXN")
public class FS_FlaggedTxnEntity {

	@Id
	@Column(name = "TRANSACTIONBATCHID")
	private String transactionBatchId;

	@Column(name = "TRANSACTIONID")
	private String transactionId;

	@Column(name = "CUSTOMERID")
	private Long customerId;

	@Column(name = "ACCOUNTNO")
	private Long accountNo;

	@Column(name = "BRANCHCODE")
	private String branchCode;

	@Column(name = "TRANSACTIONTYPE")
	private String transactionType;

	@Column(name = "CHANNELTYPE")
	private String channelType;

	@Column(name = "CHANNELID")
	private String channelId;

	@Column(name = "TRANSACTIONDATE")
	private String transactionDate;

	@Column(name = "TRANSACTIONTIME")
	private String transactionTime;

	@Column(name = "AMOUNT")
	private BigDecimal amount;

	@Column(name = "DEPOSITORWITHDRAWAL")
	private String depositorWithdrawal;

	@Column(name = "FLOWCODE")
	private String flowCode;

	@Column(name = "INSTRUMENTCODE")
	private String instrumentCode;

	@Column(name = "INSTRUMENTNO")
	private String instrumentNo;

	@Column(name = "INSTRUMENTDATE")
	private String instrumentDate;

	@Column(name = "MICRCODE")
	private String micrCode;

	@Column(name = "ACCTCURRENCYCODE")
	private String acctCurrencyCode;

	@Column(name = "CURRENCYCODE")
	private String currencyCode;

	@Column(name = "ORIGINALCURRENCYTRNAMT")
	private Long originalCurrencyTrnAmt;

	@Column(name = "RATECODE")
	private String rateCode;

	@Column(name = "CONVERSIONRATE")
	private Long conversionRate;

	@Column(name = "NARRATION")
	private String narration;

	@Column(name = "REMARKS")
	private String remarks;

	@Column(name = "REFNO")
	private Long refNo;

	@Column(name = "CREATIONUSERID")
	private String creationUserId;

	@Column(name = "POSTEDUSERID")
	private Long postedUserId;

	@Column(name = "FOREXREMITTANCE")
	private String forexRemittance;

	@Column(name = "COUNTERPARTYID")
	private String counterpartyId;

	@Column(name = "COUNTERPARTYNAME")
	private String counterpartyName;

	@Column(name = "COUNTERPARTYACCOUNTNO")
	private Long counterpartyAccountNo;

	@Column(name = "COUNTERPARTYTYPE")
	private String counterpartyType;

	@Column(name = "COUNTERBANKCODE")
	private String counterBankCode;

	@Column(name = "COUNTERBRANCHCODE")
	private String counterBranchCode;

	@Column(name = "COUNTERPARTYADDRESS")
	private String counterpartyAddress;

	@Column(name = "COUNTERCOUNTRYCODE")
	private String counterCountryCode;

	@Column(name = "ACCOUNTCURRENCYTRNAMT")
	private Long accountCurrencyTrnAmt;

	@Column(name = "BALANCEAMOUNT")
	private Long balanceAmount;

	@Column(name = "POSTINGDATE")
	private Timestamp postingDate;

	@Column(name = "MERCHANTCATEGORYCODE")
	private String merchantCategoryCode;

	@Column(name = "CATEGORY_NAME")
	private String categoryName;

	@Column(name = "RULE_ID")
	private String ruleId;

	@Column(name = "RULE_NAME")
	private String ruleName;

	@Column(name = "CREATED_DATE")
	private Timestamp createdDate;

	public String getTransactionBatchId() {
		return transactionBatchId;
	}

	public void setTransactionBatchId(String transactionBatchId) {
		this.transactionBatchId = transactionBatchId;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public Long getCustomerId() {
		return customerId;
	}

	public void setCustomerId(Long customerId) {
		this.customerId = customerId;
	}

	public Long getAccountNo() {
		return accountNo;
	}

	public void setAccountNo(Long accountNo) {
		this.accountNo = accountNo;
	}

	public String getBranchCode() {
		return branchCode;
	}

	public void setBranchCode(String branchCode) {
		this.branchCode = branchCode;
	}

	public String getTransactionType() {
		return transactionType;
	}

	public void setTransactionType(String transactionType) {
		this.transactionType = transactionType;
	}

	public String getChannelType() {
		return channelType;
	}

	public void setChannelType(String channelType) {
		this.channelType = channelType;
	}

	public String getChannelId() {
		return channelId;
	}

	public void setChannelId(String channelId) {
		this.channelId = channelId;
	}

	public String getTransactionDate() {
		return transactionDate;
	}

	public void setTransactionDate(String transactionDate) {
		this.transactionDate = transactionDate;
	}

	public String getTransactionTime() {
		return transactionTime;
	}

	public void setTransactionTime(String transactionTime) {
		this.transactionTime = transactionTime;
	}

	public BigDecimal getAmount() {
		return amount;
	}

	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}

	public String getDepositorWithdrawal() {
		return depositorWithdrawal;
	}

	public void setDepositorWithdrawal(String depositorWithdrawal) {
		this.depositorWithdrawal = depositorWithdrawal;
	}

	public String getFlowCode() {
		return flowCode;
	}

	public void setFlowCode(String flowCode) {
		this.flowCode = flowCode;
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

	public String getMicrCode() {
		return micrCode;
	}

	public void setMicrCode(String micrCode) {
		this.micrCode = micrCode;
	}

	public String getAcctCurrencyCode() {
		return acctCurrencyCode;
	}

	public void setAcctCurrencyCode(String acctCurrencyCode) {
		this.acctCurrencyCode = acctCurrencyCode;
	}

	public String getCurrencyCode() {
		return currencyCode;
	}

	public void setCurrencyCode(String currencyCode) {
		this.currencyCode = currencyCode;
	}

	public Long getOriginalCurrencyTrnAmt() {
		return originalCurrencyTrnAmt;
	}

	public void setOriginalCurrencyTrnAmt(Long originalCurrencyTrnAmt) {
		this.originalCurrencyTrnAmt = originalCurrencyTrnAmt;
	}

	public String getRateCode() {
		return rateCode;
	}

	public void setRateCode(String rateCode) {
		this.rateCode = rateCode;
	}

	public Long getConversionRate() {
		return conversionRate;
	}

	public void setConversionRate(Long conversionRate) {
		this.conversionRate = conversionRate;
	}

	public String getNarration() {
		return narration;
	}

	public void setNarration(String narration) {
		this.narration = narration;
	}

	public String getRemarks() {
		return remarks;
	}

	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	public Long getRefNo() {
		return refNo;
	}

	public void setRefNo(Long refNo) {
		this.refNo = refNo;
	}

	public String getCreationUserId() {
		return creationUserId;
	}

	public void setCreationUserId(String creationUserId) {
		this.creationUserId = creationUserId;
	}

	public Long getPostedUserId() {
		return postedUserId;
	}

	public void setPostedUserId(Long postedUserId) {
		this.postedUserId = postedUserId;
	}

	public String getForexRemittance() {
		return forexRemittance;
	}

	public void setForexRemittance(String forexRemittance) {
		this.forexRemittance = forexRemittance;
	}

	public String getCounterpartyId() {
		return counterpartyId;
	}

	public void setCounterpartyId(String counterpartyId) {
		this.counterpartyId = counterpartyId;
	}

	public String getCounterpartyName() {
		return counterpartyName;
	}

	public void setCounterpartyName(String counterpartyName) {
		this.counterpartyName = counterpartyName;
	}

	public Long getCounterpartyAccountNo() {
		return counterpartyAccountNo;
	}

	public void setCounterpartyAccountNo(Long counterpartyAccountNo) {
		this.counterpartyAccountNo = counterpartyAccountNo;
	}

	public String getCounterpartyType() {
		return counterpartyType;
	}

	public void setCounterpartyType(String counterpartyType) {
		this.counterpartyType = counterpartyType;
	}

	public String getCounterBankCode() {
		return counterBankCode;
	}

	public void setCounterBankCode(String counterBankCode) {
		this.counterBankCode = counterBankCode;
	}

	public String getCounterBranchCode() {
		return counterBranchCode;
	}

	public void setCounterBranchCode(String counterBranchCode) {
		this.counterBranchCode = counterBranchCode;
	}

	public String getCounterpartyAddress() {
		return counterpartyAddress;
	}

	public void setCounterpartyAddress(String counterpartyAddress) {
		this.counterpartyAddress = counterpartyAddress;
	}

	public String getCounterCountryCode() {
		return counterCountryCode;
	}

	public void setCounterCountryCode(String counterCountryCode) {
		this.counterCountryCode = counterCountryCode;
	}

	public Long getAccountCurrencyTrnAmt() {
		return accountCurrencyTrnAmt;
	}

	public void setAccountCurrencyTrnAmt(Long accountCurrencyTrnAmt) {
		this.accountCurrencyTrnAmt = accountCurrencyTrnAmt;
	}

	public Long getBalanceAmount() {
		return balanceAmount;
	}

	public void setBalanceAmount(Long balanceAmount) {
		this.balanceAmount = balanceAmount;
	}

	public Timestamp getPostingDate() {
		return postingDate;
	}

	public void setPostingDate(Timestamp postingDate) {
		this.postingDate = postingDate;
	}

	public String getMerchantCategoryCode() {
		return merchantCategoryCode;
	}

	public void setMerchantCategoryCode(String merchantCategoryCode) {
		this.merchantCategoryCode = merchantCategoryCode;
	}

	public String getCategoryName() {
		return categoryName;
	}

	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}

	public String getRuleId() {
		return ruleId;
	}

	public void setRuleId(String ruleId) {
		this.ruleId = ruleId;
	}

	public String getRuleName() {
		return ruleName;
	}

	public void setRuleName(String ruleName) {
		this.ruleName = ruleName;
	}

	public Timestamp getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Timestamp createdDate) {
		this.createdDate = createdDate;
	}

}
