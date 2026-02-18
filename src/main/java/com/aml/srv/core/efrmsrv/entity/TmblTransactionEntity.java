package com.aml.srv.core.efrmsrv.entity;

import java.sql.Timestamp;
import java.time.LocalDate;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_TBML_TRN")
public class TmblTransactionEntity {

    @Id
    @Column(name = "TRANSACTIONNO")
    private Double transactionNo;   // DOUBLE → Double (PK here, adjust if needed)

    @Column(name = "TRANSACTIONBATCHID")
    private Long transactionBatchId;

    @Column(name = "TRANSACTIONID")
    private Double transactionId;

    @Column(name = "TRANSACTIONTYPE")
    private Long transactionType;

    @Column(name = "FLOWCODE")
    private String flowCode;

    @Column(name = "CUSTOMERID")
    private String customerId;

    @Column(name = "INSTRUMENTCODE")
    private String instrumentCode;

    @Column(name = "INSTRUMENTNO")
    private String instrumentNo;

    @Column(name = "INSTRUMENTDATE")
    private LocalDate instrumentDate;

    @Column(name = "ACCOUNTNO")
    private String accountNo;

    @Column(name = "BRANCHCODE")
    private Long branchCode;

    @Column(name = "AMOUNT")
    private Long amount;

    @Column(name = "DEPOSITORWITHDRAWAL")
    private String depositorWithdrawal;

    @Column(name = "COUNTERPARTYID")
    private String counterpartyId;

    @Column(name = "COUNTERPARTYNAME")
    private String counterpartyName;

    @Column(name = "COUNTERACCOUNTNO")
    private String counterAccountNo;

    @Column(name = "COUNTERPARTYTYPE")
    private String counterpartyType;

    @Column(name = "COUNTERBRANCHCODE")
    private String counterBranchCode;

    @Column(name = "COUNTERBANKCODE")
    private String counterBankCode;

    @Column(name = "CURRENCYCODE")
    private String currencyCode;

    @Column(name = "RATECODE")
    private String rateCode;

    @Column(name = "CONVERSIONRATE")
    private Long conversionRate;

    @Column(name = "NARRATION")
    private String narration;

    @Column(name = "CHANNELID")
    private String channelId;

    @Column(name = "MICRCODE")
    private String micrCode;

    @Column(name = "COUNTERCOUNTRYCODE")
    private String counterCountryCode;

    @Column(name = "CHANNELTYPE")
    private String channelType;

    @Column(name = "FOREXREMITTANCE")
    private String forexRemittance;

    @Column(name = "COUNTERPARTYADDRESS")
    private String counterpartyAddress;

    @Column(name = "BALANCEAMOUNT")
    private String balanceAmount;

    @Column(name = "POSTINGDATE")
    private String postingDate;

    @Column(name = "TRANSACTIONDATETIME")
    private LocalDate transactionDateTime;

    @Column(name = "REMARKS")
    private String remarks;

    @Column(name = "REFNO")
    private String refNo;

    @Column(name = "POSTEDUSERID")
    private String postedUserId;

    @Column(name = "AMOUNTINACCOUNTCURRENCY")
    private String amountInAccountCurrency;

    @Column(name = "CREATIONUSERID")
    private String creationUserId;

    @Column(name = "ORIGINALAMOUNT")
    private String originalAmount;

    @Column(name = "COUNTERPARTYNATIONALITY")
    private String counterpartyNationality;

    @Column(name = "UPDATETIMESTAMP")
    private Timestamp updateTimestamp;

    @Column(name = "COMMODITYCODE")
    private String commodityCode;

    @Column(name = "INTANGIBLE_FLAG")
    private String intangibleFlag;

    @Column(name = "INVOICEVALUE")
    private String invoiceValue;

    @Column(name = "BOEDATE")
    private String boeDate;

    @Column(name = "BOEAMOUNT")
    private String boeAmount;

    @Column(name = "TRADEREF_NUMBER")
    private String tradeRefNumber;

    @Column(name = "TRADETYPE")
    private String tradeType;

    @Column(name = "ADVANCE_REMITFLAG")
    private String advanceRemitFlag;

    @Column(name = "EXPORTIMPORTDATE")
    private String exportImportDate;

    @Column(name = "SHIPPINGROUTE")
    private String shippingRoute;

    @Column(name = "FIRSTTIME_IMPORT")
    private String firstTimeImport;

    @Column(name = "ACCTCURRENCYCODE")
    private String acctCurrencyCode;

    @Column(name = "AMOUNTINORIGINALCURRENCY")
    private String amountInOriginalCurrency;

    @Column(name = "PURPOSECODE")
    private String purposeCode;

    @Column(name = "FIRSTTRADETRNFLAG")
    private String firstTradeTrnFlag;

    @Column(name = "TRADEIDENTIFIER")
    private String tradeIdentifier;

    @Column(name = "TRANSACTIONTIME")
    private String transactionTime;

    @Column(name = "VALUEDATE")
    private String valueDate;

    @Column(name = "DESCRIPTION")
    private String description;

    @Column(name = "REFERENCE")
    private String reference;

    @Column(name = "EODBALANCEAMOUNT")
    private String eodBalanceAmount;

    @Column(name = "INTANGIBLECOMMODITYFLAG")
    private String intangibleCommodityFlag;

	public Double getTransactionNo() {
		return transactionNo;
	}

	public void setTransactionNo(Double transactionNo) {
		this.transactionNo = transactionNo;
	}

	public Long getTransactionBatchId() {
		return transactionBatchId;
	}

	public void setTransactionBatchId(Long transactionBatchId) {
		this.transactionBatchId = transactionBatchId;
	}

	public Double getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(Double transactionId) {
		this.transactionId = transactionId;
	}

	public Long getTransactionType() {
		return transactionType;
	}

	public void setTransactionType(Long transactionType) {
		this.transactionType = transactionType;
	}

	public String getFlowCode() {
		return flowCode;
	}

	public void setFlowCode(String flowCode) {
		this.flowCode = flowCode;
	}

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
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

	public LocalDate getInstrumentDate() {
		return instrumentDate;
	}

	public void setInstrumentDate(LocalDate instrumentDate) {
		this.instrumentDate = instrumentDate;
	}

	public String getAccountNo() {
		return accountNo;
	}

	public void setAccountNo(String accountNo) {
		this.accountNo = accountNo;
	}

	public Long getBranchCode() {
		return branchCode;
	}

	public void setBranchCode(Long branchCode) {
		this.branchCode = branchCode;
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

	public String getCounterAccountNo() {
		return counterAccountNo;
	}

	public void setCounterAccountNo(String counterAccountNo) {
		this.counterAccountNo = counterAccountNo;
	}

	public String getCounterpartyType() {
		return counterpartyType;
	}

	public void setCounterpartyType(String counterpartyType) {
		this.counterpartyType = counterpartyType;
	}

	public String getCounterBranchCode() {
		return counterBranchCode;
	}

	public void setCounterBranchCode(String counterBranchCode) {
		this.counterBranchCode = counterBranchCode;
	}

	public String getCounterBankCode() {
		return counterBankCode;
	}

	public void setCounterBankCode(String counterBankCode) {
		this.counterBankCode = counterBankCode;
	}

	public String getCurrencyCode() {
		return currencyCode;
	}

	public void setCurrencyCode(String currencyCode) {
		this.currencyCode = currencyCode;
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

	public String getChannelId() {
		return channelId;
	}

	public void setChannelId(String channelId) {
		this.channelId = channelId;
	}

	public String getMicrCode() {
		return micrCode;
	}

	public void setMicrCode(String micrCode) {
		this.micrCode = micrCode;
	}

	public String getCounterCountryCode() {
		return counterCountryCode;
	}

	public void setCounterCountryCode(String counterCountryCode) {
		this.counterCountryCode = counterCountryCode;
	}

	public String getChannelType() {
		return channelType;
	}

	public void setChannelType(String channelType) {
		this.channelType = channelType;
	}

	public String getForexRemittance() {
		return forexRemittance;
	}

	public void setForexRemittance(String forexRemittance) {
		this.forexRemittance = forexRemittance;
	}

	public String getCounterpartyAddress() {
		return counterpartyAddress;
	}

	public void setCounterpartyAddress(String counterpartyAddress) {
		this.counterpartyAddress = counterpartyAddress;
	}

	public String getBalanceAmount() {
		return balanceAmount;
	}

	public void setBalanceAmount(String balanceAmount) {
		this.balanceAmount = balanceAmount;
	}

	public String getPostingDate() {
		return postingDate;
	}

	public void setPostingDate(String postingDate) {
		this.postingDate = postingDate;
	}

	public LocalDate getTransactionDateTime() {
		return transactionDateTime;
	}

	public void setTransactionDateTime(LocalDate transactionDateTime) {
		this.transactionDateTime = transactionDateTime;
	}

	public String getRemarks() {
		return remarks;
	}

	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	public String getRefNo() {
		return refNo;
	}

	public void setRefNo(String refNo) {
		this.refNo = refNo;
	}

	public String getPostedUserId() {
		return postedUserId;
	}

	public void setPostedUserId(String postedUserId) {
		this.postedUserId = postedUserId;
	}

	public String getAmountInAccountCurrency() {
		return amountInAccountCurrency;
	}

	public void setAmountInAccountCurrency(String amountInAccountCurrency) {
		this.amountInAccountCurrency = amountInAccountCurrency;
	}

	public String getCreationUserId() {
		return creationUserId;
	}

	public void setCreationUserId(String creationUserId) {
		this.creationUserId = creationUserId;
	}

	public String getOriginalAmount() {
		return originalAmount;
	}

	public void setOriginalAmount(String originalAmount) {
		this.originalAmount = originalAmount;
	}

	public String getCounterpartyNationality() {
		return counterpartyNationality;
	}

	public void setCounterpartyNationality(String counterpartyNationality) {
		this.counterpartyNationality = counterpartyNationality;
	}

	public Timestamp getUpdateTimestamp() {
		return updateTimestamp;
	}

	public void setUpdateTimestamp(Timestamp updateTimestamp) {
		this.updateTimestamp = updateTimestamp;
	}

	public String getCommodityCode() {
		return commodityCode;
	}

	public void setCommodityCode(String commodityCode) {
		this.commodityCode = commodityCode;
	}

	public String getIntangibleFlag() {
		return intangibleFlag;
	}

	public void setIntangibleFlag(String intangibleFlag) {
		this.intangibleFlag = intangibleFlag;
	}

	public String getInvoiceValue() {
		return invoiceValue;
	}

	public void setInvoiceValue(String invoiceValue) {
		this.invoiceValue = invoiceValue;
	}

	public String getBoeDate() {
		return boeDate;
	}

	public void setBoeDate(String boeDate) {
		this.boeDate = boeDate;
	}

	public String getBoeAmount() {
		return boeAmount;
	}

	public void setBoeAmount(String boeAmount) {
		this.boeAmount = boeAmount;
	}

	public String getTradeRefNumber() {
		return tradeRefNumber;
	}

	public void setTradeRefNumber(String tradeRefNumber) {
		this.tradeRefNumber = tradeRefNumber;
	}

	public String getTradeType() {
		return tradeType;
	}

	public void setTradeType(String tradeType) {
		this.tradeType = tradeType;
	}

	public String getAdvanceRemitFlag() {
		return advanceRemitFlag;
	}

	public void setAdvanceRemitFlag(String advanceRemitFlag) {
		this.advanceRemitFlag = advanceRemitFlag;
	}

	public String getExportImportDate() {
		return exportImportDate;
	}

	public void setExportImportDate(String exportImportDate) {
		this.exportImportDate = exportImportDate;
	}

	public String getShippingRoute() {
		return shippingRoute;
	}

	public void setShippingRoute(String shippingRoute) {
		this.shippingRoute = shippingRoute;
	}

	public String getFirstTimeImport() {
		return firstTimeImport;
	}

	public void setFirstTimeImport(String firstTimeImport) {
		this.firstTimeImport = firstTimeImport;
	}

	public String getAcctCurrencyCode() {
		return acctCurrencyCode;
	}

	public void setAcctCurrencyCode(String acctCurrencyCode) {
		this.acctCurrencyCode = acctCurrencyCode;
	}

	public String getAmountInOriginalCurrency() {
		return amountInOriginalCurrency;
	}

	public void setAmountInOriginalCurrency(String amountInOriginalCurrency) {
		this.amountInOriginalCurrency = amountInOriginalCurrency;
	}

	public String getPurposeCode() {
		return purposeCode;
	}

	public void setPurposeCode(String purposeCode) {
		this.purposeCode = purposeCode;
	}

	public String getFirstTradeTrnFlag() {
		return firstTradeTrnFlag;
	}

	public void setFirstTradeTrnFlag(String firstTradeTrnFlag) {
		this.firstTradeTrnFlag = firstTradeTrnFlag;
	}

	public String getTradeIdentifier() {
		return tradeIdentifier;
	}

	public void setTradeIdentifier(String tradeIdentifier) {
		this.tradeIdentifier = tradeIdentifier;
	}

	public String getTransactionTime() {
		return transactionTime;
	}

	public void setTransactionTime(String transactionTime) {
		this.transactionTime = transactionTime;
	}

	public String getValueDate() {
		return valueDate;
	}

	public void setValueDate(String valueDate) {
		this.valueDate = valueDate;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getReference() {
		return reference;
	}

	public void setReference(String reference) {
		this.reference = reference;
	}

	public String getEodBalanceAmount() {
		return eodBalanceAmount;
	}

	public void setEodBalanceAmount(String eodBalanceAmount) {
		this.eodBalanceAmount = eodBalanceAmount;
	}

	public String getIntangibleCommodityFlag() {
		return intangibleCommodityFlag;
	}

	public void setIntangibleCommodityFlag(String intangibleCommodityFlag) {
		this.intangibleCommodityFlag = intangibleCommodityFlag;
	}

    
}

