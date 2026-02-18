package com.aml.srv.core.efrmsrv.repo;

import java.math.BigDecimal;

public class TransactionDetailsDTO {

	public BigDecimal sumAmount = null;
	
	public BigDecimal minAmount = null;
	
	public BigDecimal maxAmount = null;
	
	public BigDecimal txnAmount = null;
	
	public Long countAmount = null;
	
	public Long COuntDistcounterpartyAccountNo = null;
	
	public Long CountcounterpartyAccountNo = null;
	
	public Double avgAmount=null;
	
	public String counterContryCode=null;
	
	private String transDate=null;
	
	private String counterLocation=null;
	
	public Long counterAccountNo = null;

	public BigDecimal getSumAmount() {
		return sumAmount;
	}

	public void setSumAmount(BigDecimal sumAmount) {
		this.sumAmount = sumAmount;
	}

	public BigDecimal getMinAmount() {
		return minAmount;
	}

	public void setMinAmount(BigDecimal minAmount) {
		this.minAmount = minAmount;
	}

	public BigDecimal getMaxAmount() {
		return maxAmount;
	}

	public void setMaxAmount(BigDecimal maxAmount) {
		this.maxAmount = maxAmount;
	}

	public Long getCountAmount() {
		return countAmount;
	}

	public void setCountAmount(Long countAmount) {
		this.countAmount = countAmount;
	}

	public Double getAvgAmount() {
		return avgAmount;
	}

	public void setAvgAmount(Double avgAmount) {
		this.avgAmount = avgAmount;
	}

	public String getCounterContryCode() {
		return counterContryCode;
	}

	public void setCounterContryCode(String counterContryCode) {
		this.counterContryCode = counterContryCode;
	}

	public Long getCOuntDistcounterpartyAccountNo() {
		return COuntDistcounterpartyAccountNo;
	}

	public void setCOuntDistcounterpartyAccountNo(Long cOuntDistcounterpartyAccountNo) {
		COuntDistcounterpartyAccountNo = cOuntDistcounterpartyAccountNo;
	}

	public Long getCountcounterpartyAccountNo() {
		return CountcounterpartyAccountNo;
	}

	public void setCountcounterpartyAccountNo(Long countcounterpartyAccountNo) {
		CountcounterpartyAccountNo = countcounterpartyAccountNo;
	}

	public BigDecimal getTxnAmount() {
		return txnAmount;
	}

	public void setTxnAmount(BigDecimal txnAmount) {
		this.txnAmount = txnAmount;
	}

	public String getTransDate() {
		return transDate;
	}

	public void setTransDate(String transDate) {
		this.transDate = transDate;
	}

	public String getCounterLocation() {
		return counterLocation;
	}

	public void setCounterLocation(String counterLocation) {
		this.counterLocation = counterLocation;
	}

	public Long getCounterAccountNo() {
		return counterAccountNo;
	}

	public void setCounterAccountNo(Long counterAccountNo) {
		this.counterAccountNo = counterAccountNo;
	}
	
	
	
}
