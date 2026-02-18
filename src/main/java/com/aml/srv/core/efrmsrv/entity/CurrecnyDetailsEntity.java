package com.aml.srv.core.efrmsrv.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_CURM")
public class CurrecnyDetailsEntity {


	    @Id
	    @Column(name = "CURRENCYCODE")
	    private String currencyCode;

	    @Column(name = "CURRENCYNAME")
	    private String currencyName;

	    @Column(name = "COUNTRYCODE")
	    private String countryCode;

	    @Column(name = "RATECODE")
	    private String rateCode;

	    @Column(name = "RATE")
	    private String rate;

	    @Column(name = "RATEDATE")
	    private String rateDate;

	    @Column(name = "SYMBOL")
	    private String symbol;

	    @Column(name = "REMARKS")
	    private String remarks;

	    @Column(name = "UPDATETIMESTAMP")
	    private String updateTimestamp;

	    @Column(name = "SECURITYID")
	    private String securityId;

		public String getCurrencyCode() {
			return currencyCode;
		}

		public void setCurrencyCode(String currencyCode) {
			this.currencyCode = currencyCode;
		}

		public String getCurrencyName() {
			return currencyName;
		}

		public void setCurrencyName(String currencyName) {
			this.currencyName = currencyName;
		}

		public String getCountryCode() {
			return countryCode;
		}

		public void setCountryCode(String countryCode) {
			this.countryCode = countryCode;
		}

		public String getRateCode() {
			return rateCode;
		}

		public void setRateCode(String rateCode) {
			this.rateCode = rateCode;
		}

		public String getRate() {
			return rate;
		}

		public void setRate(String rate) {
			this.rate = rate;
		}

		public String getRateDate() {
			return rateDate;
		}

		public void setRateDate(String rateDate) {
			this.rateDate = rateDate;
		}

		public String getSymbol() {
			return symbol;
		}

		public void setSymbol(String symbol) {
			this.symbol = symbol;
		}

		public String getRemarks() {
			return remarks;
		}

		public void setRemarks(String remarks) {
			this.remarks = remarks;
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

	    
	    
	    // Getters and Setters (or use Lombok @Data)
	}


