package com.aml.srv.core.efrmsrv.entity;

import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FINSENTINEL_RISK")
public class FinsentinelRiskEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "ID")
	private Integer id;

	@Column(name = "CUSTOMER_ID")
	private String customerId;

	@Column(name = "WEEKLY_AVG_DEBIT_AMOUNT")
	private String weeklyAvgDebitAmount;

	@Column(name = "MONTHLY_AVG_DEBIT_AMOUNT")
	private String monthlyAvgDebitAmount;

	@Column(name = "MOST_ACTIVE_WEEKDAY")
	private String mostActiveWeekDay;

	@Column(name = "MOST_ACTIVE_HOUR")
	private String mostActiveHour;

	@Column(name = "WEEKLY_LOW_AVG_AMOUNT")
	private String weeklyLowAvgAmount;

	@Column(name = "WEEKLY_HIGH_AVG_AMOUNT")
	private String weeklyHighAvgAmount;

	@Column(name = "FOREIGN_TRANSACTION_FLAG")
	private String foreignTransactionFlag;

	@Column(name = "VOLATILITY_RISK")
	private String volatilityRisk;

	@Column(name = "DEVIATION_RISK")
	private String deviationRisk;

	@Column(name = "TIME_RISK")
	private String timeRisk;

	@Column(name = "FOREIGN_RISK")
	private String foreignRisk;

	@Column(name = "RISK_SCORE")
	private String riskScore;

	@Column(name = "RISK_PROBABILITY")
	private String riskProbability;

	@Column(name = "RISK_LEVEL")
	private String riskLevel;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public String getWeeklyAvgDebitAmount() {
		return weeklyAvgDebitAmount;
	}

	public void setWeeklyAvgDebitAmount(String weeklyAvgDebitAmount) {
		this.weeklyAvgDebitAmount = weeklyAvgDebitAmount;
	}

	public String getMonthlyAvgDebitAmount() {
		return monthlyAvgDebitAmount;
	}

	public void setMonthlyAvgDebitAmount(String monthlyAvgDebitAmount) {
		this.monthlyAvgDebitAmount = monthlyAvgDebitAmount;
	}

	public String getMostActiveWeekDay() {
		return mostActiveWeekDay;
	}

	public void setMostActiveWeekDay(String mostActiveWeekDay) {
		this.mostActiveWeekDay = mostActiveWeekDay;
	}

	public String getMostActiveHour() {
		return mostActiveHour;
	}

	public void setMostActiveHour(String mostActiveHour) {
		this.mostActiveHour = mostActiveHour;
	}

	public String getWeeklyLowAvgAmount() {
		return weeklyLowAvgAmount;
	}

	public void setWeeklyLowAvgAmount(String weeklyLowAvgAmount) {
		this.weeklyLowAvgAmount = weeklyLowAvgAmount;
	}

	public String getWeeklyHighAvgAmount() {
		return weeklyHighAvgAmount;
	}

	public void setWeeklyHighAvgAmount(String weeklyHighAvgAmount) {
		this.weeklyHighAvgAmount = weeklyHighAvgAmount;
	}

	public String getForeignTransactionFlag() {
		return foreignTransactionFlag;
	}

	public void setForeignTransactionFlag(String foreignTransactionFlag) {
		this.foreignTransactionFlag = foreignTransactionFlag;
	}

	public String getVolatilityRisk() {
		return volatilityRisk;
	}

	public void setVolatilityRisk(String volatilityRisk) {
		this.volatilityRisk = volatilityRisk;
	}

	public String getDeviationRisk() {
		return deviationRisk;
	}

	public void setDeviationRisk(String deviationRisk) {
		this.deviationRisk = deviationRisk;
	}

	public String getTimeRisk() {
		return timeRisk;
	}

	public void setTimeRisk(String timeRisk) {
		this.timeRisk = timeRisk;
	}

	public String getForeignRisk() {
		return foreignRisk;
	}

	public void setForeignRisk(String foreignRisk) {
		this.foreignRisk = foreignRisk;
	}

	public String getRiskScore() {
		return riskScore;
	}

	public void setRiskScore(String riskScore) {
		this.riskScore = riskScore;
	}

	public String getRiskProbability() {
		return riskProbability;
	}

	public void setRiskProbability(String riskProbability) {
		this.riskProbability = riskProbability;
	}

	public String getRiskLevel() {
		return riskLevel;
	}

	public void setRiskLevel(String riskLevel) {
		this.riskLevel = riskLevel;
	}

}
