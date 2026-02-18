package com.aml.srv.core.efrmsrv.entity;

import java.math.BigDecimal;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_MAB")
public class MonthlyAverageEntity {

	    @Id
	    @Column(name = "ACCOUNTNO")
	    private String accountNo;

	    @Column(name = "MINBALANCE")
	    private BigDecimal minBalance;

		public String getAccountNo() {
			return accountNo;
		}

		public void setAccountNo(String accountNo) {
			this.accountNo = accountNo;
		}

		public BigDecimal getMinBalance() {
			return minBalance;
		}

		public void setMinBalance(BigDecimal minBalance) {
			this.minBalance = minBalance;
		}
	    
	    
}
