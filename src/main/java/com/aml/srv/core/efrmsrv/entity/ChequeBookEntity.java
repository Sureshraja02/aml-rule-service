package com.aml.srv.core.efrmsrv.entity;


import jakarta.persistence.*;
import java.time.LocalDate;

@Entity
@Table(name = "FS_CHEQUE")
public class ChequeBookEntity {

    @Id
    @Column(name = "CHEQUEBOOKNO")
    private String chequeBookNo;   // VARCHAR → String (Primary Key)

    @Column(name = "ACCOUNTNO")
    private String accountNo;

    @Column(name = "CHEQUEBOOKISSUEDDATE")
    private LocalDate chequeBookIssuedDate;

    @Column(name = "NOOFLEAVES")
    private Long noOfLeaves;

    @Column(name = "UPDATETIMESTAMP")
    private LocalDate updateTimestamp;

    @Column(name = "CUSTOMERID")
    private String customerId;

    @Column(name = "STARTINGCHEQUENO")
    private Long startingChequeNo;

    @Column(name = "ENDNUMBER")
    private Long endNumber;

	public String getChequeBookNo() {
		return chequeBookNo;
	}

	public void setChequeBookNo(String chequeBookNo) {
		this.chequeBookNo = chequeBookNo;
	}

	public String getAccountNo() {
		return accountNo;
	}

	public void setAccountNo(String accountNo) {
		this.accountNo = accountNo;
	}

	public LocalDate getChequeBookIssuedDate() {
		return chequeBookIssuedDate;
	}

	public void setChequeBookIssuedDate(LocalDate chequeBookIssuedDate) {
		this.chequeBookIssuedDate = chequeBookIssuedDate;
	}

	public Long getNoOfLeaves() {
		return noOfLeaves;
	}

	public void setNoOfLeaves(Long noOfLeaves) {
		this.noOfLeaves = noOfLeaves;
	}

	public LocalDate getUpdateTimestamp() {
		return updateTimestamp;
	}

	public void setUpdateTimestamp(LocalDate updateTimestamp) {
		this.updateTimestamp = updateTimestamp;
	}

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public Long getStartingChequeNo() {
		return startingChequeNo;
	}

	public void setStartingChequeNo(Long startingChequeNo) {
		this.startingChequeNo = startingChequeNo;
	}

	public Long getEndNumber() {
		return endNumber;
	}

	public void setEndNumber(Long endNumber) {
		this.endNumber = endNumber;
	}

    
}
