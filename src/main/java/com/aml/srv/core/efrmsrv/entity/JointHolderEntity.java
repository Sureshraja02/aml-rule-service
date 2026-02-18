package com.aml.srv.core.efrmsrv.entity;


import jakarta.persistence.*;
import java.time.LocalDate;

@Entity
@Table(name = "FS_JTH")
public class JointHolderEntity {

    @Id
    @Column(name = "ACCOUNTNO")
    private String accountNo;   // VARCHAR → String (Primary Key)

    @Column(name = "CUSTOMERID")
    private String customerId;

    @Column(name = "JOINTHOLDERTYPE")
    private String jointHolderType;

    @Column(name = "JOINTHOLDERNAME")
    private String jointHolderName;

    @Column(name = "RELATIONCODE")
    private String relationCode;

    @Column(name = "UPDATETIMESTAMP")
    private LocalDate updateTimestamp;   // DATE → LocalDate

	public String getAccountNo() {
		return accountNo;
	}

	public void setAccountNo(String accountNo) {
		this.accountNo = accountNo;
	}

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public String getJointHolderType() {
		return jointHolderType;
	}

	public void setJointHolderType(String jointHolderType) {
		this.jointHolderType = jointHolderType;
	}

	public String getJointHolderName() {
		return jointHolderName;
	}

	public void setJointHolderName(String jointHolderName) {
		this.jointHolderName = jointHolderName;
	}

	public String getRelationCode() {
		return relationCode;
	}

	public void setRelationCode(String relationCode) {
		this.relationCode = relationCode;
	}

	public LocalDate getUpdateTimestamp() {
		return updateTimestamp;
	}

	public void setUpdateTimestamp(LocalDate updateTimestamp) {
		this.updateTimestamp = updateTimestamp;
	}

    
}
