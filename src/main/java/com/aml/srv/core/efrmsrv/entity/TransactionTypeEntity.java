package com.aml.srv.core.efrmsrv.entity;



import jakarta.persistence.*;

@Entity
@Table(name = "FS_TRANS_TYPE")
public class TransactionTypeEntity {

    @Id
    @Column(name = "TRANSACTIONTYPE")
    private String transactionType;   // VARCHAR → String (Primary Key)

    @Column(name = "TRANSUBTYPE")
    private String tranSubType;

    @Column(name = "DESCRIPTION")
    private String description;

    @Column(name = "AMOUNTTYPE")
    private String amountType;

    @Column(name = "UPDATETIMESTAMP")
    private String updateTimestamp;

    @Column(name = "SECURITYID")
    private String securityId;

    @Column(name = "CLS_CODE")
    private String clsCode;

    @Column(name = "CLS_SUB_CODE")
    private String clsSubCode;

    @Column(name = "ISCASH")
    private String isCash;

    @Column(name = "TRANSACTIONGROUP")
    private String transactionGroup;

    @Column(name = "TRANSACTIONNAME")
    private String transactionName;

    @Column(name = "ISCLEARING")
    private String isClearing;

    @Column(name = "ISTRANSFER")
    private String isTransfer;

    @Column(name = "ISEFT")
    private String isEft;

	public String getTransactionType() {
		return transactionType;
	}

	public void setTransactionType(String transactionType) {
		this.transactionType = transactionType;
	}

	public String getTranSubType() {
		return tranSubType;
	}

	public void setTranSubType(String tranSubType) {
		this.tranSubType = tranSubType;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getAmountType() {
		return amountType;
	}

	public void setAmountType(String amountType) {
		this.amountType = amountType;
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

	public String getClsCode() {
		return clsCode;
	}

	public void setClsCode(String clsCode) {
		this.clsCode = clsCode;
	}

	public String getClsSubCode() {
		return clsSubCode;
	}

	public void setClsSubCode(String clsSubCode) {
		this.clsSubCode = clsSubCode;
	}

	public String getIsCash() {
		return isCash;
	}

	public void setIsCash(String isCash) {
		this.isCash = isCash;
	}

	public String getTransactionGroup() {
		return transactionGroup;
	}

	public void setTransactionGroup(String transactionGroup) {
		this.transactionGroup = transactionGroup;
	}

	public String getTransactionName() {
		return transactionName;
	}

	public void setTransactionName(String transactionName) {
		this.transactionName = transactionName;
	}

	public String getIsClearing() {
		return isClearing;
	}

	public void setIsClearing(String isClearing) {
		this.isClearing = isClearing;
	}

	public String getIsTransfer() {
		return isTransfer;
	}

	public void setIsTransfer(String isTransfer) {
		this.isTransfer = isTransfer;
	}

	public String getIsEft() {
		return isEft;
	}

	public void setIsEft(String isEft) {
		this.isEft = isEft;
	}

    
}
