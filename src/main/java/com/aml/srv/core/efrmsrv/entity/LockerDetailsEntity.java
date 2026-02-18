package com.aml.srv.core.efrmsrv.entity;


import jakarta.persistence.*;

@Entity
@Table(name = "FS_LCKR")
public class LockerDetailsEntity {

    @Id
    @Column(name = "LOCKERNO")
    private String lockerNo;   // VARCHAR → String (Primary Key)

    @Column(name = "CUSTOMERID")
    private String customerId;

    @Column(name = "ACCOUNTNO")
    private String accountNo;

    @Column(name = "CUSTOMERNAME")
    private String customerName;

    @Column(name = "INDATETIME")
    private String inDateTime;

    @Column(name = "OUTDATETIME")
    private String outDateTime;

    @Column(name = "UPDATETIMESTAMP")
    private String updateTimestamp;

    @Column(name = "SECURITYID")
    private String securityId;

	public String getLockerNo() {
		return lockerNo;
	}

	public void setLockerNo(String lockerNo) {
		this.lockerNo = lockerNo;
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

	public String getInDateTime() {
		return inDateTime;
	}

	public void setInDateTime(String inDateTime) {
		this.inDateTime = inDateTime;
	}

	public String getOutDateTime() {
		return outDateTime;
	}

	public void setOutDateTime(String outDateTime) {
		this.outDateTime = outDateTime;
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
