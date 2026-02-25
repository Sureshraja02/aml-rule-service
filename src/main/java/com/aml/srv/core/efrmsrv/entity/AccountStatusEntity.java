package com.aml.srv.core.efrmsrv.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_ACCOUNT_STATUS")
public class AccountStatusEntity {

	@Id
	@Column(name = "ACCOUNTNO")
	private String accountNo; // VARCHAR → String (Primary Key)

	@Column(name = "CHANGEDATE")
	private String changeDate; // DATE → LocalDate

	@Column(name = "STATUS")
	private String status; // VARCHAR → String

	@Column(name = "UPDATETIMESTAMP")
	private String updateTimestamp; // DATE → LocalDate

	public String getAccountNo() {
		return accountNo;
	}

	public void setAccountNo(String accountNo) {
		this.accountNo = accountNo;
	}

	public String getChangeDate() {
		return changeDate;
	}

	public void setChangeDate(String changeDate) {
		this.changeDate = changeDate;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getUpdateTimestamp() {
		return updateTimestamp;
	}

	public void setUpdateTimestamp(String updateTimestamp) {
		this.updateTimestamp = updateTimestamp;
	}
}
