package com.aml.srv.core.efrmsrv.entity;


import java.time.LocalDate;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_ACCOUNT_STATUS")
public class AccountStatusAuditEntity {

    @Id
    @Column(name = "ACCOUNTNO")
    private String accountNo;   // VARCHAR → String (Primary Key)

    @Column(name = "CHANGEDATE")
    private LocalDate changeDate;   // DATE → LocalDate

    @Column(name = "STATUS")
    private String status;   // VARCHAR → String

    @Column(name = "UPDATETIMESTAMP")
    private LocalDate updateTimestamp;   // DATE → LocalDate

    // Getters and Setters (or use Lombok @Data)
    public String getAccountNo() {
        return accountNo;
    }

    public void setAccountNo(String accountNo) {
        this.accountNo = accountNo;
    }

    public LocalDate getChangeDate() {
        return changeDate;
    }

    public void setChangeDate(LocalDate changeDate) {
        this.changeDate = changeDate;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDate getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(LocalDate updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }
}
