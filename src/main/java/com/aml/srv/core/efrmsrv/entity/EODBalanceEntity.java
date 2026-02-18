package com.aml.srv.core.efrmsrv.entity;


import java.sql.Timestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_EOD_BALANCES")
public class EODBalanceEntity {

    @Id
    @Column(name = "ACCOUNTNO")
    private String accountNo;   // VARCHAR → String (Primary Key)

    @Column(name = "UPDATETIMESTAMP")
    private Timestamp updateTimestamp;   // DATE → Timestamp

    @Column(name = "EODBALANCE")
    private Double eodBalance;   // DOUBLE → Double

    @Column(name = "BALANCEDATE")
    private Timestamp balanceDate;   // DATE → Timestamp

    // Getters and Setters (or use Lombok @Data)
    public String getAccountNo() {
        return accountNo;
    }

    public void setAccountNo(String accountNo) {
        this.accountNo = accountNo;
    }

    public Timestamp getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(Timestamp updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }

    public Double getEodBalance() {
        return eodBalance;
    }

    public void setEodBalance(Double eodBalance) {
        this.eodBalance = eodBalance;
    }

    public Timestamp getBalanceDate() {
        return balanceDate;
    }

    public void setBalanceDate(Timestamp balanceDate) {
        this.balanceDate = balanceDate;
    }
}
