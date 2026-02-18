package com.aml.srv.core.efrmsrv.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_INSTRUMENTS")
public class InstrumentEntity {

    @Id
    @Column(name = "INSTRUMENTCODE")
    private String instrumentCode;

    @Column(name = "INSTRUMENTNAME")
    private String instrumentName;

    @Column(name = "DESCRIPTION")
    private String description;

    @Column(name = "ISCASH")
    private String isCash;

    @Column(name = "UPDATETIMESTAMP")
    private String updateTimestamp;

    @Column(name = "SECURITYID")
    private String securityId;

	public String getInstrumentCode() {
		return instrumentCode;
	}

	public void setInstrumentCode(String instrumentCode) {
		this.instrumentCode = instrumentCode;
	}

	public String getInstrumentName() {
		return instrumentName;
	}

	public void setInstrumentName(String instrumentName) {
		this.instrumentName = instrumentName;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getIsCash() {
		return isCash;
	}

	public void setIsCash(String isCash) {
		this.isCash = isCash;
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

   
}
