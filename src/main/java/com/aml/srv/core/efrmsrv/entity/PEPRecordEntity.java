package com.aml.srv.core.efrmsrv.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "FS_PEP_DATA")
public class PEPRecordEntity {

	@Id
	@Column(name = "PERSON_ID", length = 36, nullable = false)
	private String personId; // UUID or surrogate PK

	@Column(name = "PERSON", length = 500)
	private String person;

	@Column(name = "person_name", length = 500)
	private String personName;

	@Column(name = "BIRTH_DATE")
	private String birthDate;

	@Column(name = "COUNTRIES", length = 200)
	private String countries; // ISO codes or comma-separated

	@Column(name = "DATASET", length = 100)
	private String dataset;

	@Column(name = "PROGRAM_IDS", length = 100)
	private String programIds;

	@Column(name = "FIRST_SEEN")
	private String firstSeen;

	@Column(name = "LAST_SEEN")
	private String lastSeen;

	@Column(name = "LAST_CHANGE")
	private String lastChange;

	@Column(name = "SANCTIONS", length = 500)
	private String sanctions;

	@Column(name = "IDENTIFIERS", length = 500)
	private String identifiers;

	

	// --- Getters & Setters ---
	public String getPersonId() {
		return personId;
	}

	public void setPersonId(String personId) {
		this.personId = personId;
	}

	public String getPerson() {
		return person;
	}

	public void setPerson(String person) {
		this.person = person;
	}

	public String getBirthDate() {
		return birthDate;
	}

	public void setBirthDate(String birthDate) {
		this.birthDate = birthDate;
	}

	public String getCountries() {
		return countries;
	}

	public void setCountries(String countries) {
		this.countries = countries;
	}

	public String getDataset() {
		return dataset;
	}

	public void setDataset(String dataset) {
		this.dataset = dataset;
	}

	public String getProgramIds() {
		return programIds;
	}

	public void setProgramIds(String programIds) {
		this.programIds = programIds;
	}

	public String getFirstSeen() {
		return firstSeen;
	}

	public void setFirstSeen(String firstSeen) {
		this.firstSeen = firstSeen;
	}

	public String getLastSeen() {
		return lastSeen;
	}

	public void setLastSeen(String lastSeen) {
		this.lastSeen = lastSeen;
	}

	public String getLastChange() {
		return lastChange;
	}

	public void setLastChange(String lastChange) {
		this.lastChange = lastChange;
	}

	public String getSanctions() {
		return sanctions;
	}

	public void setSanctions(String sanctions) {
		this.sanctions = sanctions;
	}

	public String getIdentifiers() {
		return identifiers;
	}

	public void setIdentifiers(String identifiers) {
		this.identifiers = identifiers;
	}


	public String getPersonName() {
		return personName;
	}

	public void setPersonName(String personName) {
		this.personName = personName;
	}
	
}
