package com.aml.srv.core.efrmsrv.entity;


import jakarta.persistence.*;

@Entity
@Table(name = "FS_CNTRY")
public class CountryDetailsEntity {

    @Id
    @Column(name = "COUNTRYCODE")
    private String countryCode;   // VARCHAR → String (Primary Key)

    @Column(name = "COUNTRYNAME")
    private String countryName;

    @Column(name = "GEOGRAPHICREGIONCODE")
    private String geographicRegionCode;

    @Column(name = "FATFMEMBER")
    private String fatfMember;

    @Column(name = "WTOMEMBER")
    private String wtoMember;

    @Column(name = "SECRECYHAVEN")
    private String secrecyHaven;

    @Column(name = "TAXHAVEN")
    private String taxHaven;

    @Column(name = "ISNCCT")
    private String isNcct;

    @Column(name = "RISKRATING")
    private Long riskRating;   // BIGINT → Long

    @Column(name = "UPDATETIMESTAMP")
    private String updateTimestamp;

    @Column(name = "SECURITYID")
    private String securityId;

    @Column(name = "COUNTRYSTATUS")
    private String countryStatus;

    @Column(name = "INTERCOUNTRY")
    private String interCountry;

    @Column(name = "FATFNCCT")
    private String fatfNcct;

    @Column(name = "DRUGTRAFFICKING")
    private String drugTrafficking;

    @Column(name = "OFACTERRORISM")
    private String ofacTerrorism;

    @Column(name = "CURRUPTIONSCORE")
    private String corruptionScore;

    @Column(name = "POLITICALSTABILITY")
    private String politicalStability;

    @Column(name = "AMLEXISTENCE")
    private String amlExistence;

    @Column(name = "NEIGHBOURLYRELATION")
    private String neighbourlyRelation;

    @Column(name = "SCORE")
    private String score;

    @Column(name = "COUNTRYRISKCODE")
    private String countryRiskCode;

    @Column(name = "ISHIGHTFRISK")
    private String isHighTfRisk;

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public String getCountryName() {
		return countryName;
	}

	public void setCountryName(String countryName) {
		this.countryName = countryName;
	}

	public String getGeographicRegionCode() {
		return geographicRegionCode;
	}

	public void setGeographicRegionCode(String geographicRegionCode) {
		this.geographicRegionCode = geographicRegionCode;
	}

	public String getFatfMember() {
		return fatfMember;
	}

	public void setFatfMember(String fatfMember) {
		this.fatfMember = fatfMember;
	}

	public String getWtoMember() {
		return wtoMember;
	}

	public void setWtoMember(String wtoMember) {
		this.wtoMember = wtoMember;
	}

	public String getSecrecyHaven() {
		return secrecyHaven;
	}

	public void setSecrecyHaven(String secrecyHaven) {
		this.secrecyHaven = secrecyHaven;
	}

	public String getTaxHaven() {
		return taxHaven;
	}

	public void setTaxHaven(String taxHaven) {
		this.taxHaven = taxHaven;
	}

	public String getIsNcct() {
		return isNcct;
	}

	public void setIsNcct(String isNcct) {
		this.isNcct = isNcct;
	}

	public Long getRiskRating() {
		return riskRating;
	}

	public void setRiskRating(Long riskRating) {
		this.riskRating = riskRating;
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

	public String getCountryStatus() {
		return countryStatus;
	}

	public void setCountryStatus(String countryStatus) {
		this.countryStatus = countryStatus;
	}

	public String getInterCountry() {
		return interCountry;
	}

	public void setInterCountry(String interCountry) {
		this.interCountry = interCountry;
	}

	public String getFatfNcct() {
		return fatfNcct;
	}

	public void setFatfNcct(String fatfNcct) {
		this.fatfNcct = fatfNcct;
	}

	public String getDrugTrafficking() {
		return drugTrafficking;
	}

	public void setDrugTrafficking(String drugTrafficking) {
		this.drugTrafficking = drugTrafficking;
	}

	public String getOfacTerrorism() {
		return ofacTerrorism;
	}

	public void setOfacTerrorism(String ofacTerrorism) {
		this.ofacTerrorism = ofacTerrorism;
	}

	public String getCorruptionScore() {
		return corruptionScore;
	}

	public void setCorruptionScore(String corruptionScore) {
		this.corruptionScore = corruptionScore;
	}

	public String getPoliticalStability() {
		return politicalStability;
	}

	public void setPoliticalStability(String politicalStability) {
		this.politicalStability = politicalStability;
	}

	public String getAmlExistence() {
		return amlExistence;
	}

	public void setAmlExistence(String amlExistence) {
		this.amlExistence = amlExistence;
	}

	public String getNeighbourlyRelation() {
		return neighbourlyRelation;
	}

	public void setNeighbourlyRelation(String neighbourlyRelation) {
		this.neighbourlyRelation = neighbourlyRelation;
	}

	public String getScore() {
		return score;
	}

	public void setScore(String score) {
		this.score = score;
	}

	public String getCountryRiskCode() {
		return countryRiskCode;
	}

	public void setCountryRiskCode(String countryRiskCode) {
		this.countryRiskCode = countryRiskCode;
	}

	public String getIsHighTfRisk() {
		return isHighTfRisk;
	}

	public void setIsHighTfRisk(String isHighTfRisk) {
		this.isHighTfRisk = isHighTfRisk;
	}

  
}
