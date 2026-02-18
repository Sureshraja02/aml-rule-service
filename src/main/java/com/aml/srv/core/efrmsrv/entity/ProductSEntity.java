package com.aml.srv.core.efrmsrv.entity;


import jakarta.persistence.*;

@Entity
@Table(name = "FS_PRD")
public class ProductSEntity {

    @Id
    @Column(name = "PRODUCTCODE")
    private Long productCode;   // BIGINT → Long

    @Column(name = "PRODUCTNAME")
    private String productName;

    @Column(name = "DESCRIPTION")
    private String description;

    @Column(name = "INTERNALPRODUCTID")
    private String internalProductId;

    @Column(name = "PARENTCODE")
    private String parentCode;

    @Column(name = "ISSERVICE")
    private String isService;

    @Column(name = "MINBALANCE")
    private Long minBalance;

    @Column(name = "PRODUCTALIASNAME")
    private String productAliasName;

    @Column(name = "RISKRATING")
    private String riskRating;

    @Column(name = "UPDATETIMESTAMP")
    private String updateTimestamp;

    @Column(name = "SECURITYID")
    private String securityId;

    @Column(name = "INTERNATIONALTRANSFER")
    private String internationalTransfer;

    @Column(name = "SUBSWITHDRAWALS")
    private String subsWithdrawals;

    @Column(name = "EXEMPTPRODUCTS")
    private String exemptProducts;

    @Column(name = "SCORE")
    private String score;

    @Column(name = "ISGL")
    private String isGl;

    @Column(name = "PRODUCTGROUPCODE")
    private String productGroupCode;

	public Long getProductCode() {
		return productCode;
	}

	public void setProductCode(Long productCode) {
		this.productCode = productCode;
	}

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getInternalProductId() {
		return internalProductId;
	}

	public void setInternalProductId(String internalProductId) {
		this.internalProductId = internalProductId;
	}

	public String getParentCode() {
		return parentCode;
	}

	public void setParentCode(String parentCode) {
		this.parentCode = parentCode;
	}

	public String getIsService() {
		return isService;
	}

	public void setIsService(String isService) {
		this.isService = isService;
	}

	public Long getMinBalance() {
		return minBalance;
	}

	public void setMinBalance(Long minBalance) {
		this.minBalance = minBalance;
	}

	public String getProductAliasName() {
		return productAliasName;
	}

	public void setProductAliasName(String productAliasName) {
		this.productAliasName = productAliasName;
	}

	public String getRiskRating() {
		return riskRating;
	}

	public void setRiskRating(String riskRating) {
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

	public String getInternationalTransfer() {
		return internationalTransfer;
	}

	public void setInternationalTransfer(String internationalTransfer) {
		this.internationalTransfer = internationalTransfer;
	}

	public String getSubsWithdrawals() {
		return subsWithdrawals;
	}

	public void setSubsWithdrawals(String subsWithdrawals) {
		this.subsWithdrawals = subsWithdrawals;
	}

	public String getExemptProducts() {
		return exemptProducts;
	}

	public void setExemptProducts(String exemptProducts) {
		this.exemptProducts = exemptProducts;
	}

	public String getScore() {
		return score;
	}

	public void setScore(String score) {
		this.score = score;
	}

	public String getIsGl() {
		return isGl;
	}

	public void setIsGl(String isGl) {
		this.isGl = isGl;
	}

	public String getProductGroupCode() {
		return productGroupCode;
	}

	public void setProductGroupCode(String productGroupCode) {
		this.productGroupCode = productGroupCode;
	}

    
    
    // Getters and Setters (or use Lombok @Data)
}
