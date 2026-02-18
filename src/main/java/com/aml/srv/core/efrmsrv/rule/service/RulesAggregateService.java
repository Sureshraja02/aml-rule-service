package com.aml.srv.core.efrmsrv.rule.service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.entity.AccountDetailsEntity;
import com.aml.srv.core.efrmsrv.entity.AccountStatusEntity;
import com.aml.srv.core.efrmsrv.entity.CustomerDetailsEntity;
import com.aml.srv.core.efrmsrv.entity.FS_FactConditionAttributeEntity;
import com.aml.srv.core.efrmsrv.entity.FS_FactConditionEntity;
import com.aml.srv.core.efrmsrv.filewatcher.service.FileWatcher;
import com.aml.srv.core.efrmsrv.repo.AccountDetailsRepoImpl;
import com.aml.srv.core.efrmsrv.repo.AccountStatusRepositryImpl;
import com.aml.srv.core.efrmsrv.repo.CustomerDetailsRepoImpl;
import com.aml.srv.core.efrmsrv.repo.FS_FactConditionAttributeRepoImpl;
import com.aml.srv.core.efrmsrv.repo.FS_FactConditionRepoImpl;
import com.aml.srv.core.efrmsrv.repo.TransactionDetailsRepositryImpl;
import com.aml.srv.core.efrmsrv.repo.TransactionDetailsRepositryImpl2;
import com.aml.srv.core.efrmsrv.rule.intr.RuleExecutorIntr;
import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.request.Range;
import com.aml.srv.core.efrmsrv.rule.process.request.RuleRequestVo;
import com.aml.srv.core.efrmsrv.rule.process.response.ComputedFactsVO;
import com.aml.srv.core.efrmsrv.utils.AMLConstants;

/**
 * 
 * @author : E5554365 (Prabakaran.R)
 * @Project : aml-srv
 * @year : 2025
 */
@Component
public class RulesAggregateService implements RuleExecutorIntr {

	private final FileWatcher fileWatcher;

	private Logger LOGGER = LoggerFactory.getLogger(RulesAggregateService.class);

	@Autowired
	TransactionDetailsRepositryImpl transactionDetailsRepositryImpl;

	@Autowired
	TransactionDetailsRepositryImpl2 transactionDetailsRepositryImpl2;

	@Autowired
	FS_FactConditionRepoImpl fS_FactConditionRepoImpl;

	@Autowired
	FS_FactConditionAttributeRepoImpl fS_FactConditionAttributeRepoImpl;

	@Autowired
	CustomerDetailsRepoImpl customerDetailsRepoImpl;

	@Autowired
	AccountDetailsRepoImpl accountDetailsRepoImpl;

	@Autowired
	AccountStatusRepositryImpl accountStatusRepositryImpl;

	@Autowired
	RulesUtils rulesUtils;

	RulesAggregateService(FileWatcher fileWatcher) {
		this.fileWatcher = fileWatcher;
	}

	@Override
	public ComputedFactsVO ruleOfCountProcess(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;

			finalValue = transactionDetailsRepositryImpl2.getCountValue(requVoObjParam.getReqId(), accNo, custId,
					transMode, transType, hours, days, months, fieldName, factSetObj.getRange());
			computedFactsVOObj.setFact(factName);
			computedFactsVOObj.setValue(finalValue);

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfCountProcess : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfSUMProcess(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfSUMProcess (SUM) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl.getSumValue(requVoObjParam.getReqId(), accNo, custId,
							transMode, transType, hours, days, months, fieldName, columnName, factSetObj.getRange(),
							factSetObj);
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfSUMProcess : {}", e);
		} finally {
			factName = null;
			accNo = null;
			custId = null;
			transMode = null;
			transType = null;
			fieldName = null;
			LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfSUMProcess (SUM) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfMaxProcess(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfMaxProcess (MAX) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;

		try {

			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			String condition = factSetObj.getCondition();
			BigDecimal finalValue = new BigDecimal(0);

			if (condition != null) {
				if (condition.equals("LOW-CASH-PROFILE")) {
					String profile = null;

					FS_FactConditionEntity conditionEntity = fS_FactConditionRepoImpl.getFactCondititon(condition,
							requVoObjParam.getReqId());
					if (conditionEntity != null && conditionEntity.getId() != null) {

						List<FS_FactConditionAttributeEntity> conditionAttribute = fS_FactConditionAttributeRepoImpl
								.getCondititonAttributes(String.valueOf(conditionEntity.getId()),
										requVoObjParam.getReqId());
						if (conditionAttribute != null && conditionAttribute.size() > 0) {
							CustomerDetailsEntity custDetails = customerDetailsRepoImpl
									.getCustomerDetailsByCustId(custId);
							if (custDetails != null) {
								for (FS_FactConditionAttributeEntity gs : conditionAttribute) {
									if (gs.getAttributes().equals(custDetails.getCustomerCategory())) {
										profile = gs.getAttributes();
									}
								}
							}

						}
					}

					if (profile != null) {
						finalValue = transactionDetailsRepositryImpl2.getMaxValue(requVoObjParam.getReqId(), accNo,
								custId, transMode, transType, hours, days, months, fieldName, factSetObj.getRange());
						computedFactsVOObj.setFact(factName);
						computedFactsVOObj.setValue(finalValue);
						computedFactsVOObj.setStrValue(profile);
					} else {
						computedFactsVOObj.setFact(factName);
						computedFactsVOObj.setValue(finalValue);

					}

				}
			} else {

			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfMaxProcess : {}", e);
		} finally {
			LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfMaxProcess (MAX) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfAVGProcess(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfAVGProcess (AVG) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getAvgValue(requVoObjParam.getReqId(), accNo, custId,
							transMode, transType, hours, days, months, fieldName, columnName, factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfAVGProcess : {}", e);
		} finally {

			LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfAVGProcess (AVG) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfCommAggregateProcess(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfCommAggregateProcess (Common) Called::::::::::",
				requVoObjParam.getReqId());
		try {

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfCommAggregateProcess : {}", e);
		} finally {

			LOGGER.info(
					"REQID : [{}]::::::::::::RulesExecutorService@ruleOfCommAggregateProcess (Common) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfPreviousForexTurnoverProcess(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info(
				"REQID : [{}]::::::::::::RulesExecutorService@ruleOfPreviousForexTurnoverProcess (PREVIOUS_FOREX_TURNOVER) Called::::::::::",
				requVoObjParam.getReqId());
		try {

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfPreviousForexTurnoverProcess : {}", e);
		} finally {

			LOGGER.info(
					"REQID : [{}]::::::::::::RulesExecutorService@ruleOfPreviousForexTurnoverProcess (PREVIOUS_FOREX_TURNOVER) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfFDConversion(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfFDConversion (FD_CONVERSION) Called::::::::::",
				requVoObjParam.getReqId());
		try {

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfFDConversion : {}", e);
		} finally {

			LOGGER.info(
					"REQID : [{}]::::::::::::RulesExecutorService@ruleOfFDConversion (FD_CONVERSION) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfLargerDeposite(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info(
				"REQID : [{}]::::::::::::RulesExecutorService@ruleOfLargerDeposite (LARGE_DEPOSIT) Called::::::::::",
				requVoObjParam.getReqId());
		try {
			String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null;
			computedFactsVOObj = new ComputedFactsVO();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();

			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					computedFactsVOObj = transactionDetailsRepositryImpl2.getLargerDeposite(requVoObjParam.getReqId(),
							accNo, custId, transMode, transType, days, fieldName, columnName, factSetObj.getRange());

					computedFactsVOObj.setFact(factName);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfLargerDeposite : {}", e);
		} finally {

			LOGGER.info(
					"REQID : [{}]::::::::::::RulesExecutorService@ruleOfLargerDeposite (LARGE_DEPOSIT) End:::::::::\n\n:",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfAvgCreditDebit(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfAvgCreditDebit (SUM) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer months = factSetObj.getMonths();
			Integer days = factSetObj.getDays();
			BigDecimal finalValue = null;

			finalValue = transactionDetailsRepositryImpl2.getAvgCreditDebit(requVoObjParam.getReqId(), accNo, custId,
					transMode, transType, days, fieldName, months, factSetObj.getRange());
			{
				computedFactsVOObj.setValue((finalValue));
				computedFactsVOObj.setFact(factName);
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfAvgCreditDebit : {}", e);
		} finally {

			LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfAvgCreditDebit (SUM) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfSumCreditDebitAmount(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfSumCreditAmount (SUM) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer months = factSetObj.getMonths();
			BigDecimal finalValue = null;

			finalValue = transactionDetailsRepositryImpl2.getSumCreditDebitAmount(requVoObjParam.getReqId(), accNo,
					custId, transMode, transType, days, fieldName, months, factSetObj.getRange(), factSetObj);
			computedFactsVOObj.setValue((finalValue));
			computedFactsVOObj.setFact(factName);

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfSumCreditAmount : {}", e);
		} finally {

			LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfSumCreditAmount (SUM) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfMinProcess(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfMinProcess (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getMinValue(requVoObjParam.getReqId(), accNo, custId,
							transMode, transType, hours, days, months, fieldName, columnName, factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfMinProcess : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfMinProcess (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfCountCreditDebitAmount(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfCountCreditDebitAmount (SUM) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer months = factSetObj.getMonths();
			String condition = factSetObj.getCondition();
			BigDecimal finalValue = new BigDecimal(0);

			if (condition != null) {
				if (condition.equals("DORMANT_REACTIVATION")) {
					accNo = requVoObjParam.getAccountNo();
					custId = null;
					AccountStatusEntity acctStatus = accountStatusRepositryImpl.getAccountStatusByAccNO(accNo,
							requVoObjParam.getReqId());
					if (acctStatus != null && acctStatus.getStatus() != null && acctStatus.getStatus().equals("06")) {
						String status = acctStatus.getStatus();
						computedFactsVOObj.setAccountStatus(status);
						computedFactsVOObj.setAcc_Re_date(acctStatus.getChangeDate());
						computedFactsVOObj.setStrValue("DORMANT_REACTIVATION");
						finalValue = transactionDetailsRepositryImpl2.getCountCreditDebit(requVoObjParam.getReqId(),
								accNo, custId, transMode, transType, days, fieldName, months, factSetObj.getRange(),
								factSetObj);

						computedFactsVOObj.setValue((finalValue));
						computedFactsVOObj.setFact(factName);

					} else {
						computedFactsVOObj.setFact(factName);
						computedFactsVOObj.setStrValue("NO_DORMANT_REACTIVATION");
						computedFactsVOObj.setValue((finalValue));
					}

				} else if (condition.equals("LOW-CASH-PROFILE")) {

					String profile = null;

					FS_FactConditionEntity conditionEntity = fS_FactConditionRepoImpl.getFactCondititon(condition,
							requVoObjParam.getReqId());
					if (conditionEntity != null && conditionEntity.getId() != null) {

						List<FS_FactConditionAttributeEntity> conditionAttribute = fS_FactConditionAttributeRepoImpl
								.getCondititonAttributes(String.valueOf(conditionEntity.getId()),
										requVoObjParam.getReqId());
						if (conditionAttribute != null && conditionAttribute.size() > 0) {
							CustomerDetailsEntity custDetails = customerDetailsRepoImpl
									.getCustomerDetailsByCustId(custId);
							if (custDetails != null) {
								for (FS_FactConditionAttributeEntity gs : conditionAttribute) {
									if (gs.getAttributes().equals(custDetails.getCustomerCategory())) {
										profile = gs.getAttributes();
									}
								}
							}

						}
					}

					if (profile != null) {
						finalValue = transactionDetailsRepositryImpl2.getCountCreditDebit(requVoObjParam.getReqId(),
								accNo, custId, transMode, transType, days, fieldName, months, factSetObj.getRange(),
								factSetObj);
						computedFactsVOObj.setValue((finalValue));
						computedFactsVOObj.setFact(factName);
						computedFactsVOObj.setStrValue(profile);
					} else {

						computedFactsVOObj.setFact(factName);
						computedFactsVOObj.setValue(finalValue);

					}

				} else if (condition.equals("NEW_ACCOUNT_CLOSED")) {
					boolean newClosedflag = false;
					LocalDate currentDateTdy = null;
					LocalDate openDate = null;
					LocalDate closeDate = null;
					String closingDate = null;
					String combinedStr = null;
					String openingDate = accountDetailsRepoImpl
							.getAccountOpeningAndClosingDateByritiria(requVoObjParam.getReqId(), accNo, custId);
					if (openingDate != null) {
						if (openingDate.contains("@")) {
							String dateStr[] = openingDate.split("@");
							DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
							openDate = LocalDate.parse(dateStr[0], formatter);
							closeDate = LocalDate.parse(dateStr[1], formatter);
							System.out.println(openDate); // Output: 2025-05-20

							long daysBetween = ChronoUnit.DAYS.between(openDate, closeDate);
							if (days != null) {

								if (daysBetween <= days) {
									newClosedflag = true;
									currentDateTdy = closeDate;
								} else {
									newClosedflag = false;
									// return new BigDecimal(0);
								}
							} else if (months != null) {
								int totaldays = months * 30;
								if (daysBetween <= totaldays) {
									// Account Closed Immediately
									newClosedflag = true;
									currentDateTdy = closeDate;
								} else {
									newClosedflag = false;
									// return new BigDecimal(0);
								}
							}

						} else {
							newClosedflag = false;
						}
					} else {
						newClosedflag = false;
					}

					if (newClosedflag) {
						finalValue = transactionDetailsRepositryImpl2.getCountCreditDebit(requVoObjParam.getReqId(),
								accNo, custId, transMode, transType, days, fieldName, months, factSetObj.getRange(),
								factSetObj);

						computedFactsVOObj.setValue((finalValue));
						computedFactsVOObj.setFact(factName);
					} else {
						computedFactsVOObj.setValue((finalValue));
						computedFactsVOObj.setFact(factName);
					}

				}

				else {

					finalValue = transactionDetailsRepositryImpl2.getCountCreditDebit(requVoObjParam.getReqId(), accNo,
							custId, transMode, transType, days, fieldName, months, factSetObj.getRange(), factSetObj);

					computedFactsVOObj.setValue((finalValue));
					computedFactsVOObj.setFact(factName);

				}
			} else {

				finalValue = transactionDetailsRepositryImpl2.getCountCreditDebit(requVoObjParam.getReqId(), accNo,
						custId, transMode, transType, days, fieldName, months, factSetObj.getRange(), factSetObj);

				computedFactsVOObj.setValue((finalValue));
				computedFactsVOObj.setFact(factName);

			}

			if (condition != null) {
				if (condition.equals("NEW_ACCOUNT")) {
					AccountDetailsEntity acctDetails = accountDetailsRepoImpl
							.getAccountDetailsByritiria(requVoObjParam.getReqId(), accNo, custId);
					if (acctDetails != null && acctDetails.getAccountOpenedDate() != null) {
						DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
						LocalDate openDate = LocalDate.parse(acctDetails.getAccountOpenedDate(), formatter);
						LocalDate currentDate = LocalDate.now();
						System.out.println(openDate); // Output: 2025-05-20

						long daysBetween = ChronoUnit.DAYS.between(openDate, currentDate);
						if (days != null && days >= daysBetween) {
							computedFactsVOObj.setAcc_open_date(acctDetails.getAccountOpenedDate());
							computedFactsVOObj.setAccountStatus("NEW");
						} else if (months != null) {
							int totalDays = months * 30;
							if (totalDays >= daysBetween) {
								computedFactsVOObj.setAcc_open_date(acctDetails.getAccountOpenedDate());
								computedFactsVOObj.setAccountStatus("NEW");
							}
						} else {
							computedFactsVOObj.setAcc_open_date(acctDetails.getAccountOpenedDate());
							computedFactsVOObj.setAccountStatus("OLD");
						}
					} else {

						computedFactsVOObj.setAccountStatus("OLD");
					}

				}

			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfCountCreditDebitAmount : {}", e);
		} finally {

			LOGGER.info(
					"REQID : [{}]::::::::::::RulesExecutorService@ruleOfCountCreditDebitAmount (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfAvgCreditDebitAmount(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfAvgCreditDebitAmount (AVG) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer months = factSetObj.getMonths();
			BigDecimal finalValue = null;

			finalValue = transactionDetailsRepositryImpl2.getAvgCreditDebit(requVoObjParam.getReqId(), accNo, custId,
					transMode, transType, days, fieldName, months, factSetObj.getRange());

			computedFactsVOObj.setValue(finalValue);
			computedFactsVOObj.setFact(factName);

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfAvgCreditDebitAmount : {}", e);
		} finally {

			LOGGER.info(
					"REQID : [{}]::::::::::::RulesExecutorService@ruleOfAvgCreditDebitAmount (AVG) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfCountCashDeposit(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			finalValue = transactionDetailsRepositryImpl2.getCountCashDepositValue(requVoObjParam.getReqId(), accNo,
					custId, transMode, transType, hours, days, months, fieldName, factSetObj.getRange());
			computedFactsVOObj.setFact(factName);
			computedFactsVOObj.setValue(finalValue);

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfCountProcess : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfSumCashDeposit(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();

			computedFactsVOObj = transactionDetailsRepositryImpl2.getSumCashDepositValue(requVoObjParam.getReqId(),
					accNo, custId, transMode, transType, hours, days, months, fieldName, factSetObj.getRange());
			computedFactsVOObj.setFact(factName);

		} catch (Exception e) {
			computedFactsVOObj = null;
			LOGGER.error("REQ ID : [{}] Exception found in RulesExecutorService@ruleOfCountProcess : {}",
					requVoObjParam.getReqId(), e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfSumNonCashDeposit(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfSumNonCashDeposit (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getSumNonCashDepositValue(requVoObjParam.getReqId(),
							accNo, custId, transMode, transType, hours, days, months, fieldName, columnName,
							factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfSumNonCashDeposit : {}", e);
		} finally {
			LOGGER.info(
					"REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfSumNonCashDeposit (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfAvgCashDeposit(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getAvgCashDepositValue(requVoObjParam.getReqId(),
							accNo, custId, transMode, transType, hours, days, months, fieldName, columnName,
							factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfCountProcess : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfCountCashWithdraw(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			Range range = factSetObj.getRange();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getCountCashWithdrawValue(requVoObjParam.getReqId(),
							accNo, custId, transMode, transType, hours, days, months, fieldName, columnName, range);
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfCountProcess : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfSumCashWithdraw(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getSumCashWithdrawValue(requVoObjParam.getReqId(),
							accNo, custId, transMode, transType, hours, days, months, fieldName, columnName,
							factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfCountProcess : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfAvgCashWithdraw(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getAugCashWithdrawValue(requVoObjParam.getReqId(),
							accNo, custId, transMode, transType, hours, days, months, fieldName, columnName,
							factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfCountProcess : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfSumNonCashWithdraw(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getSumNonCashWithdrawValue(requVoObjParam.getReqId(),
							accNo, custId, transMode, transType, hours, days, months, fieldName, columnName,
							factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfCountProcess : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfSumNonDeposit(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfSumNonDeposit (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getSumNonCashWithdrawValue(requVoObjParam.getReqId(),
							accNo, custId, transMode, transType, hours, days, months, fieldName, columnName,
							factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfSumNonDeposit : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfSumNonDeposit (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfCountAccountTransfer(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getCountAccountTransferValue(
							requVoObjParam.getReqId(), accNo, custId, transMode, transType, hours, days, months,
							fieldName, columnName, factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfCountProcess : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfSumCashTxn(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfSumCashTxn (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getSumCashTxnValue(requVoObjParam.getReqId(), accNo,
							custId, transMode, transType, hours, days, months, fieldName, columnName,
							factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfSumCashTxn : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfSumCashTxn (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfSumNonCashTxn(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfSumNonCashTxn (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getSumNonCashTxnValue(requVoObjParam.getReqId(),
							accNo, custId, transMode, transType, hours, days, months, fieldName, columnName,
							factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfSumNonCashTxn : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfSumNonCashTxn (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfSumAccountToAccountTxn(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info(
				"REQID : [{}]::::::::::::RulesExecutorService@ruleOfSumAccountToAccountTxn (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getSumAccountToAccountTxn(requVoObjParam.getReqId(),
							accNo, custId, transMode, transType, hours, days, months, fieldName, columnName,
							factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfSumCashTxn : {}", e);
		} finally {
			LOGGER.info(
					"REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfSumAccountToAccountTxn (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfSumAccountTransfer(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info(
				"REQID : [{}]::::::::::::RulesExecutorService@ruleOfSumAccountToAccountTxn (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getSumAccountTxn(requVoObjParam.getReqId(), accNo,
							custId, transMode, transType, hours, days, months, fieldName, columnName,
							factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfSumCashTxn : {}", e);
		} finally {
			LOGGER.info(
					"REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfSumAccountToAccountTxn (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfImmediateWithdraw(RuleRequestVo requVoObjParam, Factset factSetObj,
			List<ComputedFactsVO> computedFacts) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info(
				"REQID : [{}]::::::::::::RulesExecutorService@ruleOfImmediateWithdraw (IMMEDIATE_WITHDRAWAL) Called::::::::::",
				requVoObjParam.getReqId());
		try {
			String factName = null, fieldName = null;
			computedFactsVOObj = new ComputedFactsVO();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			LOGGER.info("REQID : [{}] - fieldName : [{}] - factName : [{}]", requVoObjParam.getReqId(), fieldName,
					factName);
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				LOGGER.info("REQID : [{}] - tableName : [{}] - columnName : [{}]", requVoObjParam.getReqId(), tableName,
						columnName, factSetObj.getRange());
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					if (computedFacts != null && computedFacts.size() >= 1) {
						/**
						 * Large Deposit
						 */
						Optional<ComputedFactsVO> matchedComputedFactsVOObj = computedFacts.stream()
								.filter(p -> p.getFact().equals(AMLConstants.LARGE_DEPOSIT)).findFirst();
						if (matchedComputedFactsVOObj.isPresent()) {
							LOGGER.info("Found: " + matchedComputedFactsVOObj.get());
							BigDecimal depositeLargerAmount = matchedComputedFactsVOObj.get().getValue();
							String transDate = matchedComputedFactsVOObj.get().getTransDate();
							computedFactsVOObj = rulesUtils.toGetWithdaralFacts(requVoObjParam.getReqId(),
									requVoObjParam.getAccountNo(), requVoObjParam.getCustomerId(),
									requVoObjParam.getTransactionMode(), requVoObjParam.getTxnType(),
									depositeLargerAmount, transDate, factName, factSetObj.getCondition(), factSetObj,
									matchedComputedFactsVOObj);
						} else {
							LOGGER.info(AMLConstants.LARGE_DEPOSIT + " not found.");
						}

						/**
						 * Sum of Cash Deposit
						 */
						Optional<ComputedFactsVO> matchedComputedFactsVOObjTwo = computedFacts.stream()
								.filter(p -> p.getFact().equals(AMLConstants.SUM_CASH_DEPOSITS)).findFirst();
						if (matchedComputedFactsVOObjTwo.isPresent()) {
							LOGGER.info("Found: ", matchedComputedFactsVOObjTwo.get());
							BigDecimal depositeLargerAmount = matchedComputedFactsVOObjTwo.get().getValue();
							String transDate = matchedComputedFactsVOObjTwo.get().getTransDate();
							computedFactsVOObj = rulesUtils.toGetWithdaralFacts(requVoObjParam.getReqId(),
									requVoObjParam.getAccountNo(), requVoObjParam.getCustomerId(),
									requVoObjParam.getTransactionMode(), requVoObjParam.getTxnType(),
									depositeLargerAmount, transDate, factName, factSetObj.getCondition(), factSetObj,
									matchedComputedFactsVOObjTwo);
						} else {
							LOGGER.info(AMLConstants.SUM_CASH_DEPOSITS + " not found.");
						}
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfImmediateWithdraw : {}", e);
		} finally {

			LOGGER.info(
					"REQID : [{}]::::::::::::RulesExecutorService@ruleOfImmediateWithdraw (IMMEDIATE_WITHDRAWAL) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfCountSmallCashDeposit(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;

			finalValue = transactionDetailsRepositryImpl2.getCountSmallCashDepositValue(requVoObjParam.getReqId(),
					accNo, custId, transMode, transType, hours, days, months, fieldName, factSetObj.getRange());
			computedFactsVOObj.setFact(factName);
			computedFactsVOObj.setValue(finalValue);

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfCountProcess : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfCountProcess (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfMaxNonCashTxn(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@getMaxNonCashTxnValue (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;

			finalValue = transactionDetailsRepositryImpl2.getMaxNonCashTxnValue(requVoObjParam.getReqId(), accNo,
					custId, transMode, transType, hours, days, months, fieldName, factSetObj.getRange());
			computedFactsVOObj.setFact(factName);
			computedFactsVOObj.setValue(finalValue);

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@getMaxNonCashTxnValue : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@getMaxNonCashTxnValue (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfMaxCashTxn(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfMaxCashTxn (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;

			finalValue = transactionDetailsRepositryImpl2.getMaxCashTxnValue(requVoObjParam.getReqId(), accNo, custId,
					transMode, transType, hours, days, months, fieldName, factSetObj.getRange());
			computedFactsVOObj.setFact(factName);
			computedFactsVOObj.setValue(finalValue);

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfMaxCashTxn : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfMaxCashTxn (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfSumCreditDebitClosedAccount(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info(
				"REQID : [{}]::::::::::::RulesExecutorService@ruleOfSumCreditDebitClosedAccount (SUM) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null;
		try {

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfSumCreditDebitClosedAccount : {}", e);
		} finally {

			LOGGER.info(
					"REQID : [{}]::::::::::::RulesExecutorService@ruleOfSumCreditDebitClosedAccount (SUM) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfMinBalance(RuleRequestVo requVoObjParam, Factset factSetObj) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfMinBalance (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("ACCOUNT")) {
					finalValue = transactionDetailsRepositryImpl2.getMinBalanceValue(requVoObjParam.getReqId(), accNo,
							custId, transMode, transType, hours, days, months, fieldName, columnName,
							factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfMinProcess : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfMinBalance (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

	@Override
	public ComputedFactsVO ruleOfMaxCrossBorderTxn(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfMaxCrossBorderTxn (COUNT) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null,
				txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			String txnId = requVoObjParam.getTxnId();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			BigDecimal finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("TRANSACTION")) {
					finalValue = transactionDetailsRepositryImpl2.getTxnAmount(requVoObjParam.getReqId(), accNo, custId,
							transMode, transType, hours, days, months, fieldName, columnName, factSetObj.getRange(),
							txnId);
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfMaxCashTxn : {}", e);
		} finally {
			LOGGER.info(
					"REQ ID : [{}]::::::::::::RulesExecutorService@ruleOfMaxCrossBorderTxn (COUNT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

}
