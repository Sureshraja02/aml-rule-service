package com.aml.srv.core.efrmsrv.ruleengine;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.mvel2.MVEL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

import com.aml.srv.core.efrmsrv.entity.Alerts;
import com.aml.srv.core.efrmsrv.entity.FS_FinsecTxnEntity;
import com.aml.srv.core.efrmsrv.entity.FS_FlaggedTxnEntity;
import com.aml.srv.core.efrmsrv.entity.FinsentinelRiskEntity;
import com.aml.srv.core.efrmsrv.entity.NormalizedTblEntity;
import com.aml.srv.core.efrmsrv.entity.TransactionDetailsEntity;
import com.aml.srv.core.efrmsrv.repo.AlertsRepo;
import com.aml.srv.core.efrmsrv.repo.FS_FinsecTxnRepositry;
import com.aml.srv.core.efrmsrv.repo.FS_FlaggedTxnRepositry;
import com.aml.srv.core.efrmsrv.repo.FinsentinelRiskImpl;
import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.request.Range;
import com.aml.srv.core.efrmsrv.rule.process.request.RuleRequestVo;
import com.aml.srv.core.efrmsrv.rule.process.response.RuleResposeDetailsVO;
import com.aml.srv.core.efrmsrv.rule.service.RulesIdentifierService;
import com.aml.srv.core.efrmsrv.utils.CommonUtils;
import com.aml.srv.core.efrmsrv.utils.RuleWhizConstants;
import com.google.gson.Gson;

@EnableAsync(proxyTargetClass=true)
@EnableCaching
@Service
public class ProcessEventsService {
	

	private Logger LOGGER = LoggerFactory.getLogger(ProcessEventsService.class);
	
	@Autowired
	private CommonUtils commonUtils;

	/*
	 * @Autowired private ApiService apiService;
	 */
	
	@Autowired
	private AlertsRepo alertsRepo;
	
	@Autowired
	private RulewhizConfig appConfig;
	
	@Autowired
	RulesIdentifierService rulesIdentifierService;
	
	@Autowired
	FS_FlaggedTxnRepositry<?> fsFlaggedTxnRepositry;
	
	@Autowired
	FS_FinsecTxnRepositry<?> fs_FinsecTxnRepositry;
	
	@Autowired
	FinsentinelRiskImpl finsentinelRiskImpl;
		
	@Value("${aml.without.api.testing:false}")
	private boolean withOutAPITesting;
	
	
	@Value("${aml.cust.prof.avg.rsik.score:80}")
	private Integer custRiskScoreAvg;
	
	String clazzName = RuleExecutorService.class.getSimpleName();
	
	/**
	 * 
	 * @param transactionEntity
	 * @param groupId
	 * @param ruleEntity
	 */
	@Async("RuleEngineExecutor")
	public void processEvent(TransactionDetailsEntity transactionEntity, String groupId, NormalizedTblEntity ruleEntity) {
		Long threadId = null;
		Long startTime = new Date().getTime();
		threadId = Thread.currentThread().getId();
		LOGGER.info("Thread Id : [{}] - {}@processEvent Async method Called...........",threadId, clazzName);
		
		RuleRequestVo bean = null;
		String methodName = null;
		//AMLAPIBean bean = null;
		String payload = null;
		AMLRule ruleDetails = null;
		List<Func> funcList = null;
		List<Factset> factSetList = null;
		String responseJson = null;
		ConcurrentHashMap<String, Object> mvelConcurntMap  =  null;
		try {
			
			String threadName = Thread.currentThread().getName();
			LOGGER.info("Running in thread : {}  (ID: {})", threadName, threadId);
			methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
		
			LOGGER.info("Thread Id : [{}] - transactionEntity Time : [{}] - RuleId : [{}]- Account No.: [{}]",
					threadId, commonUtils.getCurrentDateTimeMSec(startTime), ruleEntity.getId(),
					transactionEntity.getAccountNo());
			// LOGGER.info("Request Details : [{}] ", new Gson().toJson(ruleEntity, NormalizedRuleEntity.class));
			//bean = new AMLAPIBean();
			bean = new RuleRequestVo();
			payload = ruleEntity.getPayload();
			try {
				LOGGER.info("Thread Id : [{}] - Payload JSON : [{}]", threadId, ruleEntity.getPayload());
				ruleDetails = new Gson().fromJson(payload, AMLRule.class);
			} catch (Exception e) {
				LOGGER.error("Thread Id : [{}] - [PAYLOAD] Exception found PAYLOAD Error : {}", threadId,e);
			}
			funcList = ruleDetails.getFunc();
			factSetList = new ArrayList<Factset>();

			 mvelConcurntMap  = commonUtils.toConvertJson2Map(ruleDetails,threadId);
			 LOGGER.info("Thread Id : [{}] - mvelConcurntMap Details : [{}]", threadId, mvelConcurntMap);
			//LOGGER.info("Row ID : [{}]", ruleEntity.getId());
			
			for (Func func : funcList) {
				Factset factSetObj = getFactSet(func, ruleEntity,threadId);
				factSetList.add(factSetObj);
			}
			bean.setAccountNo(String.valueOf(transactionEntity.getAccountNo()));
			bean.setReqId(UUID.randomUUID().toString());
			bean.setCustomerId(String.valueOf(transactionEntity.getCustomerId()));
			bean.setRuleId(String.valueOf(ruleEntity.getRuleName()));
			bean.setTxn_time(transactionEntity.getTransactionTime());
			bean.setTxnType(transactionEntity.getTransactionType());
			bean.setTransactionMode(ruleEntity.getTransactionMode());
			bean.setFactSet(factSetList);
		//	String jsonReq = new Gson().toJson(bean);
			//LOGGER.info("Thread Id : [{}] - API Request [{}] - URL : [{}] - Row ID : [{}] - RULE NAME : [{}]",threadId, jsonReq, apiUrl, ruleEntity.getId(), ruleEntity.getRuleName());
			if(withOutAPITesting) {
				return;
			} 
			// responseBean
			/*okHttpResp = apiService.toSendRequest(apiUrl, jsonReq);
			if (okHttpResp!=null && okHttpResp.isSuccessful()) {
				responseJson = okHttpResp.getRespMsg();
			} else {
				responseJson = apiService.makePostRequest(apiUrl, jsonReq);
			} */
			LOGGER.info("Thread Id : [{}] - Actual Response from AML Service  : [{}]", threadId, responseJson);
			RuleResposeDetailsVO ruleRespDtlVOObj = null;
		/*	if (StringUtils.isNotBlank(responseJson)) {
				ruleRespDtlVOObj = new Gson().fromJson(responseJson, RuleResposeDetailsVO.class);
			} */
			ruleRespDtlVOObj = rulesIdentifierService.toComputeAMLData(bean);
			if (ruleRespDtlVOObj != null) {
				LOGGER.info("Thread Id : [{}] - Response from AML Service [IF]: [{}]", threadId,ruleRespDtlVOObj);
				if (appConfig.ruleMvel.containsKey(ruleEntity.getId())) {
					String MVELExpression = appConfig.ruleMvel.get(ruleEntity.getId());
					LOGGER.info("Thread Id : [{}] - MVEL Expression : [{}]", threadId, MVELExpression);
					if(mvelConcurntMap!=null) {
						mvelConcurntMap = commonUtils.toUpdateConcurtMap(mvelConcurntMap,ruleRespDtlVOObj, threadId);
						
						// ruleService.executeRules(decisionEngine, entity, "AML", "AML", "ADMIN", UUID.randomUUID().toString());
						// response beancomputedFacts need to pass in entity
						boolean match = MVEL.evalToBoolean(MVELExpression, mvelConcurntMap);
						LOGGER.info("Thread Id : [{}] - MVEL Return Status : [{}]",threadId,match);
						if (match) {
							LOGGER.info("Thread Id : [{}] - MVEL Expression Match Status [IF] Block : [{}]", threadId,match);
							Alerts alert = new Alerts();
							alert.setAccNo(transactionEntity.getAccountNo().toString());
							alert.setAlertDesc(ruleEntity.getRuleDescription());
							alert.setAlertId(UUID.randomUUID().toString());
							alert.setAlertName(ruleEntity.getRuleName());
							alert.setAlertParentId(transactionEntity.getCustomerId().toString()+transactionEntity.getTransactionId());
							alert.setAlertStatus(RuleWhizConstants.ALERT_STATUS_PENDING);
							alert.setCustId(transactionEntity.getCustomerId().toString());
							alert.setRiskCategory(ruleEntity.getAlertCategory());
							alert.setRuleId(ruleEntity.getId());
							alert.setTransactionId(transactionEntity.getTransactionId());
							alert.setAlertDT(new Timestamp(new Date().getTime()));
							alert.setModifiedDt(new Timestamp(new Date().getTime()));
							alertsRepo.save(alert);
							LOGGER.info("Thread Id : [{}] - Alert Inserted SUccessfully...........",threadId);
							
							// To Insert Flagged Txn
							FS_FlaggedTxnEntity fs_FlaggedTxnEntityObj = toInsertFlaggedTxn(ruleEntity, transactionEntity, threadId);
							
							LOGGER.info("Thread Id : [{}] - FS_FlaggedTxnEntity Inserted Successfully...........",threadId);
							Optional<FS_FlaggedTxnEntity> optionalValue = Optional.ofNullable(fs_FlaggedTxnEntityObj);
	
							if(!optionalValue.isEmpty() && optionalValue.isPresent()) {
								// To Get Cust Profiling Score
								Integer riskScore = getCustProfilingScore(String.valueOf(transactionEntity.getCustomerId()), threadId);
								LOGGER.info("Thread Id : [{}] - Cust Profiling Risk Score [{}]...........",threadId, riskScore);
								if (riskScore >= custRiskScoreAvg) { // Insert finsec_txn
									toInsertFinsecTxn(fs_FlaggedTxnEntityObj, riskScore, threadId);
									LOGGER.info("Thread Id : [{}] - Cust Profiling Risk Score [{}] Finsec Txn Insert Successfully ...........",threadId, riskScore);
								} else { // NO insert Required.
									LOGGER.info("Thread Id : [{}] - Cust Profiling Risk Score [{}] No Need to insert Finsec Txn ...........",threadId, riskScore);
								}
							}
							} else {
								LOGGER.info("Thread Id : [{}] - MVEL Expression Match Status [ELSE] BLock : [{}]", threadId,match);
							}
						} else {
							LOGGER.info("Thread Id : [{}] - MVEL Expression Not found and Null.", threadId);
						}
					}
				} else {
					LOGGER.info("Thread Id : [{}] - Response from AML Service [ELSE]: [{}]", threadId,
							ruleRespDtlVOObj);
				}
			} catch (Exception e) {
				LOGGER.error("Thread Id : [{}] - Exception found in {}@{} : {}", threadId, clazzName, methodName, e);
			} finally {
				Long endTime = new Date().getTime();
				LOGGER.info("Thread Id : [{}] - {}@processEvent Async End Time : [{}]\n\n", threadId,clazzName, commonUtils.findIsHourMinSec((endTime-startTime)));
		        try {
		            Thread.sleep(6000);  } catch (InterruptedException e) {  Thread.currentThread().interrupt(); }
		       bean = null; payload = null; ruleDetails = null; funcList = null; factSetList = null;responseJson = null; 
			}
	}
	

	private FS_FlaggedTxnEntity toInsertFlaggedTxn(NormalizedTblEntity ruleEntity, TransactionDetailsEntity transactionEntity, Long threadId) {
		LOGGER.info("Thread Id : [{}] - {}@toInsertFlaggedTxn method Called...........",threadId, clazzName);
		FS_FlaggedTxnEntity fs_FlaggedTxnEntity = null;
		FS_FlaggedTxnEntity fs_FlaggedTxnEntityRtnObj = null;
		try {
			fs_FlaggedTxnEntity = new FS_FlaggedTxnEntity();
			BeanUtils.copyProperties(transactionEntity, fs_FlaggedTxnEntity);
			fs_FlaggedTxnEntity.setCategoryName(ruleEntity.getAlertCategory());
			fs_FlaggedTxnEntity.setRuleId(ruleEntity.getId());
			fs_FlaggedTxnEntity.setRuleName(ruleEntity.getRuleName());
			fs_FlaggedTxnEntity.setCreatedDate(new Timestamp(new Date().getTime()));
			LOGGER.info("fs_FlaggedTxnEntity : {}",fs_FlaggedTxnEntity.toString());
			fs_FlaggedTxnEntityRtnObj = fsFlaggedTxnRepositry.save(fs_FlaggedTxnEntity);
		} catch (Exception e) {
			LOGGER.error("Thread Id : [{}] - Exception found in {}@toInsertFlaggedTxn : {}", threadId, clazzName, e);
		} finally { 
			LOGGER.info("Thread Id : [{}] - {}@toInsertFlaggedTxn method End...........\n",threadId, clazzName);
		}
		return fs_FlaggedTxnEntityRtnObj;
	}
	
	private Integer getCustProfilingScore(String custIdParam, Long threadId) {
		LOGGER.info("Thread Id : [{}] - {}@getCustProfilingScore method Called...........",threadId, clazzName);
		FinsentinelRiskEntity finsentinelRiskEntityObj = null;
		String riskScore = null;
		BigDecimal bigDecriskScoreInt = null;
		Integer riskScoreInt = 0;
		try {
			finsentinelRiskEntityObj = finsentinelRiskImpl.getFinsentinelRiskByCustId(custIdParam);
			riskScore = Optional.ofNullable(finsentinelRiskEntityObj).map(FinsentinelRiskEntity :: getRiskScore).orElse("0");
			if (StringUtils.isNotBlank(riskScore)) {
				//riskScoreInt = Integer.parseInt(riskScore);
				bigDecriskScoreInt = new BigDecimal(riskScore);
				bigDecriskScoreInt = bigDecriskScoreInt.multiply(BigDecimal.valueOf(100));
				//riskScoreInt = (riskScoreInt * 100);
			} else {
				bigDecriskScoreInt = new BigDecimal("0");
			}
			riskScoreInt = bigDecriskScoreInt.setScale(0, RoundingMode.UP).intValue();
		} catch (Exception e) {
			LOGGER.error("Thread Id : [{}] - Exception found in {}@toInsertFlaggedTxn : {}", threadId, clazzName, e);
		} finally {
			LOGGER.info("Thread Id : [{}] - {}@getCustProfilingScore method End...........\n",threadId, clazzName);
			
		}
		return riskScoreInt;
	}
	
	private void toInsertFinsecTxn(FS_FlaggedTxnEntity fs_FlaggedTxnEntityObj, Integer custProRiskScore, Long theardId) {
		LOGGER.info("Thread Id : [{}] - {}@toInsertFinsecTxn method Called...........",theardId, clazzName);
		
		FS_FinsecTxnEntity fs_FinsecTxnEntity = null;
		try {
			fs_FinsecTxnEntity = new FS_FinsecTxnEntity();
			BeanUtils.copyProperties(fs_FlaggedTxnEntityObj, fs_FinsecTxnEntity);
			fs_FinsecTxnEntity.setCreatedDate(new Timestamp(new Date().getTime()));
			fs_FinsecTxnEntity.setCustRiskScore(custProRiskScore);
			fs_FinsecTxnRepositry.save(fs_FinsecTxnEntity);
		} catch (Exception e) {
			LOGGER.error("Exception found in ProcessEventsService@toInsertFlaggedTxn : {}", e);
		} finally {
			LOGGER.info("Thread Id : [{}] - {}@toInsertFinsecTxn method End...........\n",theardId, clazzName);
			
		}
	}

	public Factset getFactSet(Func func, NormalizedTblEntity ruleEntity, Long threadId) {
		
		Factset factset = new Factset();
		String methodName = null;
		try {
			LOGGER.info("Thread Id : [{}] - RuleName : [{}] - Row ID : [{}] - RULE VALIDATION - getFactSet Method Called......",threadId, ruleEntity.getRuleName(),ruleEntity.getId());	
			methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
			if (RuleWhizConstants.day.equalsIgnoreCase(ruleEntity.getOffsetUnit())) {
				factset.setDays(ruleEntity.getOffsetValue());
			} else if (RuleWhizConstants.month.equalsIgnoreCase(ruleEntity.getOffsetUnit())) {
				factset.setMonths(ruleEntity.getOffsetValue());
			} else if (RuleWhizConstants.hour.equalsIgnoreCase(ruleEntity.getOffsetUnit())) {
				factset.setHours(ruleEntity.getOffsetValue());
			} else {
				factset.setDays(1);
			}
			// LOGGER.info("Step 1.2 [{}]", func.getCondition());
			if (!StringUtils.isEmpty(func.getCondition())) {
				factset.setCondition(func.getCondition());
			}
			Optional.ofNullable(func.getRange()).ifPresent(m -> {
				if (!StringUtils.isEmpty(func.getRange())) {
					LOGGER.info("Step 1.3.1 [{}]", func.getRange());
					String list[] = func.getRange().split(",");
					Range range = new Range();
					range.setMin(list[0]);
					range.setMax(list[1]);
					factset.setRange(range);
				}
			});
			LOGGER.info("Step 1.4");
			factset.setFact(func.getFact());
			factset.setField(func.getTag());
			LOGGER.info("Thread Id : [{}] - RuleName : [{}] - Row ID : [{}] - RULE VALIDATION - getFactSet Method End......\n",threadId, ruleEntity.getRuleName(),ruleEntity.getId());	
			
		} catch (Exception e) {
			LOGGER.error("Thread Id : [{}] - Exception found in {}@{} : {}",threadId, clazzName, methodName, e);
		} finally {}
		return factset;
		
	}
}
