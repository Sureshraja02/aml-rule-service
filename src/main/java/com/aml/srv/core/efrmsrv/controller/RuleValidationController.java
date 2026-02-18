package com.aml.srv.core.efrmsrv.controller;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.aml.srv.core.efrmsrv.cust.profiling.service.CustomerProfiling;
import com.aml.srv.core.efrmsrv.db.service.CSVDirectImportPostgresqlService;
import com.aml.srv.core.efrmsrv.duckdb.service.CSVDirectImportService;
import com.aml.srv.core.efrmsrv.filewatcher.service.AMLDataTblDetailsFetch;
import com.aml.srv.core.efrmsrv.filewatcher.service.FLTtoCSVConverter;
import com.aml.srv.core.efrmsrv.kafka.PublishData2Kafka;
import com.aml.srv.core.efrmsrv.kafka.repo.FinSecIndicatorVO;
import com.aml.srv.core.efrmsrv.rule.process.request.RuleRequestVo;
import com.aml.srv.core.efrmsrv.rule.process.response.RuleResposeDetailsVO;
import com.aml.srv.core.efrmsrv.rule.service.RulesIdentifierService;
import com.aml.srv.core.efrmsrv.utils.AMLConstants;
import com.aml.srv.core.efrmsrv.utils.CommonUtils;
import com.google.gson.Gson;

@RestController
@RequestMapping({ "/api/v1/" })
public class RuleValidationController {

	private Logger LOGGER = LoggerFactory.getLogger(RuleValidationController.class);

	@Autowired
	RulesIdentifierService rulesIdentifierService;
	
	
	@Autowired
	FLTtoCSVConverter converter;

	@Autowired
	CSVDirectImportService cvsDirectImportService;
	
	@Autowired
	AMLDataTblDetailsFetch amlDataTblDetailsFetch;
	
	@Autowired
	CustomerProfiling customerProfiling;
	
	@Autowired
	PublishData2Kafka publishData2Kafka;
	
	@Autowired
	CSVDirectImportPostgresqlService csvDirectImportPostgresqlService;
	
	@Autowired
	CommonUtils commonUtils;
	
	
	

	@RequestMapping(value = { "fact/service" }, method = { RequestMethod.POST })
	public ResponseEntity<?> ruleProcessMethod(@RequestBody RuleRequestVo requestObjParam) {

		ResponseEntity<Object> retunRespEntity = null;
		RuleResposeDetailsVO ruleResponseVoObj = null;
		try {

			ruleResponseVoObj = rulesIdentifierService.toComputeAMLData(requestObjParam);
			if (ruleResponseVoObj != null) {
				retunRespEntity = getResponseEntity(new Gson().toJson(ruleResponseVoObj), HttpStatus.OK);
			} else {
				retunRespEntity = getResponseEntity("No Response Found", HttpStatus.OK);
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RuleValidationController@ruleProcessMethod : {}", e);
			retunRespEntity = getResponseEntity("Exception, Will check with Admin", HttpStatus.BAD_REQUEST);
		} finally {

		}
		return retunRespEntity;

	}
	
	@RequestMapping(value = { "post/fileupload" }, method = { RequestMethod.POST })
	public ResponseEntity<?> postFileUpload() {

		Long startDateMain = new Date().getTime();
		ResponseEntity<Object> retunRespEntity = null;
		RuleResposeDetailsVO ruleResponseVoObj = null;
		try {

			FinSecIndicatorVO finSecIndicatorVOoBj = new FinSecIndicatorVO();
			finSecIndicatorVOoBj = amlDataTblDetailsFetch
					.toSetFinSecIndicatorObjectForDuckDBSts(finSecIndicatorVOoBj);
			finSecIndicatorVOoBj = amlDataTblDetailsFetch
					.toGetRowCountEachAMLTblSetINFincSecIndicator(finSecIndicatorVOoBj);
			finSecIndicatorVOoBj = customerProfiling
					.addCustomerProfilingStsFinSecIndictor(finSecIndicatorVOoBj);
			publishData2Kafka.sendtoKafka(finSecIndicatorVOoBj.getUuid(), finSecIndicatorVOoBj,
					AMLConstants.KAFKA_PUB_TOPIC);
			
			Long endTime = new Date().getTime();
			LOGGER.info("Total file processed time : {}", commonUtils.findIsHourMinSec((endTime - startDateMain)));
			
		
		} catch (Exception e) {
			LOGGER.error("Exception found in RuleValidationController@ruleProcessMethod : {}", e);
			retunRespEntity = getResponseEntity("Exception, Will check with Admin", HttpStatus.BAD_REQUEST);
		} finally {

		}
		return retunRespEntity;

	}

	public ResponseEntity<Object> getResponseEntity(String respMsg, HttpStatus httpStatus) {
		return new ResponseEntity<Object>(respMsg, httpStatus);
	}
}