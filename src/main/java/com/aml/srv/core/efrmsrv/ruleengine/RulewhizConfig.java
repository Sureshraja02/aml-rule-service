package com.aml.srv.core.efrmsrv.ruleengine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.aml.srv.core.efrmsrv.entity.NormalizedTblEntity;
import com.aml.srv.core.efrmsrv.repo.NormalizedTblImpl;
import com.google.gson.Gson;

import jakarta.annotation.PostConstruct;

@Configuration
@EnableScheduling
public class RulewhizConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(RulewhizConfig.class);
	public List<NormalizedTblEntity> ruleEntity = new ArrayList<NormalizedTblEntity>();
	public HashMap<String, String> ruleMvel = new HashMap<String, String>();
	public HashMap<String, String> ruleJson = new HashMap<String, String>();

	@Autowired
	private NormalizedTblImpl normalizedTblImpl;

	@PostConstruct
	public void loadConfigs() {
		LOGGER.info("--------------STARTING-----------------------");
		loadRulesConfig();
		generateMVEL();
	}

	public void loadRulesConfig() {
		LOGGER.info("Loading Rules.....");

		ruleEntity = normalizedTblImpl.getActiveRules();

		LOGGER.info("[{}] - Rules are Loaded.", ruleEntity.size());
	}

	public void generateMVEL() {
		for (NormalizedTblEntity entity : ruleEntity) {
			LOGGER.info("Rule Details are ID: [{}] - Rule Name : [{}] - Payload : [{}]", entity.getId(),
					entity.getRuleName(), entity.getPayload());
			ruleJson.put(entity.getId(), entity.getPayload());
			String MVELExpression = processMVEL(entity.getPayload());
			ruleMvel.put(entity.getId(), MVELExpression);
			LOGGER.info("MVELExpression - [{}] : [{}]", entity.getId(), MVELExpression);
		}
	}

	public String processMVEL(String payload) {

		StringBuilder sb = new StringBuilder();
		AMLRule rulepayload = new Gson().fromJson(payload, AMLRule.class);
		int schemaCounter = 0;
		int funcCounter = 0;
		boolean schemaFlag = false;
		for (Schema schema : rulepayload.getSchema()) {
			schemaCounter += 1;
			if ("between".equals(schema.getCondition())) {
				schemaFlag = true;
				sb.append(" (");
				sb.append(schema.getTag());
				String[] valuelist = schema.getValue().split(",");
				sb.append(getExpression("greaterthanequals"));
				// sb.append(valuelist[0]);
				sb.append(toCheckKeyName(schema.getTag(), valuelist[0]));
				sb.append(" " + getExpression(schema.getJoinexpression()));
				sb.append(" " + schema.getTag());
				sb.append(getExpression("lesserthanequals"));
				// sb.append(valuelist[1]);
				sb.append(toCheckKeyName(schema.getTag(), valuelist[1]));
				sb.append(")");
			} else {
				schemaFlag = true;
				sb.append(" " + schema.getTag());
				sb.append(getExpression(schema.getCondition()));
				// sb.append(schema.getValue());
				sb.append(toCheckKeyName(schema.getTag(), schema.getValue()));
			}
			if (!(schemaCounter == rulepayload.getSchema().size())) {
				sb.append(" " + getExpression(schema.getJoinexpression()));
			}
		}

		if (rulepayload.getFunc().size() > 0) {
			if (schemaFlag) {
				sb.append(" " + getExpression("AND"));
			}
		}

		for (Func func : rulepayload.getFunc()) {
			funcCounter += 1;
			if ("between".equals(func.getOperator())) {
				sb.append(" (");
				sb.append(func.getTag());
				String[] valuelist = func.getValue().split(",");
				sb.append(getExpression("greaterthanequals"));
				// sb.append(valuelist[0]);
				sb.append(toCheckKeyName(func.getTag(), valuelist[0]));
				sb.append(" " + getExpression(func.getJoinexpression()));
				sb.append(" " + func.getTag());
				sb.append(getExpression("lesserthanequals"));
				// sb.append(valuelist[1]);
				sb.append(toCheckKeyName(func.getTag(), valuelist[1]));
				sb.append(")");
			} else {
				sb.append(" " + func.getTag());
				sb.append(getExpression(func.getOperator()));
				// sb.append("\""+func.getValue()+"\"");
				sb.append(toCheckKeyName(func.getTag(), func.getValue()));
			}

			if (!(funcCounter == rulepayload.getFunc().size())) {
				sb.append(" " + getExpression(func.getJoinexpression()));
			}

		}

		return sb.toString().trim();
	}

	Object toCheckKeyName(String keyName, Object valuee) {

		if (isValidNumber(valuee)) {
			return valuee;
		} else {
			return "\"" + valuee + "\"";
		}

		/*
		 * 
		 * if (StringUtils.isNotBlank(keyName) &&
		 * keyName.equalsIgnoreCase(RuleWhizConstants.ACCTYPE)) { return "\"" + valuee +
		 * "\""; } else if (StringUtils.isNotBlank(keyName) &&
		 * keyName.equalsIgnoreCase(RuleWhizConstants.DEPOSITEorWITHDRAW)) { if(valuee
		 * instanceof java.lang.String) { if(isValidNumber(valuee)) { return valuee; }
		 * else { return "\"" + valuee + "\""; }
		 * 
		 * } else if(valuee instanceof java.lang.Integer) { return valuee; } else {
		 * return valuee; } } else if (StringUtils.isNotBlank(keyName) &&
		 * keyName.equalsIgnoreCase(RuleWhizConstants.AMOUNT)) { return valuee; } else
		 * if (StringUtils.isNotBlank(keyName) &&
		 * keyName.equalsIgnoreCase(RuleWhizConstants.PANNO)) { return "\"" + valuee +
		 * "\""; } else { return valuee; }
		 */
	}

	public boolean isValidNumber(Object str) {
		return str.toString().matches("\\d+(\\.\\d+)?");
	}

	public String getExpression(String expression) {
		String retVal = "";
		switch (expression) {
		case "equals":
			retVal = MyExpression.equals.getAction();
			break;
		case "AND":
			retVal = MyExpression.AND.getAction();
			break;
		case "greater_than_equal":
			retVal = MyExpression.greaterthanequals.getAction();
			break;
		case "greater_than":
			retVal = MyExpression.greatherthan.getAction();
			break;
		case "lesser_than_equal":
			retVal = MyExpression.lesserthanequals.getAction();
			break;
		case "lesser_than":
			retVal = MyExpression.lesserthan.getAction();
			break;
		default:
			retVal = MyExpression.equals.getAction();
		}

		return retVal;
	}

}
