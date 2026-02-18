package com.aml.srv.core.efrmsrv.rule.intr;

import java.util.List;

import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.request.RuleRequestVo;
import com.aml.srv.core.efrmsrv.rule.process.response.ComputedFactsVO;

/**
 * 
 * @author : E5554365 (Prabakaran.R)
 * @Project : aml-srv
 * @year : 2025
 */
@Component
public interface RuleExecutorIntr {
	
	public ComputedFactsVO ruleOfCountProcess(RuleRequestVo requVoObjParam, Factset factSetObj);

	public ComputedFactsVO ruleOfSUMProcess(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfMaxProcess(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfAVGProcess(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfCommAggregateProcess(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfPreviousForexTurnoverProcess(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfFDConversion(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfLargerDeposite(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfImmediateWithdraw(RuleRequestVo requVoObjParam, Factset factSetObj, List<ComputedFactsVO> computedFacts);
	
	public ComputedFactsVO ruleOfAvgCreditDebit(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfSumCreditDebitAmount(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfCountCreditDebitAmount(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfAvgCreditDebitAmount(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfMinProcess(RuleRequestVo requVoObjParam, Factset factSetObj);	
	
	public ComputedFactsVO ruleOfCountCashDeposit(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfCountSmallCashDeposit(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfSumCashDeposit(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfAvgCashDeposit(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfCountCashWithdraw(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfSumCashWithdraw(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfAvgCashWithdraw(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfSumNonCashWithdraw(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfSumNonDeposit(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfCountAccountTransfer(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfSumAccountTransfer(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfSumCashTxn(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfSumNonCashTxn(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfSumAccountToAccountTxn(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfSumNonCashDeposit(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfMaxNonCashTxn(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfMaxCashTxn(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfSumCreditDebitClosedAccount(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfMinBalance(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfMaxCrossBorderTxn(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	
	
	//filed:Tablename.column
	
}
