package com.aml.srv.core.efrm.kyc.service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.codec.language.Soundex;
import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.aml.srv.core.efrmsrv.entity.CustomerDetailsEntity;
import com.aml.srv.core.efrmsrv.entity.PEPRecordEntity;
import com.aml.srv.core.efrmsrv.kyc.request.NameMatchRequest;
import com.aml.srv.core.efrmsrv.kyc.response.NameMatchResponse;
import com.aml.srv.core.efrmsrv.repo.CustomerDetailsRepoImpl;
import com.aml.srv.core.efrmsrv.repo.PEPRecord_Repositry;

@Service
public class NameMatchService {

	@Autowired
	JaroWinklerSimilarity similarity;
	
	@Autowired
	PEPRecord_Repositry pepRecord_Repositry;
	
	@Autowired
	CustomerDetailsRepoImpl customerDetailsRepoImpl;
	
	@Autowired
	Soundex soundex;
	
	public static final Logger Logger = LoggerFactory.getLogger(NameMatchService.class);
	
	public NameMatchResponse matchPercentage(NameMatchRequest req) {
		String custName=null;
		NameMatchResponse res=new NameMatchResponse();
		res.setFirstnamePercentage(0);
		res.setSoundMatch(false);
		res.setReqid(req.getReqid());
	
		String dob=null;
		double nameScore=0.0;
		 boolean dobMatch=false;
		if(req.getCustId()!=null){
			Logger.info(":::::::::::::::::::NameMatchService getMatch Entry:::::::::::::::::");			
			List<CustomerDetailsEntity>custList= customerDetailsRepoImpl.getCustomerDetailsbyCriteria(req.getCustId());
			if(custList!=null && custList.size()>0)	{
				Logger.info(":::::::::::::::::::NameMatchService getMatch custList [{}]:::::::::::::::::",custList.size());
				CustomerDetailsEntity custEntity=custList.get(0);
				 custName=custEntity.getCustomerName();
				 dob=custEntity.getDateOfBirth();
			}			
			List<PEPRecordEntity> pepList=pepRecord_Repositry.findAll();

			if(pepList!=null && pepList.size()>0)			{
				Logger.info(":::::::::::::::::::NameMatchService getMatch pepList [{}]:::::::::::::::::",pepList.size());
				for(PEPRecordEntity lst:pepList)
				{
					if(custName!=null && lst.getPersonName()!=null)
					{
					double score = similarity.apply(custName.toLowerCase(), lst.getPersonName().toLowerCase());
					boolean soundMatch=soundmatch(custName.toLowerCase(), lst.getPersonName().toLowerCase());
					if(lst.getBirthDate()!=null && !lst.getBirthDate().equals("") && dob!=null && !dob.equals(""))
					{
						
						Date pepDob=changeDateYYYYMMDD(lst.getBirthDate());	
						Date custDob=changeDateDDMMYYYY(dob);
						 System.out.println(pepDob.equals(custDob)); // trued1.equals(d2)
						 dobMatch=false;
						 if(pepDob.equals(custDob))
						 {
							 Logger.info(":::::::::::::::::::NameMatchService getMatch dobMatch custName [{}]:::::::::::::::::",custName);
							 dobMatch=true; 
							
						 }
					}
					
					nameScore=score * 100;
					if((nameScore>80 || soundMatch) && dobMatch){						
						 CustomerDetailsEntity custEntity=custList.get(0);
						 res.setCustId(custEntity.getCustomerName());
						 res.setDob(custEntity.getDateOfBirth());
						 res.setEmail(custEntity.getEmailId());
						 res.setPan(custEntity.getPanNo());
						 res.setFirstnamePercentage(nameScore);
						 res.setSoundMatch(soundMatch);						
						 res.setMobile(custEntity.getMobileNo());
						 res.setLastName(custEntity.getLastName());
						 Logger.info(":::::::::::::::::::NameMatchService getMatch dobMatch custName matched [{}] [{}] [{}] [{}] [{}]:::::::::::::::::",custName,custEntity.getDateOfBirth(),custEntity.getPanNo(),nameScore,custEntity.getLastName());
							
					}
					}
				}
			}
			
		
		
		}

		return res; // convert to percentage
	}
	
	public boolean soundmatch(String name1, String name2) {
		boolean score = soundex.encode(name1).equals(soundex.encode(name2));
		return score;
		
	}
	
	
	public Date changeDateYYYYMMDD(String dateStr)
	{
		Date date = null;
		try {
			// Define the format matching the string
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

			// Parse string into Date object
			 date = sdf.parse(dateStr);

			System.out.println(date); // Fri Aug 09 00:00:00 IST 1946
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}
	public Date changeDateDDMMYYYY(String dateStr)
	{
		Date date = null;
		try {
			// Define the format matching the string
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");

			// Parse string into Date object
			 date = sdf.parse(dateStr);

			System.out.println(date); // Fri Aug 09 00:00:00 IST 1946
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}
}
