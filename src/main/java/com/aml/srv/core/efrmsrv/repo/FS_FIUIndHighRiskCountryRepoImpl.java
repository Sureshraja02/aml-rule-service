package com.aml.srv.core.efrmsrv.repo;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.entity.AccountDetailsEntity;
import com.aml.srv.core.efrmsrv.entity.FS_FIUIndHighRiskCountryEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Component
public class FS_FIUIndHighRiskCountryRepoImpl {

	private Logger LOGGER = LoggerFactory.getLogger(FS_FIUIndHighRiskCountryRepoImpl.class);

	@Autowired
	EntityManager em;
	
	public FS_FIUIndHighRiskCountryEntity getCountryByritiria(String reqId, String countryCode) {

		CriteriaBuilder cb = null;
		CriteriaQuery<FS_FIUIndHighRiskCountryEntity> cq = null;
		Root<FS_FIUIndHighRiskCountryEntity> book = null;
		List<Predicate> predicates = null;		
		FS_FIUIndHighRiskCountryEntity countryDetails=null;
		TypedQuery<FS_FIUIndHighRiskCountryEntity> query = null;
		try {
			cb = em.getCriteriaBuilder();
			cq = cb.createQuery(FS_FIUIndHighRiskCountryEntity.class);
			book = cq.from(FS_FIUIndHighRiskCountryEntity.class);
			predicates = new ArrayList<Predicate>();
			
			
			if (StringUtils.isNotBlank(countryCode)) {
				predicates.add(cb.equal(book.get("country_Code"), countryCode));
			}
			
			
			cq.where(predicates.toArray(new Predicate[] {}));

			query = em.createQuery(cq);
			FS_FIUIndHighRiskCountryEntity customerEnityObj = query.getSingleResult();
			if (customerEnityObj != null)
			{
				countryDetails=customerEnityObj;
				
				
				LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, countryDetails);
			} else {
				countryDetails = null;
				LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, countryDetails);
			}
		} catch (Exception e) {
			return null;
		} finally {

		}
		
	
		return countryDetails;}
	
	
	
	
}
