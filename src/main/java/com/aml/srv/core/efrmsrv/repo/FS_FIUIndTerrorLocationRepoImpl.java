package com.aml.srv.core.efrmsrv.repo;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.entity.FS_FIUIndHighRiskCountryEntity;
import com.aml.srv.core.efrmsrv.entity.FS_FIUIndTerrorLocationEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Component
public class FS_FIUIndTerrorLocationRepoImpl {
	private Logger LOGGER = LoggerFactory.getLogger(FS_FIUIndTerrorLocationRepoImpl.class);

	@Autowired
	EntityManager em;
	
	public FS_FIUIndTerrorLocationEntity getCountryByritiria(String reqId, String countryCode) {

		CriteriaBuilder cb = null;
		CriteriaQuery<FS_FIUIndTerrorLocationEntity> cq = null;
		Root<FS_FIUIndTerrorLocationEntity> book = null;
		List<Predicate> predicates = null;		
		FS_FIUIndTerrorLocationEntity countryDetails=null;
		TypedQuery<FS_FIUIndTerrorLocationEntity> query = null;
		try {
			cb = em.getCriteriaBuilder();
			cq = cb.createQuery(FS_FIUIndTerrorLocationEntity.class);
			book = cq.from(FS_FIUIndTerrorLocationEntity.class);
			predicates = new ArrayList<Predicate>();
			
			
			if (StringUtils.isNotBlank(countryCode)) {
				predicates.add(cb.equal(book.get("country_Code"), countryCode));
				//predicates.add(cb.equal(book.get("riskLevel"), "High"));
			}

			
			
			
			cq.where(predicates.toArray(new Predicate[] {}));

			query = em.createQuery(cq);
			FS_FIUIndTerrorLocationEntity customerEnityObj = query.getSingleResult();
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
		
	
		return countryDetails;
		}
	
	
	
}
