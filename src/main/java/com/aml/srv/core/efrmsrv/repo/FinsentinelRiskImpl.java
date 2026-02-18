package com.aml.srv.core.efrmsrv.repo;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.entity.FinsentinelRiskEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Component
public class FinsentinelRiskImpl {

	private Logger LOGGER = LoggerFactory.getLogger(FinsentinelRiskImpl.class);

	@Autowired
	EntityManager em;

	public FinsentinelRiskEntity getFinsentinelRiskByCustId(String custId) {
		LOGGER.info(":::::::::::FinsentinelRiskImpl@getFinsentinelRiskByCustId Method Called.::::::::::");
		FinsentinelRiskEntity finsentinelRiskEntityObj = null;
		CriteriaBuilder cb = null;
		CriteriaQuery<FinsentinelRiskEntity> cq = null;
		Root<FinsentinelRiskEntity> book = null;
		List<Predicate> predicates = null;
		TypedQuery<FinsentinelRiskEntity> query = null;
		try {
			cb = em.getCriteriaBuilder();
			cq = cb.createQuery(FinsentinelRiskEntity.class);
			book = cq.from(FinsentinelRiskEntity.class);
			predicates = new ArrayList<Predicate>();
			predicates.add(cb.equal(book.get("customerId"), custId));
			cq.where(predicates.toArray(new Predicate[] {}));
			query = em.createQuery(cq);
			if (query != null) {
				try {
					finsentinelRiskEntityObj = query.getSingleResult();
				} catch(NoResultException e) {
					finsentinelRiskEntityObj = null;
				}
			} else {
				finsentinelRiskEntityObj = null;
			}
			
		} catch (Exception e) {
			LOGGER.error("Exception found in FinsentinelRiskImpl@getFinsentinelRiskByCustId : {}", e);
		} finally {
			cb = null; cq = null; book = null; predicates = null; query = null;
			LOGGER.info(":::::::::::FinsentinelRiskImpl@getFinsentinelRiskByCustId Method End.::::::::::\n");
			
		}
		return finsentinelRiskEntityObj;

	}
}
