package com.aml.srv.core.efrmsrv.repo;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.entity.FS_FactConditionEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Component
public class FS_FactConditionRepoImpl {

	private Logger LOGGER = LoggerFactory.getLogger(FS_FactConditionRepoImpl.class);
	
	@Autowired
	EntityManager em;
	
	public FS_FactConditionEntity getFactCondititon(String factName, String reqId) {
		LOGGER.info("REQ ID : [{}] - FS_FactConditionRepoImpl@getFactCondititon method called.....", reqId);
		FS_FactConditionEntity factCondititonObj = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<FS_FactConditionEntity> cq = null;
		Root<FS_FactConditionEntity> rootBk = null;
		TypedQuery<FS_FactConditionEntity> query = null;
		try {
			cb = em.getCriteriaBuilder();
			cq = cb.createQuery(FS_FactConditionEntity.class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(FS_FactConditionEntity.class);
			predicates.add(cb.equal(rootBk.get("name"), factName));
			if (predicates != null && predicates.size() > 0) {
				cq.where(predicates.toArray(new Predicate[] {}));
				query = em.createQuery(cq);
				factCondititonObj = query.getSingleResult();
			} else {
				factCondititonObj = null;
			}
		} catch (Exception e) {
			factCondititonObj = null;
			LOGGER.error("REQ ID : [{}] - Exception found in FS_FactConditionEntity@getFactCondititon : {}", reqId, e);
		} finally {
			cb = null; predicates = null; cq = null; rootBk = null;query = null;
			LOGGER.info("REQ ID : [{}] - FS_FactConditionEntity@getFactCondititon method end.....\n\n", reqId);
		}
		return factCondititonObj;
	}
}
