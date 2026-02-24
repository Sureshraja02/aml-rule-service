package com.aml.srv.core.efrmsrv.repo;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.aml.srv.core.efrmsrv.entity.FS_FinsecTxnEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Repository
public class FS_FinsecTxnImpl {

	private Logger LOGGER = LoggerFactory.getLogger(FS_FinsecTxnImpl.class);

	@Autowired
	EntityManager em;

	@Transactional(isolation = Isolation.SERIALIZABLE)
	public FS_FinsecTxnEntity getFinsecTxn(String transIdParam) {
		LOGGER.info("FS_FinsecTxnImpl@getFinsecTxn Called....");
		FS_FinsecTxnEntity fsFinsecTxnEntityObj = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<FS_FinsecTxnEntity> cq = null;
		Root<FS_FinsecTxnEntity> rootBk = null;
		TypedQuery<FS_FinsecTxnEntity> query = null;
		try {

			cb = em.getCriteriaBuilder();
			cq = cb.createQuery(FS_FinsecTxnEntity.class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(FS_FinsecTxnEntity.class);
			predicates.add(cb.equal(rootBk.get("transactionId"), transIdParam));//transactionId
			if (predicates != null && predicates.size() > 0) {
				cq.where(predicates.toArray(new Predicate[] {}));
				query = em.createQuery(cq);

				if (query != null) {
					try {
						fsFinsecTxnEntityObj = query.getSingleResult();
					} catch (NoResultException e) {
						fsFinsecTxnEntityObj = null;
					}
				} else {
					fsFinsecTxnEntityObj = null;
				}

			} else {
				fsFinsecTxnEntityObj = null;
			}

		} catch (Exception e) {
			fsFinsecTxnEntityObj = null;
			LOGGER.error("Exception found in FS_FinsecTxnImpl@getFinsecTxn : {}", e);
		} finally {
			cb = null; predicates = null; cq = null; rootBk = null; query = null;
		}
		return fsFinsecTxnEntityObj;
	}
}
