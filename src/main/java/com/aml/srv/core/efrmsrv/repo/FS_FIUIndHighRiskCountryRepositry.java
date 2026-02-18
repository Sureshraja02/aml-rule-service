package com.aml.srv.core.efrmsrv.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import com.aml.srv.core.efrmsrv.entity.FS_FIUIndHighRiskCountryEntity;

import jakarta.data.repository.Repository;

@Repository
public interface FS_FIUIndHighRiskCountryRepositry<T> extends JpaRepository<FS_FIUIndHighRiskCountryEntity, Integer> {

}
