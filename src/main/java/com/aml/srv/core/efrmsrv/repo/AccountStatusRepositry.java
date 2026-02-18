package com.aml.srv.core.efrmsrv.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import com.aml.srv.core.efrmsrv.entity.AccountStatusEntity;

import jakarta.data.repository.Repository;

@Repository
public interface AccountStatusRepositry<T> extends JpaRepository<AccountStatusEntity, String>{

}
