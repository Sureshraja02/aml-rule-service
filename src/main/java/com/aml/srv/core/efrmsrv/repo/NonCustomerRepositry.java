package com.aml.srv.core.efrmsrv.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import com.aml.srv.core.efrmsrv.entity.NonCustomerDetailsEntity;

import jakarta.data.repository.Repository;

@Repository
public interface NonCustomerRepositry<T> extends JpaRepository<NonCustomerDetailsEntity, String>  {

}