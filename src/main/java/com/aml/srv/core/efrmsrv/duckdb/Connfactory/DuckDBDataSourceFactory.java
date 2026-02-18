package com.aml.srv.core.efrmsrv.duckdb.Connfactory;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class DuckDBDataSourceFactory {

	public static DataSource createDataSource(String dbFilePath) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.duckdb.DuckDBDriver");
        dataSource.setUrl("jdbc:duckdb:" + dbFilePath);
        return dataSource;
    }
	
	//t
	 // Switch to the correct DuckDB file
    //DataSource ds = DuckDBDataSourceFactory.createDataSource(dbFilePath);
   // JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
}
