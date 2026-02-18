package com.aml.srv.core.efrmsrv.duckdb.Connfactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import com.zaxxer.hikari.HikariDataSource;

public class DynamicDuckDBDataSource extends AbstractRoutingDataSource {

	private static final ThreadLocal<String> CONTEXT = new ThreadLocal<>();
    private final Map<Object, Object> dataSources = new ConcurrentHashMap<>();

    public static void setCurrentDb(String dbKey) {
        CONTEXT.set(dbKey);
    }

    public static void clear() {
        CONTEXT.remove();
    }
    
    @Override
    protected Object determineCurrentLookupKey() {
        return CONTEXT.get();
    }
    
    public void addDataSource(String key, String dbFilePath) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl("jdbc:duckdb:" + dbFilePath);
        ds.setDriverClassName("org.duckdb.DuckDBDriver");
        ds.setMaximumPoolSize(5);
        dataSources.put(key, ds);
        super.setTargetDataSources(dataSources);
        super.afterPropertiesSet();
    }
    
 
}
