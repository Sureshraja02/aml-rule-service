package com.aml.srv.core.efrmsrv.db.service;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.utils.CommonUtils;

@Component
public class CSVDirectImportPostgresqlService {

	public static final Logger Logger = LoggerFactory.getLogger(CSVDirectImportPostgresqlService.class);
	
	@Autowired
	CommonUtils commonUtils;
	
	@Value("${csv.destination.folder}")
	private String DESTINATION_CSV_FOLDER;
	
	@Value("${csv.insert.copy.via:JDBCCommon}")
	private String insertVia;
	
	@Value("${spring.datasource.url}")
	private String dbUrl;
	
	@Value("${spring.datasource.username}")
	private String useName;
	
	@Value("${spring.datasource.password}")
	private String secureTerm;
	
	@Value("${spring.jpa.properties.hibernate.default_schema}")
	private String schemaName;
	
	@Value("${spring.datasource.driver-class-name}")
	private String driverName;
	
	public Connection getDBConn() {
		
		DriverManagerDataSource dataSource = null;
		Connection connectionObj = null;
		try {
//			String url = "jdbc:postgresql://localhost:5432/AMLDB";
//			String user = "amluser";
//			String password = "amluser";
			dataSource = new DriverManagerDataSource();
			dataSource.setDriverClassName(driverName);
			dataSource.setSchema(schemaName);
			dataSource.setUsername(useName);
			dataSource.setPassword(secureTerm);
			dataSource.setUrl(dbUrl);
			connectionObj = dataSource.getConnection();
		} catch (Exception e) {
			connectionObj = null;
		}
		Logger.info("-------------------DB COnnection : {}", connectionObj);
		return connectionObj;
	}
	
	public void closeConn(Connection con) throws SQLException {
		if(con!=null) {
			con.close(); con=null;
		}
	}

	public void toImportCsv(String tableName, String csvFilePath) throws SQLException {
		Path csvFilePathObj = null;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String csvFileName = null;
		String currentDateNmFldr = new SimpleDateFormat("yyyyMMdd").format(new Date());
		try {
			Long startDate = new Date().getTime();
			Logger.info("[POSTGRESQL] CSV Import Start Time : [{}]", startDate);
			
			
			Logger.info("[POSTGRESQL] CSV File Path csvFilePath : {}",csvFilePath);
			csvFilePathObj = Paths.get(csvFilePath);
			
			csvFileName = csvFilePathObj.getFileName().toString();
			Logger.info("[POSTGRESQL] CSV file Name is : [{}]",csvFileName);
			
			tableName = csvFileName.replace(".csv", "");
			tableName = tableName.replaceAll(" ", "_");
			Logger.info("[POSTGRESQL] Table Name is : [{}]",tableName);
			
			tableName = commonUtils.toSpltFileNameNDGetTableName(csvFileName);
			Logger.info("[POSTGRESQL] Final - Table Name is : [{}]", tableName);
			
			conn = getDBConn();
			
			stmt = conn.createStatement();
        	// Check if table exists
           /* String checkSql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '" + tableName + "'";
            rs = stmt.executeQuery(checkSql);
            rs.next();
			boolean tableExists = rs.getInt(1) > 0;
			Logger.info("[POSTGRESQL] Table isexists : {}", tableExists);
			*/
           /* if(tableExists) {
            	String insertSql = "INSERT INTO " + tableName + " SELECT * FROM read_csv_auto('" + csvFilePathObj.toString() + "')";
                stmt.execute(insertSql);
            } else {
            	String createQuery = "CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT * FROM read_csv_auto('" + csvFilePathObj.toString() + "')";
				stmt.execute(createQuery);
            }if(rs!=null) {
				rs.close(); rs = null;
			}*/
            			
			if (StringUtils.isNotBlank(insertVia) && insertVia.equalsIgnoreCase("JDBCCommon")) {
				Logger.info("[POSTGRESQL]  JDBC Common Block Called......");
				String sql = "COPY " + tableName + " FROM '" + csvFilePathObj.toString() + "' DELIMITER ',' CSV HEADER";
				stmt.execute(sql);
				if (stmt != null) {
					stmt.close(); stmt = null;
				}
			} else if (StringUtils.isNotBlank(insertVia) && insertVia.equalsIgnoreCase("PGCopyManager")) {
				Logger.info("[POSTGRESQL]  PGCopyManager Block Called......");
				CopyManager copyManager = null;
				FileReader fileReader = null;
				try {
					copyManager = new CopyManager((BaseConnection) conn);
					fileReader = new FileReader(csvFilePathObj.toString());
					copyManager.copyIn("COPY " + tableName + " FROM STDIN WITH CSV HEADER", fileReader);
				} catch (Exception e) {
					Logger.error("Exception found in [POSTGRESQL]  PGCopyManager Block : {}", e);
				} finally {
					if (fileReader != null) {
						fileReader.close();
						fileReader = null;
					}
					if (copyManager != null) {
						copyManager = null;
					}
				}
			}
			closeConn(conn);
			
			Logger.info("::::::::::::[POSTGRESQL] CSV imported successfully.\n\n");
			
			// To move into current data folder
			//Path toPath = Paths.get(DESTINATION_CSV_FOLDER +"/"+ currentDateNmFldr+"/", csvFileName);
			Path toPath = Paths.get(DESTINATION_CSV_FOLDER +"/"+ currentDateNmFldr+"/");
			Logger.info("Before Create destination folder: {}", toPath);
			if (!Files.exists(toPath)) {
				Files.createDirectories(toPath);
				Logger.info("After Created destination folder: {}", toPath);
			}
			toPath = Paths.get(DESTINATION_CSV_FOLDER +"/"+ currentDateNmFldr+"/",csvFileName);
			Logger.info("[POSTGRESQL] Completed from file path : {}", csvFilePathObj);
			Logger.info("[POSTGRESQL] Completed to file path : {}", toPath);
			commonUtils.toMove(csvFilePathObj, toPath);
		
		} catch(Exception e) {
			closeConn(conn);
			if (stmt != null) {
				stmt.close(); stmt = null;
			}
			String faliPathFlder = "FAIL-PATH";
			Path toPath = Paths.get(DESTINATION_CSV_FOLDER + "/" + faliPathFlder + "/" + currentDateNmFldr + "/");
			Logger.info("Before Create destination folder: {}", toPath);
			if (!Files.exists(toPath)) {
				try {
					Files.createDirectories(toPath);
				} catch (IOException e1) {
				}
				Logger.info("After Created destination folder: {}", toPath);
			}
			toPath = Paths.get(DESTINATION_CSV_FOLDER + "/" + faliPathFlder + "/" + currentDateNmFldr + "/",
					csvFileName);
			Logger.info("[POSTGRESQL] Completed from file path : {}", csvFilePathObj);
			Logger.info("[POSTGRESQL] Completed to file path : {}", toPath);
			commonUtils.toMove(csvFilePathObj, toPath);
			
			
			Logger.error("Exception found in CSVDirectImportPostgresqlService@toImportCsv : {}", e);
		} finally {
			
		}
	}
}
