package com.aml.srv.core.efrmsrv.filewatcher.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.utils.CommonUtils;

@Component
public class FLTtoCSVConverter {

	public static final Logger Logger = LoggerFactory.getLogger(FLTtoCSVConverter.class);

	@Value("${file.watch.ismove:false}")
	private boolean isMove;

	@Value("${file.watch.isdelete:true}")
	private boolean isDelete;

	@Autowired
	CommonUtils commonUtils;
	
	public String convertFLTToCsv(Path tlfPath, String newCsvPath, String processedPath) {
		String csvFileName = null;
		try {
			// Path csvPath = Paths.get(tlfPath.toString().replace(".flt", ".csv"));
			// Path csvPath = Paths.get(tlfPath.getParent().toString(), csvFileName);
			Path destinationDir = Paths.get(newCsvPath);
			if (!Files.exists(destinationDir)) {
				Files.createDirectories(destinationDir);
				Logger.info("Created destination folder: {}", destinationDir);
			}
			Path processeddestinationDir = Paths.get(processedPath);
			if (!Files.exists(processeddestinationDir)) {
				Files.createDirectories(processeddestinationDir);
				Logger.info("Created destination folder:{} ", processeddestinationDir);
			}
			
			if(tlfPath!=null && Files.exists(tlfPath) && Files.isRegularFile(tlfPath)) {
				csvFileName = tlfPath.getFileName().toString().replace(".flt", ".csv");
				Logger.info(" ::::::::: New File Name : {}", csvFileName);
				Path finalCsvPath = Paths.get(newCsvPath, csvFileName);
				Logger.info(" ::::::::: New File Name with path : {}", finalCsvPath.toString());
				try (BufferedReader reader = Files.newBufferedReader(tlfPath);
						BufferedWriter writer = Files.newBufferedWriter(finalCsvPath)) {
					String line = null;
					while ((line = reader.readLine()) != null) {
						String csvLine = line.replace("~", ",");
						writer.write(csvLine);
						writer.newLine();
					}
					Logger.info("Converted to CSV: {}", finalCsvPath);
				} catch (IOException e) {
					Logger.error("Exception found in FLTtoCSVConverter : {}", e);
				} finally { }
				
				String tlfFileName = tlfPath.getFileName().toString();
				Path finalprocessPath = Paths.get(processedPath.toString(), tlfFileName);
				if (isMove) {
					Logger.info("Moving file : {}", tlfFileName);
					commonUtils.toMove(tlfPath, finalprocessPath);
				}
				if (isDelete) {
					Logger.info("Deleting file : {}", tlfPath.toString());
					commonUtils.toDelete(tlfPath.toString());

				}
			}
		} catch (Exception e) {
			Logger.error("Exception found in FLTtoCSVConverter@convertFLTToCsv : {}", e);
		} finally { }
		return csvFileName;
	}
}
