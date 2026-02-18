package com.aml.srv.core.efrmsrv.filewatcher.service;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.cust.profiling.service.CustomerProfiling;
import com.aml.srv.core.efrmsrv.db.service.CSVDirectImportPostgresqlService;
import com.aml.srv.core.efrmsrv.duckdb.service.CSVDirectImportService;
import com.aml.srv.core.efrmsrv.kafka.PublishData2Kafka;
import com.aml.srv.core.efrmsrv.kafka.repo.FinSecIndicatorVO;
import com.aml.srv.core.efrmsrv.utils.AMLConstants;
import com.aml.srv.core.efrmsrv.utils.CommonUtils;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class FileWatcher {

	private static final Logger Logger = LoggerFactory.getLogger(FileWatcher.class);

	@Autowired
	CommonUtils commonUtils;
	
	@Value("${file.directory.to.watch}")
	private String DIRECTORY_TO_WATCH;
	
	@Value("${csv.destination.folder}")
	private String DESTINATION_CSV_FOLDER;
	
	@Value("${processed.file.path}")
	private String DESTINATION_PROCESSED_FOLDER;
	
	@Value("${from.file.format}")
	private String FROM_FILE_FRMT;
	
	@Value("${to.file.format}")
	private String TO_FILE_FRMT;
	
	
	@Value("${daily.cbs.file.count}")
	private Integer CBSAmlFileC0unt;
	
	@Value("${file.fetch.interval:6000}")
	private Integer fileFetchInterval;
	
	@Value("${file.completion.check.interval:1000}")
	private Integer fileCompletionCheckval;

	@Autowired
	FLTtoCSVConverter converter;

	@Autowired
	CSVDirectImportService cvsDirectImportService;
	
	@Autowired
	AMLDataTblDetailsFetch amlDataTblDetailsFetch;
	
	@Autowired
	CustomerProfiling customerProfiling;
	
	@Autowired
	PublishData2Kafka publishData2Kafka;
	
	@Autowired
	CSVDirectImportPostgresqlService csvDirectImportPostgresqlService;

	Long startDateMain = new Date().getTime();
	/**
	 * 
	 * 
	 * watchDirectory
	 * void
	 */
	@PostConstruct
	public void watchDirectory() {
		Thread thread = new Thread(() -> {
			boolean completedFileCountSts=false;
			try {
				WatchService watchService = FileSystems.getDefault().newWatchService();
				Path path = Paths.get(DIRECTORY_TO_WATCH);
				path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);//, StandardWatchEventKinds.ENTRY_MODIFY);// StandardWatchEventKinds.ENT
				Logger.info("=============Processing started==================");
				Logger.info("Watching directory: {}", DIRECTORY_TO_WATCH);
				while (true) {
					WatchKey key = watchService.take();
					List<WatchEvent<?>> watcheventss = key.pollEvents();
					Integer fileCount = watcheventss.size();
					//Logger.info("fileCount : {}", fileCount);
					for (WatchEvent<?> event : watcheventss) {
						WatchEvent.Kind<?> kind = event.kind();
						Path fileName = (Path) event.context();
						Logger.info("kind : {}", kind);
						if (!StandardWatchEventKinds.ENTRY_DELETE.equals(kind)
								&& !StandardWatchEventKinds.ENTRY_MODIFY.equals(kind)) {
							Logger.info("FROM_FILE_FRMT : [{}]", FROM_FILE_FRMT);
							if (fileName!=null && fileName.toString().endsWith(FROM_FILE_FRMT)) {
								Logger.info("File format {} Block",FROM_FILE_FRMT);
								Path fullPath = path.resolve(fileName);
								// Waiting for file write completion
								waitForFileCompletion(fullPath);
								
								String csvNewFilename = converter.convertFLTToCsv(fullPath, DESTINATION_CSV_FOLDER,DESTINATION_PROCESSED_FOLDER);
								try {
									if(StringUtils.isNotBlank(csvNewFilename)) {
									Path csvFilePath = Paths.get(DESTINATION_CSV_FOLDER, csvNewFilename);
										if (csvFilePath != null && Files.exists(csvFilePath)) {
											Logger.info("Final CSV Path and Name after Convert : {}", csvFilePath.toString());
											Long startDate = new Date().getTime();
											Logger.info("FileWatcher CSV Import Start Time : [{}]", startDate);
											
											csvDirectImportPostgresqlService.toImportCsv(csvFilePath.getFileName().toString(), csvFilePath.toString());
											
											//cvsDirectImportService.importCsv(csvFilePath.toString());
											Long endTime = new Date().getTime();
											Logger.info("FileWatcher CSV Import - Total time : {}", commonUtils.findIsHourMinSec((endTime - startDate)));
										}
									}
								} catch (Exception e) {
									Logger.error("Exception found in watchDirectory : {}", e);
								}
								
							} else if (fileName!=null && fileName.toString().endsWith(AMLConstants.CSV_FORMAT)) {
								Logger.info("File format {} block entred.",AMLConstants.CSV_FORMAT);
								// Move file
								Path fullPath = path.resolve(fileName);
								// Waiting for file write completion
								waitForFileCompletion(fullPath);
								
								String toCsvFileName = fileName.getFileName().toString();
								Path csvFilePath = Paths.get(DESTINATION_CSV_FOLDER, toCsvFileName);
								commonUtils.toMove(fullPath, csvFilePath);
								Logger.info("File moved successfully");
								
								try {
									if(StringUtils.isNotBlank(toCsvFileName)) {
									Path csvNewFilePath = Paths.get(DESTINATION_CSV_FOLDER, toCsvFileName);
										if (csvNewFilePath != null && Files.exists(csvNewFilePath)) {
											Logger.info("[CSV] Final CSV Path and Name after Convert : {}", csvNewFilePath.toString());
											Long startDate = new Date().getTime();
											Logger.info("[CSV] FileWatcher CSV Import Start Time : [{}]", startDate);
											csvDirectImportPostgresqlService.toImportCsv(csvFilePath.getFileName().toString(), csvFilePath.toString());
											//cvsDirectImportService.importCsv(csvNewFilePath.toString());
											Long endTime = new Date().getTime();
											Logger.info("[CSV] FileWatcher CSV Import - Total time : {}", commonUtils.findIsHourMinSec((endTime - startDate)));
										}
									}
								} catch (Exception e) {
									Logger.error("Exception found in watchDirectory : {}", e);
								}
								Logger.info("File format {} block End.", AMLConstants.CSV_FORMAT);
							}
							
							
							completedFileCountSts = packageWatcherToChkFileCntReached();
							Logger.info("completedFileCountSts - [{}]",completedFileCountSts);
							if (completedFileCountSts) {
								FinSecIndicatorVO finSecIndicatorVOoBj = new FinSecIndicatorVO();
								finSecIndicatorVOoBj = amlDataTblDetailsFetch
										.toSetFinSecIndicatorObjectForDuckDBSts(finSecIndicatorVOoBj);
								finSecIndicatorVOoBj = amlDataTblDetailsFetch
										.toGetRowCountEachAMLTblSetINFincSecIndicator(finSecIndicatorVOoBj);
								finSecIndicatorVOoBj = customerProfiling
										.addCustomerProfilingStsFinSecIndictor(finSecIndicatorVOoBj);
								publishData2Kafka.sendtoKafka(finSecIndicatorVOoBj.getUuid(), finSecIndicatorVOoBj,
										AMLConstants.KAFKA_PUB_TOPIC);
								
								Long endTime = new Date().getTime();
								Logger.info("Total file processed time : {}", commonUtils.findIsHourMinSec((endTime - startDateMain)));
								
							}
							// File fetch interval on each.
							Thread.sleep(fileFetchInterval);
						}
					}
					boolean valid = key.reset();
					if (!valid) { break; }
				}
			} catch (IOException | InterruptedException e) {
				Logger.error("Exception found in FileWatcher@watchDirectory : {}",e);
			}  finally {
				Logger.info("Thread count : [{}]", Thread.activeCount());
			}
		});
		thread.setDaemon(true);
		thread.start();
	}
	
	/**
	 * 
	 * @param file
	 * @throws InterruptedException
	 * waitForFileCompletion
	 * void
	 */
	private void waitForFileCompletion(Path file) throws InterruptedException {
        long previousSize = -1;
        while (true) {
            long currentSize = file.toFile().length();
            if (currentSize == previousSize) break;
            previousSize = currentSize;
            try {
            	//File Coppied Completion Check
				Thread.sleep(fileCompletionCheckval);
			} catch (InterruptedException e) { } // Wait and check again
        }
    }
	
	/**
	 * 
	 * @return
	 * packageWatcherToChkFileCntReached
	 * boolean
	 */
	private boolean packageWatcherToChkFileCntReached() {
		Logger.info("::::::::::::packageWatcherToChkFileCntReached methos called.:::::");
		String currentDateNmFldr = null;
		Path toPath = null;
		boolean cbsFileImportStatus = false;
		long count = 0;
		try {
			currentDateNmFldr = new SimpleDateFormat("yyyyMMdd").format(new Date());
			toPath = Paths.get(DESTINATION_CSV_FOLDER + "/" + currentDateNmFldr + "/");
			Logger.info("Get File Count - CSV Folder Path : [{}]",toPath);
			// while(true) {
			if (toPath != null) {
				count = Files.list(toPath).filter(Files::isRegularFile).count();
				if (count == 1) {
					startDateMain = new Date().getTime();
				}
				Logger.info("Config / Required Count is : [{}], File Count : [{}]", CBSAmlFileC0unt, count);
				if (count == CBSAmlFileC0unt) {
					cbsFileImportStatus = true;
					// break;
				}
			} // Thread.sleep(6000);}
		} catch (Exception e) {
			Logger.error("Exception found in FileWatcher@fileWatcherTogetCount : {}", e);
		} finally {

		}
		Logger.info("::::::::::::packageWatcherToChkFileCntReached method end.:::::\n");
		return cbsFileImportStatus;
	}
	
	
	
}
