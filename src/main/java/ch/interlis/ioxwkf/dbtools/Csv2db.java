package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.Main;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.ObjectEvent;

public class Csv2db extends AbstractImport2db {
	
	/** import csvData to database.
	 * @param file
	 * @param db
	 * @param config
	 * @throws SQLException
	 * @throws IoxException
	 */
	@Override
	public void importData(File file,Connection db,Settings config) throws SQLException, IoxException {
		Map<String, String> attributes=new HashMap<String, String>();
		CsvReader csvReader=new CsvReader(file);
		String definedHeader=config.getValue(Config.HEADER);
		String definedIliDirs=config.getValue(Config.SETTING_ILIDIRS);
		String definedModelNames=config.getValue(Config.SETTING_MODELNAMES);
		String definedDelimiter=config.getValue(Config.DELIMITER);
		String definedRecordDelimiter=config.getValue(Config.RECORD_DELIMITER);
		String definedSchemaName=config.getValue(Config.DBSCHEMA);
		String definedTableName=config.getValue(Config.TABLE);
		List<String> modelNames=null;
		TransferDescription td=null;
		
		EhiLogger.logState("dataFile <"+file.getAbsolutePath()+">");
		if(definedModelNames!=null){
			EhiLogger.logState("modelNames <"+definedModelNames+">");
		}
		
		if(!(file.exists())) {
			throw new IoxException("csv file: "+file.getAbsolutePath()+" not found");
		}
		if(!(file.canRead())) {
			throw new IoxException("csv file: "+file.getAbsolutePath()+" not readable");
		}
		
		// validity of connection
		if(db==null) {
			throw new IoxException("connection==null");
		}else if(!(db.isValid(0))) {
			throw new IoxException("connection to: "+db+" failed");
		}
		
		// header validity
		if(definedHeader==null || !definedHeader.equals(Config.HEADERPRESENT)) {
			definedHeader=Config.HEADERABSENT;
		}
		
		// model directory validity
		List<String> dirList=new ArrayList<String>();
		if(definedIliDirs!=null) {
			String[] dirs=definedIliDirs.split(";");
			for(String dir:dirs) {
				dirList.add(dir);
			}
		}
				
		// models validity
		if(definedModelNames!=null) {
			modelNames=getSpecifiedModelNames(definedModelNames);
			String filePath=null;
			if(definedIliDirs==null) {
				filePath=new java.io.File(file.getPath().toString()).getAbsoluteFile().getParentFile().getAbsolutePath();
			}else {
				filePath=new java.io.File(dirList.get(0).toString()).getAbsoluteFile().getAbsolutePath();
			}
			td=compileIli(modelNames,null,filePath,Main.getIli2cHome(),config);
			if(td==null){
				throw new IoxException("models "+modelNames.toString()+" not found");
			}
		}
		
		// delimiter validity
		if(definedDelimiter==null) {
			definedDelimiter=Config.DEFAULT_DELIMITER;
		}
		
		// record delimiter validity
		if(definedRecordDelimiter==null) {
			definedRecordDelimiter=Config.DEFAULT_RECORD_DELIMITER;
		}
		
		// build csvReader
		if(definedHeader.equals(Config.HEADERPRESENT)) {
			csvReader.setHeader(Config.HEADERPRESENT);
		}else {
			csvReader.setHeader(Config.HEADERABSENT);
		}
		CsvReader.setDelimiter(definedDelimiter);
		csvReader.setRecordDelimiter(definedRecordDelimiter);
		if(td!=null) {
			csvReader.setModel(td);
		}
		
		// read IoxEvents
		IoxEvent event=csvReader.read();
		while(event instanceof IoxEvent){
			if(event instanceof ObjectEvent) {
				IomObject iomObj=((ObjectEvent)event).getIomObject();
				
				// schema validity
				if(definedSchemaName!=null) {
					if(!(schemaExists(definedSchemaName, db))){
						throw new IoxException("schema "+definedSchemaName+" not found");
					}
				}
				
				// table validity
				List<String> databaseAttrNames=new ArrayList<String>();
				if(config.getValue(Config.TABLE)!=null){
					if(definedSchemaName==null) {
						// default schema
					}
					if(dbTableExists(definedSchemaName, definedTableName, db)) {
						// attribute names of database table
						databaseAttrNames=getAttrNamesOfTable(definedSchemaName, definedTableName, db);
					}else {
						throw new IoxException("table "+definedTableName+" not found");
					}
				}else {
					throw new IoxException("expected tablename");
				}
				
				// build attributes
				attributes.clear();
				for(int i=0;i<iomObj.getattrcount();i++) {
					if(databaseAttrNames.contains(iomObj.getattrname(i))) {
						String attrValue=iomObj.getattrvalue(iomObj.getattrname(i));
						if(attrValue==null) {
							attrValue=iomObj.getattrobj(iomObj.getattrname(i), 0).toString();
						}
						if(attrValue!=null) {
							attributes.put(iomObj.getattrname(i), attrValue);
						}
					}else {
						
					}
				}
				if(attributes.size()==0) {
					throw new IoxException("data base attribute names: "+databaseAttrNames.toString()+" not found in "+file.getAbsolutePath());
				}
				
				// insert attributes to database
				insertIntoTable(definedSchemaName, definedTableName, attributes, db, iomObj);
				event=csvReader.read();
			}else {
				// next IoxEvent
				event=csvReader.read();
			}
		}
		
		// close csvReader
		if(csvReader!=null) {
			csvReader.close();
			csvReader=null;
		}
		event=null;
	}
}