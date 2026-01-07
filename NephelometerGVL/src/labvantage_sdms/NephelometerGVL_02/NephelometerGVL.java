package labvantage_sdms.NephelometerGVL_02;

import com.labvantage.training.sdms.dao.SDMSDao;
import com.labvantage.training.sdms.utl.SDMSCommonUtil;
import com.labvantage.sapphire.modules.sdms.util.ResultDataGrid;
import org.apache.commons.lang.StringUtils;
import sapphire.SapphireException;
import sapphire.attachment.Attachment;
import sapphire.attachmenthandler.BaseAttachmentHandler;
import sapphire.util.DataSet;
import sapphire.util.StringUtil;
import sapphire.xml.PropertyList;

import java.io.*;
import java.util.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;

//Java interface code for GreenVille Nephelometer


public class NephelometerGVL extends BaseAttachmentHandler {
    public static final String FIELD_SEPARATOR = ",";
    private static final String SAMPLE_ID_COLUMN = "Sample ID";
    public void handleData(List<Attachment> list, PropertyList propertyList) throws SapphireException {
        logMessage("handleData() is executing!!!");
        String datacaptureId = "", instrumentId = "";
        SDMSDao sdmsDao = new SDMSDao(this.getConnectionId());
        SDMSCommonUtil commonUtil = new SDMSCommonUtil(sdmsDao);
        
        for (Attachment attachment : list) {
        	
            datacaptureId = attachment.getKeyId1();
            instrumentId = sdmsDao.getInstrumentID(datacaptureId);
            //List<String> allLines = getDataFromAttachment(attachment.getInputStream());
            
            propertyList.setProperty("instrumentid", instrumentId);
            propertyList.setProperty("datacaptureid", datacaptureId);
            
            logMessage("instrumentId = " + instrumentId );
            logMessage("datacaptureId = " + datacaptureId);

            DataSet resultSet = processRawData(attachment.getInputStream(), propertyList, commonUtil, sdmsDao);
            
            if(resultSet==null || resultSet.getRowCount() == 0) {
                logMessage("There is no valid record found in the file for processing!!!");
                continue;
            }
            if(resultSet !=null)
            logMessage("processRawData() is executed successfully!!!");

            //common in every attachment handler
            DataSet resultSetForResultGrid = commonUtil.getResultSetForResultGrid(propertyList, resultSet);
            logMessage("resultSetForResultGrid successfully completed!!!");

            ResultDataGrid resultDataGrid = commonUtil.createResultGrid(resultSetForResultGrid, propertyList,
                    this.getConnectionProcessor().getConnectionInfo(this.getConnectionId()), "Sample");
            logMessage("Create resultDataGrid successfully completed!!!");

            //get the auto release option value
            boolean autoRelease = resultDataGrid.getOptions().getAutoRelease();
            //set the auto release option value to true
            resultDataGrid.getOptions().setAutoRelease(false);
            
            logMessage("before saveAndUpdateDataItem successfully completed!!!");

            //invoke saveAndUpdateDataItem
            commonUtil.saveAndUpdateDataItem(resultDataGrid, resultSetForResultGrid, getActionProcessor());
            logMessage("saveAndUpdateDataItem successfully completed!!!");

            //Release Data Item
            commonUtil.releaseDataItem(propertyList, resultSetForResultGrid, autoRelease, getActionProcessor(),
                    this.getConfigurationProcessor());
            logMessage("Data is updated in LIMS successfully!!!");

            //create Required Links
            //createRequiredLinks(resultDataGrid);

            //Post Data Actions.
            commonUtil.executePostDataEntryAction(propertyList, resultSetForResultGrid, getActionProcessor());

            logMessage("handleData() is executed successfully!!!");
        }
    }

    

    // Update processRawData by writing a code to parse the given file:
    private DataSet processRawData(InputStream inputStream, PropertyList context, SDMSCommonUtil commonUtil, SDMSDao sdmsDao ) throws SapphireException{
        String instrumentId = context.getProperty("instrumentid");
       
        String sampleId="";
        String paramListId = "";
        String paramListVersionId = "";
        String variantId = "";
        //String dataset= "";
        String replicateId="";
        String analystid="";
        int dsRow = -1,availableDataSet = 0, maxDataSet = 0;
        String  isSampleValid = "";
        String sampleCreatedBy = "";
      
        
        String line;
        String serialNumber ="";  // B2 to get instrument ID ->select instrument id from instrument where serialnum = 'serialNumber'
        String date=""; //A5
        String log=""; //B5
        String result=""; //C5
        String unit=""; //D5
        String operateID=""; //H5
        String lot=""; //I5 internal lot - search for sample id with SQL
       

        //Main HashMaps needed for Parsing
        Map<String, Integer> sampleAndDataSet = new HashMap<String, Integer>();

        //HashMaps to check Sample Validity
        Map<String, String> sampleAndSampleCreatedBy = new LinkedHashMap<String, String>();
        Map<String, String> sampleAndSampleStatus = new LinkedHashMap<String, String>();
        Map<String, Integer> sampleAndMaxDataSet = new HashMap<String, Integer>();
        Map<String, String> sampleAndSourceWorkItemId = new LinkedHashMap<String, String>();
        Map<String, String> sampleAndSourceWorkItemInstance = new LinkedHashMap<String, String>();

        //HashMaps to get Parameter List details
        Map<String, String> sampleAndParamListId = new LinkedHashMap<String, String>();
        Map<String, String> sampleAndParamListVersionId = new LinkedHashMap<String, String>();
        Map<String, String> sampleAndVariantId = new LinkedHashMap<String, String>();

        BufferedReader reader = null;
        DataSet dsResults = null;
       // DataSet dsResults = new DataSet();
       // commonUtil.addRequiredColumnsToResultSet(dsResults);
        
        try {
           
			reader = new BufferedReader(new InputStreamReader(inputStream));
			
			// Read all lines into a list
			List<String> allLines = new ArrayList<>();
			while ((line = reader.readLine()) != null) {
				if (!StringUtils.isBlank(line)&& line.length()>1) {
					allLines.add(line);
				}
			}
			String sampleDataLine="";
			//data is in line #4
			if(allLines.size()>=4){
				sampleDataLine = allLines.get(4);
			}
		
			String[] sampleDataCol =sampleDataLine.split(",");
			//logMessage("Parsing lot number at specific col = " + sampleDataCol[8]);
			
			//serialNumber = serialNumberCol[1];  // B2 to get instrument ID ->select instrument id from instrument where serialnum = 'serialNumber'
	        date=sampleDataCol[0]; //A5
	        log=sampleDataCol[1]; //B5
	        result=sampleDataCol[2]; //C5
	        unit=sampleDataCol[3]; //D5
	        operateID=sampleDataCol[7]; //H5getSampleId
	        lot=sampleDataCol[8]; //I5 internal lot - search for sample id with SQL
	        
	        //Data from file
	        /*
	        logMessage("Parsing all sample data !!! " );
            logMessage("date = " + date);
            logMessage("log = " + log);
            logMessage("result= " + result);
            logMessage("unit = " + unit);
            logMessage("operateID = " + operateID);
            logMessage("Lot = " + lot);
            */
	        
            //remove extra hidden chars
            //String stringWithoutEmptyChars = lot.replaceAll("\\s+", "");
            lot = lot.replaceAll("\\p{C}", "");
            operateID = operateID.replaceAll("\\p{C}", "");
            result= result.replaceAll("\\p{C}", "");
            date= date.replaceAll("\\p{C}", "");
            unit=unit.replaceAll("\\p{C}", "");
            log=log.replaceAll("\\p{C}", "");

            date=getFormattedDate(date);
            
            logMessage("Parsing all sample data !!! " );
            logMessage("date = " + date);
            logMessage("log = " + log);
            logMessage("result= " + result);
            logMessage("unit = " + unit);
            logMessage("operateID = " + operateID);
            logMessage("Lot = " +lot);
            logMessage("Instrument = " + instrumentId);
            
            String logAlt1 = "Verification Log";
            String logAlt2 = "Calibration Log";
           // logMessage("logAlt1 = " + logAlt1 + ".  logAlt1.");
            if(log.equals(logAlt1) || log.equals(logAlt2)){
            	logMessage("Log = " + log + ".  Sample is not processed further.");
            	return dsResults;
            }
            
            //get sample from lot number
			sampleId = getSampleId(lot);
			logMessage("SampleId = " + sampleId);
			
			//get operaterID
			operateID=getOperateUser(operateID);
			logMessage("OperatorId = " + operateID);
			
	        Boolean error=false;   
			//Failed parsing if any of four required fields are missing
			
			if (instrumentId==null){
				logMessage("Instrument Id not found in the file.");
				error=true;
			}
			if (sampleId==null){
				logMessage("Sample Id not found in the file or dataset is existing.");
				error=true;
			}
			if(date==null) {
				logMessage("Date tested not found in the file.");
			    error=true;
			}
			
			if(result==null) {
				logMessage("Result not found in the file.");
				error=true;
			}
			
			if(operateID==null) {
				logMessage("Operator ID not found in the file or system.");
				error=true;
			}
			if(unit.equals("FTU") || unit.equals("NTU")){
			}
			else {
			//if(unit!="FTU" || unit!="NTU") {
				logMessage("Invalid Unit: " + unit );
				error=true;
			}
			if (error==true) {
				throw new SapphireException("Exiting all further processing.Required field not found!!!!");
			}
			dsResults = new DataSet();
	        commonUtil.addRequiredColumnsToResultSet(dsResults);
		
			//check if sample is valid or not
			if(sampleAndSampleCreatedBy.get(sampleId) == null) {
				PropertyList sampleDetails = sdmsDao.getSampleDetails(sampleId);
				isSampleValid = sampleDetails.getProperty("isvalidsample");
				sampleCreatedBy = sampleDetails.getProperty("createby");
				sampleAndSampleCreatedBy.put(sampleId, sampleCreatedBy);
				sampleAndSampleStatus.put(sampleId, isSampleValid);
			} else {
				sampleCreatedBy = sampleAndSampleCreatedBy.get(sampleId);
				isSampleValid = sampleAndSampleStatus.get(sampleId);
			}

			if ("No".equals(isSampleValid)) {
				throw new SapphireException("Invalid Sample Id provided in File!!!!");
			}
			

            // Code used to check if sample has multiple datasets. If multiple datasets found, increments dataset value, else, returns original dataset number.
            availableDataSet = sdmsDao.getDataSetNumber(sampleId, instrumentId, "Result");
            logMessage("Result availableDataSet = " + availableDataSet);
            
            int dID= sdmsDao.getAvailableReplicate(sampleId,instrumentId, "Result", "", Integer.toString(availableDataSet)); 
            replicateId	=Integer.toString(dID);
			
			analystid=sampleCreatedBy;
			logMessage("Get parameter list");
		    //PropertyList properties = getParamList(sampleId, instrumentId, "Result");
			PropertyList properties = sdmsDao.getParamListWithoutDataSet(sampleId, instrumentId, "Result");
			
            
            paramListId = properties.getProperty("paramlistid");
            paramListVersionId = properties.getProperty("paramlistversionid");
            variantId = properties.getProperty("variantid");

            logMessage("Result Param List Id = " + paramListId);
            logMessage("Result Param List Version Id = " + paramListVersionId);
            logMessage("Result Variant Id = " + variantId);
            
            
            //first paramListId 
            logMessage("Get paramListId --------------");
            logMessage("paramListId = " + paramListId);
            logMessage("paramListVersionId = " + paramListVersionId);
            logMessage("variantId = " + variantId);
            logMessage("dataset = " + availableDataSet);
            logMessage("replicateid = " + replicateId);
            logMessage("analystid = " + analystid);
            
            dsRow = dsResults.addRow();
			dsResults.setValue(dsRow, "sdcid", "Sample");
			dsResults.setValue(dsRow, "keyid1", sampleId);
			dsResults.setValue(dsRow, "paramlistid", paramListId);
			dsResults.setValue(dsRow, "paramlistversionid", paramListVersionId);
			dsResults.setValue(dsRow, "variantid",variantId);
			dsResults.setValue(dsRow, "instrumentfield", "Result");
			dsResults.setValue(dsRow, "replicateid", replicateId);
			dsResults.setValue(dsRow, "dataset", availableDataSet + "");
			dsResults.setValue(dsRow, "s_analystid", analystid);
			dsResults.setValue(dsRow, "value", result);
			
		
			logMessage("instrumentfield" +"=="+ "Result");
			logMessage("result = " + result);
			
			if (replicateId.equals("1")) {
				dsRow = dsResults.addRow();
				dsResults.addRow();
				dsResults.setValue(dsRow, "sdcid", "Sample");
				dsResults.setValue(dsRow, "keyid1", sampleId);
				dsResults.setValue(dsRow, "paramlistid", paramListId);
				dsResults.setValue(dsRow, "paramlistversionid", paramListVersionId);
				dsResults.setValue(dsRow, "variantid", variantId);
				dsResults.setValue(dsRow, "instrumentfield", "Instrument_Num");
				dsResults.setValue(dsRow, "replicateid", replicateId);
				dsResults.setValue(dsRow, "dataset", availableDataSet + "");
				dsResults.setValue(dsRow, "s_analystid", analystid);
				dsResults.setValue(dsRow, "value", instrumentId);
				
				
				logMessage("instrumentfield" +"=="+ "Instrument_Num");
				logMessage("instrumentId = " + instrumentId);
				
			
				 dsRow = dsResults.addRow();
				dsResults.setValue(dsRow, "sdcid", "Sample");
				dsResults.setValue(dsRow, "keyid1", sampleId);
				dsResults.setValue(dsRow, "paramlistid", paramListId);
				dsResults.setValue(dsRow, "paramlistversionid", paramListVersionId);
				dsResults.setValue(dsRow, "variantid", variantId);
				dsResults.setValue(dsRow, "instrumentfield", "Date");
				dsResults.setValue(dsRow, "replicateid", replicateId);
				dsResults.setValue(dsRow, "dataset", availableDataSet + "");
				dsResults.setValue(dsRow, "s_analystid", analystid);
				dsResults.setValue(dsRow, "value", date);
			
				
				logMessage("instrumentfield" +"=="+ "Date");
				logMessage("date = " + date);
			
				dsRow = dsResults.addRow();
				dsResults.setValue(dsRow, "sdcid", "Sample");
				dsResults.setValue(dsRow, "keyid1", sampleId);
				dsResults.setValue(dsRow, "paramlistid", paramListId);
				dsResults.setValue(dsRow, "paramlistversionid", paramListVersionId);
				dsResults.setValue(dsRow, "variantid",variantId);
				dsResults.setValue(dsRow, "instrumentfield", "Sampling_Technician");
				dsResults.setValue(dsRow, "replicateid", replicateId);
				dsResults.setValue(dsRow, "dataset", availableDataSet + "");
				dsResults.setValue(dsRow, "s_analystid", analystid);
				dsResults.setValue(dsRow, "value", operateID);
				logMessage("instrumentfield" +"=="+ "Sampling_Technician");
				logMessage("operateID = " + operateID);
			}
			
			
		} catch (SapphireException e) {
            logMessage(e.getMessage());
            e.printStackTrace();
            throw new SapphireException(e.getMessage());
        } catch (FileNotFoundException e){
            e.printStackTrace();
            throw new SapphireException(e.getMessage());
        } catch(IOException e){
            e.printStackTrace();
            throw new SapphireException(e.getMessage());
        } finally{
            if(reader != null ){
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new SapphireException(e.getMessage());
                }
            }
        }

        return dsResults;
    }
    
   
    

    private String getOperateUser(String operateID){
    	String opId="";
    	StringBuffer operateUser = new StringBuffer();
       //select refvalueid,refdisplayvalue from refvalue where reftypeid like 'Chem Sampling Techs' and refvalueid = 'ashwoom'
    	operateUser.append("select refvalueid,refdisplayvalue ");
    	operateUser.append("from refvalue ");
    	operateUser.append("where reftypeid = 'Chem Sampling Techs' ");
    	//operateUser.append("and refvalueid = '").append(operateID).append("' ");
    	operateUser.append("and lower(refvalueid) = lower('").append(operateID).append("') ");
    	 
        DataSet operateIds = getQueryProcessor().getSqlDataSet(operateUser.toString());
    	opId = operateIds.getValue(0, "refvalueid");
    	logMessage("Converted Sampling_Technician = " + opId);
    	//logMessage("OP ID" + operateUser);
        
        return opId;
    }


	
	
	private String getSampleId(String lot) throws SapphireException{
		String sampleId="";
		   /*
		select sourcespsourcelabel, u_lotid, s_sampleid from s_sample, sdidataitem 
		where sdcid = 'Sample' and s_sampleid = keyid1 and paramid in ('Clarity', 'Turbidity') 
		and variantid = 'TP-7873' and displayvalue is null and securitydepartment = 'Greenville' 
		and samplestatus <> 'Cancelled' and lower(u_lotid) = lower('TestFPBatchGVL2') 
		and s_sampleid in (select min(s_sampleid) from s_sample, sdidataitem 
		where sdcid = 'Sample' and s_sampleid = keyid1 and paramid in ('Clarity', 'Turbidity') 
		and variantid = 'TP-7873' and displayvalue is null and lower(u_lotid) = lower('TestFPBatchGVL2') 
		and securitydepartment = 'Greenville' and samplestatus <> 'Cancelled')
		
		
		securitydepartment = 'Greenville' and samplestatus <> 'Cancelled' and lower(u_lotid) = lower('AB25000 BA25000') and s_sampleid in (select min(s_sampleid) from s_sample, sdidataitem where sdcid = 'Sample' and s_sampleid = keyid1 and paramid in ('Clarity', 'Turbidity') and variantid = 'TP-7873' and displayvalue is null and lower(u_lotid) = lower('AB25000 BA25000') and securitydepartment = 'Greenville' and samplestatus <> 'Cancelled')
		*/
        StringBuffer sampleData = new StringBuffer();
        sampleData.append("select s_sampleid ");
        sampleData.append("from s_sample, sdidataitem ");
        sampleData.append("where sdcid = 'Sample' and s_sampleid = keyid1 ");
        sampleData.append("and paramid in ('Clarity', 'Turbidity') "); 
        sampleData.append("and variantid = 'TP-7873' and displayvalue is null "); 
        sampleData.append("and securitydepartment = 'Greenville' and samplestatus <> 'Cancelled' "); 
        sampleData.append("and lower(u_lotid) = lower('").append(lot).append("') ");
        sampleData.append("and s_sampleid in (select min(s_sampleid) "); 
        sampleData.append("from s_sample, sdidataitem ");
        sampleData.append("where sdcid = 'Sample' and s_sampleid = keyid1 ");
        sampleData.append("and paramid in ('Clarity', 'Turbidity') ");
        sampleData.append("and variantid = 'TP-7873' and displayvalue is null "); 
        sampleData.append("and lower(u_lotid) = lower('").append(lot).append("') ");
        sampleData.append("and securitydepartment = 'Greenville' and samplestatus <> 'Cancelled')");
        //logMessage("sql get SampleID= " + sampleData);
        
        DataSet sampleSet = getQueryProcessor().getSqlDataSet(sampleData.toString());
    	if (sampleSet.getRowCount() > 0) {
        	sampleId = sampleSet.getValue(0, "s_sampleid");
        	logMessage("Convereted SampleID = " + sampleId);
            }
        return sampleId;
    }
	
	
	public  PropertyList getParamList(String sampleID, String instrumentID, String instrFieldID) {

        String sparamlistid ="";
        String sparamlistversionid ="";
        String svariantid ="";
        String sdataset="";
        String sreplicateid="";
        String sanalystid="";
        PropertyList pl = new PropertyList();
        

        StringBuffer sql = new StringBuffer();
        sql.append(" select di.paramlistid, di.paramlistversionid, di.variantid,di.variantid,di.replicateid,di.dataset,di.s_analystid");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" left join instrument i on i.instrumenttype = pl.s_instrumenttype");
        if(instrumentID != null){
            sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        }
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");

        //logMessage("getParamListWithoutDataSet_V1:: " + sql.toString());

        DataSet dsParamlist = getQueryProcessor().getSqlDataSet(sql.toString());
        
        if (dsParamlist.getRowCount() > 0) {
            sparamlistid = dsParamlist.getValue(0, "paramlistid");
            sparamlistversionid = dsParamlist.getValue(0, "paramlistversionid");
            svariantid = dsParamlist.getValue(0, "variantid");
            sdataset=dsParamlist.getValue(0, "dataset");
            sreplicateid=dsParamlist.getValue(0, "replicateid");
            sanalystid=dsParamlist.getValue(0, "s_analystid");

            pl.setProperty("sdcid", "Sample");
            pl.setProperty("keyid1", sampleID);
            pl.setProperty("keyid2", "(null)");
            pl.setProperty("keyid3", "(null)");
            pl.setProperty("paramlistid", sparamlistid);
            pl.setProperty("paramlistversionid", sparamlistversionid);
            pl.setProperty("variantid", svariantid);
            pl.setProperty("dataset", sdataset);
            pl.setProperty("replicateid", sreplicateid);
            pl.setProperty("s_analystid", sanalystid);
        }
        return pl;

    }
	
	// not used 
        private Boolean dataExisting(String sampleId){
    	Boolean dataExists=false;
     	String instrumentid="";
        String displayvalue="";
    	StringBuffer dataEntryExist = new StringBuffer();
    
       //	select  instrumentid, displayvalue from sdidataitem 
       // where keyid1 = 'S-2508-00366' and paramid in ('Clarity', 'Turbidity')
    	
    	dataEntryExist.append("select instrumentid,displayvalue ");
    	dataEntryExist.append("from displayvalue ");
    	dataEntryExist.append("where paramid in ('Clarity', 'Turbidity') ");
    	dataEntryExist.append("and keyid1 = '").append(sampleId).append("' ");
    	 
        DataSet operateIds = getQueryProcessor().getSqlDataSet(dataEntryExist.toString());
       
        instrumentid = operateIds.getValue(0, "instrumentid");
    	displayvalue= operateIds.getValue(0, "displayvalue");
    	logMessage("Param displayvalue= " + displayvalue);
    	logMessage("Param instrumentid= " + instrumentid);
    	if (instrumentid !=null || displayvalue !="0")
    		dataExists=true;

        return dataExists;
       
    }

	
    private  String getFormattedDate(String date) {
    String inputPattern = "yyyy/MM/dd"; // Pattern for the input string
    String outputPattern = "MMM dd, yyyy"; // Desired output pattern

    try {
    	//date format Aug 22, 2024
        if (!Character.isDigit(date.charAt(0)) ) {
                   date=date.substring(1);
        
        if(date.length()>=10)
        	date=date.substring(0,10);
        }

        // 1. Parse the input date string
        SimpleDateFormat inputFormatter = new SimpleDateFormat(inputPattern);
        Date dateTo = inputFormatter.parse(date);

        // 2. Format the Date object to the desired output pattern
        SimpleDateFormat outputFormatter = new SimpleDateFormat(outputPattern);
        date = outputFormatter.format(dateTo);
    
    }catch (ParseException e) {
    	logMessage("Error parsing date!!! ");
		}
	
	return date;
	}
    
    
}