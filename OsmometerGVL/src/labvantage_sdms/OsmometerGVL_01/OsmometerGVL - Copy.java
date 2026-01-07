package labvantage_sdms.OsmometerGVL_01;

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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

//osmoTECHXT parser for automatic PDF output

public class OsmometerGVL extends BaseAttachmentHandler {

    @Override
    public void handleData(List<Attachment> list, PropertyList propertyList) throws SapphireException {
        logMessage("handleData() is executing!!!");
        String datacaptureId = "", instrumentId = "";
        SDMSDao sdmsDao = new SDMSDao(this.getConnectionId());
        SDMSCommonUtil commonUtil = new SDMSCommonUtil(sdmsDao);

        for (Attachment attachment : list) {
            datacaptureId = attachment.getKeyId1();
            instrumentId = sdmsDao.getInstrumentID(datacaptureId);
            propertyList.setProperty("instrumentid", instrumentId);
            propertyList.setProperty("datacaptureid", datacaptureId);

            DataSet resultSet = processRawData(attachment.getInputStream(), propertyList, commonUtil, sdmsDao);
            if(resultSet.getRowCount() == 0) {
                logger.info("There is no valid record found in the file for processing!!!");
                logMessage("There is no valid record found in the file for processing!!!");
                continue;
            }
            logMessage("processRawData() is executed successfully!!!");

            //common in every attachment handler
            DataSet resultSetForResultGrid = commonUtil.getResultSetForResultGrid(propertyList, resultSet);
            logMessage("resultSetForResultGrid successfully completed!!!");

            ResultDataGrid resultDataGrid = commonUtil.createResultGrid(resultSetForResultGrid, propertyList,
                    this.getConnectionProcessor().getConnectionInfo(this.getConnectionId()), "Sample");
            logMessage("Create resultDataGrid successfully completed!!!");

            //get the auto release option value
            boolean autoRelease = resultDataGrid.getOptions().getAutoRelease();
            //set the auto release option value to false
            resultDataGrid.getOptions().setAutoRelease(false);
            logMessage("before saveAndUpdateDataItem successfully completed!!!");

            //invoke saveAndUpdateDataItem
            commonUtil.saveAndUpdateDataItem(resultDataGrid, resultSetForResultGrid, getActionProcessor());
            logMessage("saveAndUpdateDataItem successfully completed!!!");

//            //Release Data Item
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
        //String fieldForNullCheck = context.getProperty("field_for_null_check");
        //String analystidFieldName = context.getProperty("analystid_field_name");
        logMessage("instrumentId = " + instrumentId);
        //logMessage("fieldForNullCheck = " + fieldForNullCheck);
        //logMessage("analystidFieldName = " + analystidFieldName);

        String sampleId = "", paramListId = "", paramListVersionId = "", variantId = "";
        String replicateId = "";
        String analystIdInFile = "";
        
        String date=""; 
        String operateID=""; 
        String lot=""; 
        String osmolality="";

        String analystId = "",isSampleValid = "", sampleCreatedBy = "";
       
        int dsRow = -1,  availableDataSet = 0, maxDataSet = 0;

        //sampleId="S-2508-00578";
        //sampleId="S-2508-00611"; // empty sample
       // instrumentId="979289";
		
        
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

        //Get the instrument fields
        //Map<String, String> instrumentFields = sdmsDao.getInstrumentFields(instrumentId);

        //Get All Valid Users
        Map<String, String> validUsers = sdmsDao.getAllUsers();
        BufferedReader reader = null;

        DataSet dsResults = new DataSet();
        commonUtil.addRequiredColumnsToResultSet(dsResults);
        //int testNo = 0;

        try {
            //Reading PDF file
        	String rawText = "";
        	try { 
        		rawText = PDFPlainTextParser.parsePDF(inputStream);     	
			} catch (Exception e) {
				logMessage("Error parsing PDF: " + e.getMessage());
				e.printStackTrace();
				throw new SapphireException("Error parsing PDF File");
			}
        	
        	logMessage("PDF Text = " + rawText);
        			

        	// Get Sample ID
        	
        	String[] lines = rawText.split("\n");
        	for (String line1 : lines) {
        		line1= RemoveHiddenChars(line1);
        		//logMessage("line1= " + line1);
        	    if (line1.contains("Sample ID:")) {
        	    	//logMessage("split = " + line1.split(":")[1].trim());
        	    	lot = line1.split(":")[1].trim();
        	    	//sampleId=getSampleId(lot);
        	    	
        	    	sampleId = getSampleId(lot);
        	        context.setProperty("sampleId", sampleId);
        	        
        	    }
        	    if (line1.contains("User ID:")) {
        	    	analystIdInFile = line1.split(":")[1].trim();
        	    	operateID=getOperateUser(analystIdInFile);
        	    	
        	    }
        	}
        	
        	
        	// Get Osmolality
        	osmolality = PDFParserUtil.extractOsmolality(rawText);
        	
        	// Get Date
        	String datetime = "";
        	if (lines[6]!=null) {
    	    	datetime = lines[6].trim();
    	    	date=getFormattedDate(datetime);
    	    	logMessage("Date = " + date); 
        	}
        	logMessage("Sample ID = " + sampleId);
        	logMessage("Operate ID = " + operateID); 
        	logMessage("Osmolality = " + osmolality);
        	logMessage("Date = " + date); 
        	
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
            
            analystId = sampleCreatedBy;
            Boolean error=false;
            if ("No".equals(isSampleValid)) {
                throw new SapphireException("Invalid Sample Id provided in File!!!!");
            } 
            /*
            if(!StringUtils.isBlank(analystIdInFile) && validUsers.get(analystIdInFile.toLowerCase()) != null){
                analystId = validUsers.get(analystIdInFile.toLowerCase());
            } else {
                analystId = sampleCreatedBy;
            }
            */
            
            if (instrumentId==null){
				logMessage("Instrument Id not found.");
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
			
			if(osmolality==null) {
				logMessage("Osmolality not found in the file.");
				error=true;
			}
			
			if(operateID==null) {
				logMessage("User ID not found in the file or system.");
				error=true;
			}
			if (error==true) {
				throw new SapphireException("Required field not found!!!!");
			}
			
           
            // Code used to check if sample has multiple datasets. If multiple datasets found, increments dataset value, else, returns original dataset number.
            availableDataSet = sdmsDao.getDataSetNumber(sampleId, instrumentId, "Osmolality");
            logMessage("Osmolality availableDataSet = " + availableDataSet);
            
            int dID= sdmsDao.getAvailableReplicate(sampleId,instrumentId, "Osmolality", "", Integer.toString(availableDataSet)); 
            replicateId	=Integer.toString(dID);
       
            PropertyList properties = sdmsDao.getParamListWithoutDataSet(sampleId, instrumentId, "Osmolality");
            
            
            paramListId = properties.getProperty("paramlistid");
            paramListVersionId = properties.getProperty("paramlistversionid");
            variantId = properties.getProperty("variantid");

            logMessage("Osmolality Param List Id = " + paramListId);
            logMessage("Osmolality Param List Version Id = " + paramListVersionId);
            logMessage("Osmolality Variant Id = " + variantId);
            logMessage("Osmolality Replicate Id = " + replicateId);
         
            dsRow = dsResults.addRow();
            dsResults.setValue(dsRow, "sdcid", "Sample");
            dsResults.setValue(dsRow, "keyid1", sampleId);
            dsResults.setValue(dsRow, "paramlistid", paramListId);
            dsResults.setValue(dsRow, "paramlistversionid", paramListVersionId);
            dsResults.setValue(dsRow, "variantid", variantId);
            dsResults.setValue(dsRow, "instrumentfield", "Osmolality");
            dsResults.setValue(dsRow, "dataset", availableDataSet + "");
            dsResults.setValue(dsRow, "replicateid", replicateId);
            dsResults.setValue(dsRow, "value", osmolality);
            dsResults.setValue(dsRow, "s_analystid", analystId);
            
            if (replicateId.equals("1")) {
            dsRow = dsResults.addRow();
            dsResults.setValue(dsRow, "sdcid", "Sample");
            dsResults.setValue(dsRow, "keyid1", sampleId);
            dsResults.setValue(dsRow, "paramlistid", paramListId);
            dsResults.setValue(dsRow, "paramlistversionid", paramListVersionId);
            dsResults.setValue(dsRow, "variantid", variantId);
            dsResults.setValue(dsRow, "instrumentfield", "Date");
            dsResults.setValue(dsRow, "dataset", availableDataSet + "");
            dsResults.setValue(dsRow, "replicateid", replicateId);
            dsResults.setValue(dsRow, "value", date);
            dsResults.setValue(dsRow, "s_analystid", analystId);
            
            dsRow = dsResults.addRow();
            dsResults.setValue(dsRow, "sdcid", "Sample");
            dsResults.setValue(dsRow, "keyid1", sampleId);
            dsResults.setValue(dsRow, "paramlistid", paramListId);
            dsResults.setValue(dsRow, "paramlistversionid", paramListVersionId);
            dsResults.setValue(dsRow, "variantid", variantId);
            dsResults.setValue(dsRow, "instrumentfield", "Sampling_Technician");
            dsResults.setValue(dsRow, "dataset", availableDataSet + "");
            dsResults.setValue(dsRow, "replicateid", replicateId);
            dsResults.setValue(dsRow, "value", operateID);
            dsResults.setValue(dsRow, "s_analystid", analystId);
            
            dsRow = dsResults.addRow();
            dsResults.setValue(dsRow, "sdcid", "Sample");
            dsResults.setValue(dsRow, "keyid1", sampleId);
            dsResults.setValue(dsRow, "paramlistid", paramListId);
            dsResults.setValue(dsRow, "paramlistversionid", paramListVersionId);
            dsResults.setValue(dsRow, "variantid", variantId);
            dsResults.setValue(dsRow, "instrumentfield", "Instrument_Num");
            dsResults.setValue(dsRow, "dataset", availableDataSet + "");
            dsResults.setValue(dsRow, "replicateid", replicateId);
            dsResults.setValue(dsRow, "value", instrumentId);
            dsResults.setValue(dsRow, "s_analystid", analystId);
            
            }
            
            

        } catch (SapphireException e) {
            logMessage(e.getMessage());
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

   

    
    private String getSampleId(String lot) throws SapphireException{
		String sampleId="";
		PropertyList pl = new PropertyList();
		   /*		
        select s_sampleid from s_sample, sdidataitem where sdcid = 'Sample' and s_sampleid = keyid1 
        and paramid in ('Osmolality') and variantid = 'TP-7803' and displayvalue is null 
        and securitydepartment = 'Greenville' and samplestatus <> 'Cancelled' and lower(u_lotid) = lower('XX25001')
        and s_sampleid in (select min(s_sampleid) from s_sample, sdidataitem where sdcid = 'Sample'
        and s_sampleid = keyid1 and paramid in ('Osmolality') and variantid = 'TP-7803' 
        and displayvalue is null and lower(u_lotid) = lower('XX25001') and securitydepartment = 'Greenville' 
        and samplestatus <> 'Cancelled')
		*/
        StringBuffer sampleData = new StringBuffer();
        sampleData.append("select s_sampleid ");
        sampleData.append("from s_sample, sdidataitem ");
        sampleData.append("where sdcid = 'Sample' and s_sampleid = keyid1 ");
        sampleData.append("and paramid in ('Osmolality') "); 
        sampleData.append("and variantid = 'TP-7803' and displayvalue is null "); 
        sampleData.append("and securitydepartment = 'Greenville' and samplestatus <> 'Cancelled' "); 
        sampleData.append("and lower(u_lotid) = lower('").append(lot).append("') ");
        sampleData.append("and s_sampleid in (select min(s_sampleid) "); 
        sampleData.append("from s_sample, sdidataitem ");
        sampleData.append("where sdcid = 'Sample' and s_sampleid = keyid1 ");
        sampleData.append("and paramid in ('Osmolality') ");
        sampleData.append("and variantid = 'TP-7803' and displayvalue is null "); 
        sampleData.append("and lower(u_lotid) = lower('").append(lot).append("') ");
        sampleData.append("and securitydepartment = 'Greenville' and samplestatus <> 'Cancelled')");
       
        DataSet sampleSet = getQueryProcessor().getSqlDataSet(sampleData.toString());
        if (sampleSet.getRowCount() > 0) {
        	sampleId = sampleSet.getValue(0, "s_sampleid");
        	logMessage("Convereted SampleID= " + sampleId);
            }
        return sampleId;
    }
    
    
    
    
    private String getOperateUser(String operateID){
    	String opId="";
    	StringBuffer operateUser = new StringBuffer();
       //select refvalueid,refdisplayvalue from refvalue where reftypeid like 'Chem Sampling Techs' and refvalueid = 'ashwoom'
    	operateUser.append("select refvalueid,refdisplayvalue ");
    	operateUser.append("from refvalue ");
    	operateUser.append("where reftypeid = 'Chem Sampling Techs' ");
    	operateUser.append("and refvalueid = '").append(operateID).append("' ");
    	 
        DataSet operateIds = getQueryProcessor().getSqlDataSet(operateUser.toString());
    	opId = operateIds.getValue(0, "refvalueid");
        return opId;
    }
    
    private String RemoveHiddenChars(String input) {
       String outPut="";
       outPut = input.replaceAll("[\\p{C}\\p{Z}]+", " ").trim();
       return outPut;
    }
    
    private  String getFormattedDate(String date) {
        String inputPattern = "MM/dd/yy"; // Pattern for the input string
        String outputPattern = "MMM dd, yyyy"; // Desired output pattern
        try {
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
