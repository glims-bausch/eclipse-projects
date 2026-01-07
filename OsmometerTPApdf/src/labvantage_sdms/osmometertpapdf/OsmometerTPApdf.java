package labvantage_sdms.osmometertpapdf;

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

//osmoTECHXT parser for automatic PDF output

public class OsmometerTPApdf extends BaseAttachmentHandler {

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
            resultDataGrid.getOptions().setAutoRelease(true);
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
        String replicateInput = "1";
        String analystIdInFile = "";

        String analystId = "",
                isSampleValid = "", sampleCreatedBy = "";
        String sourceWorkItemId = "", sourceWorkItemInstance = "";

        int dsRow = -1, dataSetInput = 0, lengthOfArray = 0, availableDataSet = 0, maxDataSet = 0;

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
        	    if (line1.contains("Sample ID:")) {
        	        sampleId = line1.split(":")[1].trim();
        	        context.setProperty("sampleId", sampleId);
        	        logMessage("Sample ID = " + sampleId);
        	    }
        	}
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

        	// Get User ID
        	
        	for (String line1 : lines) {
        	    if (line1.contains("User ID:")) {
        	    	analystIdInFile = line1.split(":")[1].trim();
        	    	logMessage("Analyst ID = " + analystIdInFile); 
        	    }
        	}
            
        	if(!StringUtils.isBlank(analystIdInFile) && validUsers.get(analystIdInFile.toLowerCase()) != null){
                analystId = validUsers.get(analystIdInFile.toLowerCase());
            } else {
                analystId = sampleCreatedBy;
            }
        	
        	// Get Osmolality
        	String osmolality = PDFParserUtil.extractOsmolality(rawText);
        	logMessage("Osmolality = " + osmolality); 
        	
            //get the parameterlist details for Osmolality 
        	PropertyList maxDataSetDetails = getMaxDataSetNumber(sampleId, "mOsm/kg");
            maxDataSet = Integer.parseInt(maxDataSetDetails.getProperty("maxdataset"));
            sourceWorkItemId = maxDataSetDetails.getProperty("sourceworkitemid");
            sourceWorkItemInstance = maxDataSetDetails.getProperty("sourceworkiteminstance");
                
            logMessage("Osmolality maxDataSet = " + maxDataSet);
            logMessage("Osmolality sourceWorkItemId = " + sourceWorkItemId);
            logMessage("Osmolality sourceWorkItemInstance = " + sourceWorkItemInstance);

            // Code used to check if sample has multiple datasets. If multiple datasets found, increments dataset value, else, returns original dataset number.
            availableDataSet = sdmsDao.getDataSetNumber(sampleId, instrumentId, "mOsm/kg");
            logMessage("Osmolality availableDataSet = " + availableDataSet);
            
            PropertyList properties = sdmsDao.getParamListWithoutDataSet(sampleId, instrumentId, "mOsm/kg");
            paramListId = properties.getProperty("paramlistid");
            paramListVersionId = properties.getProperty("paramlistversionid");
            variantId = properties.getProperty("variantid");

            logMessage("Osmolality Param List Id = " + paramListId);
            logMessage("Osmolality Param List Version Id = " + paramListVersionId);
            logMessage("Osmolality Variant Id = " + variantId);

            dsRow = dsResults.addRow();
            dsResults.setValue(dsRow, "sdcid", "Sample");
            dsResults.setValue(dsRow, "keyid1", sampleId);
            dsResults.setValue(dsRow, "paramlistid", paramListId);
            dsResults.setValue(dsRow, "paramlistversionid", paramListVersionId);
            dsResults.setValue(dsRow, "variantid", variantId);
            dsResults.setValue(dsRow, "instrumentfield", "mOsm/kg");
            dsResults.setValue(dsRow, "dataset", availableDataSet + "");
            dsResults.setValue(dsRow, "replicateid", replicateInput);
            dsResults.setValue(dsRow, "value", osmolality);
            dsResults.setValue(dsRow, "s_analystid", analystId);
            
        	// Get Date/Time
        	String datetime = "";
        	if (lines[6]!=null) {
    	    	datetime = lines[6].trim();
    	    	logMessage("Date/Time = " + datetime); 
        	}

            //get the parameterlist details for Date/Time   
        	PropertyList maxDataSetDetails1 = getMaxDataSetNumber(sampleId, "mOsm/kg");
            maxDataSet = Integer.parseInt(maxDataSetDetails1.getProperty("maxdataset"));
            sourceWorkItemId = maxDataSetDetails1.getProperty("sourceworkitemid");
            sourceWorkItemInstance = maxDataSetDetails1.getProperty("sourceworkiteminstance");
                
            logMessage("Date/Time maxDataSet = " + maxDataSet);
            logMessage("Date/Time sourceWorkItemId = " + sourceWorkItemId);
            logMessage("Date/Time sourceWorkItemInstance = " + sourceWorkItemInstance);

            // Code used to check if sample has multiple datasets. If multiple datasets found, increments dataset value, else, returns original dataset number.
            availableDataSet = sdmsDao.getDataSetNumber(sampleId, instrumentId, "Date/Time");
            logMessage("Date/Time availableDataSet = " + availableDataSet);
        	
            PropertyList properties1 = sdmsDao.getParamListWithoutDataSet(sampleId, instrumentId, "Date/Time");

            paramListId = properties1.getProperty("paramlistid");
            paramListVersionId = properties1.getProperty("paramlistversionid");
            variantId = properties1.getProperty("variantid");

            logMessage("Date/Time Param List Id = " + paramListId);
            logMessage("Date/Time Param List Version Id = " + paramListVersionId);
            logMessage("Date/Time Variant Id = " + variantId);

            dsRow = dsResults.addRow();
            dsResults.setValue(dsRow, "sdcid", "Sample");
            dsResults.setValue(dsRow, "keyid1", sampleId);
            dsResults.setValue(dsRow, "paramlistid", paramListId);
            dsResults.setValue(dsRow, "paramlistversionid", paramListVersionId);
            dsResults.setValue(dsRow, "variantid", variantId);
            dsResults.setValue(dsRow, "instrumentfield", "Date/Time");
            dsResults.setValue(dsRow, "dataset", availableDataSet + "");
            dsResults.setValue(dsRow, "replicateid", replicateInput);
            dsResults.setValue(dsRow, "value", datetime);
            dsResults.setValue(dsRow, "s_analystid", analystId);
            
            // Get Description
        	String description = "";
        	if (lines[11]!=null) {
        		description = lines[12].trim();
    	    	logMessage("Description = " + description); 
        	} else {
        		description = "NA";
        		logMessage("Description = " + description);
        	}

            //get the parameterlist details for Description 
        	PropertyList maxDataSetDetails2 = getMaxDataSetNumber(sampleId, "mOsm/kg");
            maxDataSet = Integer.parseInt(maxDataSetDetails2.getProperty("maxdataset"));
            sourceWorkItemId = maxDataSetDetails2.getProperty("sourceworkitemid");
            sourceWorkItemInstance = maxDataSetDetails2.getProperty("sourceworkiteminstance");
                
            logMessage("Description maxDataSet = " + maxDataSet);
            logMessage("Description sourceWorkItemId = " + sourceWorkItemId);
            logMessage("Description sourceWorkItemInstance = " + sourceWorkItemInstance);

            // Code used to check if sample has multiple datasets. If multiple datasets found, increments dataset value, else, returns original dataset number.
            availableDataSet = sdmsDao.getDataSetNumber(sampleId, instrumentId, "Description");
            logMessage("Description availableDataSet = " + availableDataSet);
        	
            PropertyList properties2 = sdmsDao.getParamListWithoutDataSet(sampleId, instrumentId, "Description");

            paramListId = properties2.getProperty("paramlistid");
            paramListVersionId = properties2.getProperty("paramlistversionid");
            variantId = properties2.getProperty("variantid");

            logMessage("Description Param List Id = " + paramListId);
            logMessage("Description Param List Version Id = " + paramListVersionId);
            logMessage("Description Variant Id = " + variantId);

            dsRow = dsResults.addRow();
            dsResults.setValue(dsRow, "sdcid", "Sample");
            dsResults.setValue(dsRow, "keyid1", sampleId);
            dsResults.setValue(dsRow, "paramlistid", paramListId);
            dsResults.setValue(dsRow, "paramlistversionid", paramListVersionId);
            dsResults.setValue(dsRow, "variantid", variantId);
            dsResults.setValue(dsRow, "instrumentfield", "Description");
            dsResults.setValue(dsRow, "dataset", availableDataSet + "");
            dsResults.setValue(dsRow, "replicateid", replicateInput);
            dsResults.setValue(dsRow, "value", description);
            dsResults.setValue(dsRow, "s_analystid", analystId);
                
            

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

    private PropertyList getMaxDataSetNumber(String sampleID, String instrFieldID) {
        PropertyList pl = new PropertyList();
        StringBuffer sql = new StringBuffer();
        sql.append("select sd.SOURCEWORKITEMID, sd.SOURCEWORKITEMINSTANCE, max(sd.dataset) maxdataset");
        sql.append(" from sdidata sd");
        sql.append(" where sd.sdcid = 'Sample'");
        sql.append(" and sd.keyid1 = '").append(sampleID).append("' ");
        sql.append(" group by sd.sourceworkitemid, sd.sourceworkiteminstance order by sd.SOURCEWORKITEMINSTANCE desc");

        DataSet maxdata = getQueryProcessor().getSqlDataSet(sql.toString());
        pl.setProperty("maxdataset", maxdata.getValue(0, "maxdataset"));
        pl.setProperty("sourceworkitemid", maxdata.getValue(0, "sourceworkitemid"));
        pl.setProperty("sourceworkiteminstance", maxdata.getValue(0, "sourceworkiteminstance"));
        return pl;
    }

}
