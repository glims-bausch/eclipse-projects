package labvantage_sdms.MicrotracTPA_01;

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

//Java interface code for Tampa Microtrac


public class MicrotracTPA extends BaseAttachmentHandler {
    public static final String FIELD_SEPARATOR = ",";

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
            //set the auto release option value to true
            resultDataGrid.getOptions().setAutoRelease(true);
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
        String fieldForNullCheck = context.getProperty("field_for_null_check");
        String analystidFieldName = context.getProperty("analystid_field_name");
        logMessage("instrumentId = " + instrumentId);
        //logMessage("fieldForNullCheck = " + fieldForNullCheck);
        //logMessage("analystidFieldName = " + analystidFieldName);

        String sampleId = "", paramListId = "", paramListVersionId = "", variantId = "", dataSet4Props = "";
        String replicateInput = "1";
        String analystIdInFile = "";

        String tempLine = "", instrumentField = "", analystId = "",
                isSampleValid = "", sampleCreatedBy = "";
        String sourceWorkItemId = "", sourceWorkItemInstance = "";
        String line;
        String[] lineSplits;

        int dsRow = -1, dataSetInput = 0, samplePosition = 0, lengthOfArray = 0, analystPosition = 0, availableDataSet = 0, maxDataSet = 0;
        boolean isRawDataHeaderFound = false;

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

        DataSet dsResults = new DataSet();
        commonUtil.addRequiredColumnsToResultSet(dsResults);
        
        try {
            //TPA Parsing of Microtrac CSV File
			//Setting full csv to variable
			reader = new BufferedReader(new InputStreamReader(inputStream));
			StringBuilder stringBuilder1 = new StringBuilder();
			
			while((line = reader.readLine()) != null) {
				stringBuilder1.append(line);
				stringBuilder1.append(System.lineSeparator());
			}
			
			reader.close();
			
			String fulltext = stringBuilder1.toString();

			//Extracting Sample ID 
			String[] rows = fulltext.split("\n");
			String[] columns = rows[1].split(",");
			String[] presampleid = columns[1].trim().split("SID:");
			sampleId = presampleid[1].trim();
			logMessage("Sample ID = " + sampleId);
			
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

			if (sampleAndMaxDataSet.get(sampleId) == null) {
				PropertyList maxDataSetDetails = getMaxDataSetNumber(sampleId, fieldForNullCheck);
				maxDataSet = Integer.parseInt(maxDataSetDetails.getProperty("maxdataset"));
				sourceWorkItemId = maxDataSetDetails.getProperty("sourceworkitemid");
				sourceWorkItemInstance = maxDataSetDetails.getProperty("sourceworkiteminstance");
				sampleAndMaxDataSet.put(sampleId, maxDataSet);
				sampleAndSourceWorkItemId.put(sampleId, sourceWorkItemId);
				sampleAndSourceWorkItemInstance.put(sampleId, sourceWorkItemInstance);
				
			} else {
				maxDataSet = sampleAndMaxDataSet.get(sampleId);
				sourceWorkItemId = sampleAndSourceWorkItemId.get(sampleId);
				sourceWorkItemInstance = sampleAndSourceWorkItemInstance.get(sampleId);
			}
			
			logMessage("maxdataset = " + maxDataSet + "; sourceworkitemid = " + sourceWorkItemId + "; sourceworkiteminstance = " + sourceWorkItemInstance);

			// Code used to check if sample has multiple datasets. If multiple datasets found, increments dataset value, else, returns original dataset number.
			if(lengthOfArray == 1){
				if(sampleAndDataSet.get(sampleId) != null){
					availableDataSet = sampleAndDataSet.get(sampleId) + 1;
				} else {
					availableDataSet = sdmsDao.getDataSetNumber(sampleId, instrumentId, fieldForNullCheck);
				}
				if( availableDataSet == 0) {
					//availableDataSet = maxDataSet + 1;
					//logMessage("No dataset available, skipping this sample = " + sampleId);
					//continue;
				}
				sampleAndDataSet.put(sampleId, availableDataSet);
			} else if (lengthOfArray > 1) {
				//Use the given dataset and store it in sampleAndDataSet Map
				availableDataSet = dataSetInput;
				sampleAndDataSet.put(sampleId, availableDataSet);
			} 



			//get the parameterlist details
			if(sampleAndParamListId.get(sampleId) == null) {
				StringBuffer sql4ParamLists = new StringBuffer();
				sql4ParamLists.append("select sd.paramlistid, sd.paramlistversionid, sd.dataset, di.replicate ");
				sql4ParamLists.append("from sdidata sd ");
				sql4ParamLists.append("where sd.sdcid = 'Sample' and sd.keyid1 = '" + sampleId + "' and sd.variantid = 'TPA Microtrac' ");
				sql4ParamLists.append("and sd.usersequence = (select min(usersequence) from sdidata where sdcid = 'Sample'  ");
				sql4ParamLists.append("and keyid1 = '" + sampleId + "' and variantid = 'TPA Microtrac' and s_datasetstatus = 'Initial') ");

		        DataSet paramLists4Props = getQueryProcessor().getSqlDataSet(sql4ParamLists.toString());

		        if (paramLists4Props.getRowCount() > 0) {
		        	paramListId = paramLists4Props.getValue(0, "paramlistid");
		        	paramListVersionId = paramLists4Props.getValue(0, "paramlistversionid");
		        	dataSet4Props = paramLists4Props.getValue(0, "dataset");
		        }
				

				logMessage("Param List Id = " + paramListId);
				logMessage("Param List Version Id = " + paramListVersionId);
				

				sampleAndParamListId.put(sampleId, paramListId);
				sampleAndParamListVersionId.put(sampleId, paramListVersionId);
				
			} else {
				paramListId = sampleAndParamListId.get(sampleId);
				paramListVersionId = sampleAndParamListVersionId.get(sampleId);
				
			}
			
			//Extracting Date
			String[] columns1 = rows[3].split(",");
			String testdate = columns1[1].trim();
			dsResults.addRow();
			dsResults.setValue(0, "sdcid", "Sample");
			dsResults.setValue(0, "keyid1", sampleId);
			dsResults.setValue(0, "paramlistid", paramListId);
			dsResults.setValue(0, "paramlistversionid", paramListVersionId);
			dsResults.setValue(0, "variantid", "TPA Microtrac");
			dsResults.setValue(0, "instrumentfield", "Test Date");
			dsResults.setValue(0, "dataset", dataSet4Props);
			dsResults.setValue(0, "replicateid", "1");
			dsResults.setValue(0, "value", testdate);
			logMessage("Test Date = " + testdate);
			
			//Extracting Time
			String[] columns2 = rows[4].split(",");
			String testtime = columns2[1].trim();
			dsResults.addRow();
			dsResults.setValue(1, "sdcid", "Sample");
			dsResults.setValue(1, "keyid1", sampleId);
			dsResults.setValue(1, "paramlistid", paramListId);
			dsResults.setValue(1, "paramlistversionid", paramListVersionId);
			dsResults.setValue(1, "variantid", "TPA Microtrac");
			dsResults.setValue(1, "instrumentfield", "Test Time");
			dsResults.setValue(1, "dataset", dataSet4Props);
			dsResults.setValue(1, "replicateid", "1");
			dsResults.setValue(1, "value", testtime);
			logMessage("Test Time = " + testtime);
			
			//Extracting MV(um)
			String[] columns3 = rows[8].split(",");
			String mvum = columns3[2].trim();
			dsResults.addRow();
			dsResults.setValue(2, "sdcid", "Sample");
			dsResults.setValue(2, "keyid1", sampleId);
			dsResults.setValue(2, "paramlistid", paramListId);
			dsResults.setValue(2, "paramlistversionid", paramListVersionId);
			dsResults.setValue(2, "variantid", "TPA Microtrac");
			dsResults.setValue(2, "instrumentfield", "MV");
			dsResults.setValue(2, "dataset", dataSet4Props);
			dsResults.setValue(2, "replicateid", "1");
			dsResults.setValue(2, "value", mvum);
			logMessage("MV = " + mvum);
			
			//Extracting Percentile 1 
			String[] columns4 = rows[25].split(",");
			String percentile1 = "Percentile " + columns4[1].trim();
			String percentile1value = columns4[2].trim();
			if(!percentile1.equals("Percentile ")) {
				dsResults.addRow();
				dsResults.setValue(3, "sdcid", "Sample");
				dsResults.setValue(3, "keyid1", sampleId);
				dsResults.setValue(3, "paramlistid", paramListId);
				dsResults.setValue(3, "paramlistversionid", paramListVersionId);
				dsResults.setValue(3, "variantid", "TPA Microtrac");
				dsResults.setValue(3, "instrumentfield",percentile1);
				dsResults.setValue(3, "dataset", dataSet4Props);
				dsResults.setValue(3, "replicateid", "1");
				dsResults.setValue(3, "value", percentile1value);
				logMessage(percentile1 + " = " + percentile1value);
			}
			
			//Extracting Percentile 2
			String[] columns5 = rows[26].split(",");
			String percentile2 = "Percentile " + columns5[1].trim();
			String percentile2value = columns5[2].trim();
			if(!percentile2.equals("Percentile ")) {
				dsResults.addRow();
				dsResults.setValue(4, "sdcid", "Sample");
				dsResults.setValue(4, "keyid1", sampleId);
				dsResults.setValue(4, "paramlistid", paramListId);
				dsResults.setValue(4, "paramlistversionid", paramListVersionId);
				dsResults.setValue(4, "variantid", "TPA Microtrac");
				dsResults.setValue(4, "instrumentfield",percentile2);
				dsResults.setValue(4, "dataset", dataSet4Props);
				dsResults.setValue(4, "replicateid", "1");
				dsResults.setValue(4, "value", percentile2value);
				logMessage(percentile2 + " = " + percentile2value);
			}
			
			//Extracting Percentile 3
			String[] columns6 = rows[27].split(",");
			String percentile3 = "Percentile " + columns6[1].trim();
			String percentile3value = columns6[2].trim();
			if(!percentile3.equals("Percentile ")) {
				dsResults.addRow();
				dsResults.setValue(5, "sdcid", "Sample");
				dsResults.setValue(5, "keyid1", sampleId);
				dsResults.setValue(5, "paramlistid", paramListId);
				dsResults.setValue(5, "paramlistversionid", paramListVersionId);
				dsResults.setValue(5, "variantid", "TPA Microtrac");
				dsResults.setValue(5, "instrumentfield",percentile3);
				dsResults.setValue(5, "dataset", dataSet4Props);
				dsResults.setValue(5, "replicateid", "1");
				dsResults.setValue(5, "value", percentile3value);
				logMessage(percentile3 + " = " + percentile3value);
			}
			
			//Extracting Percentile 4
			String[] columns7 = rows[28].split(",");
			String percentile4 = "Percentile " + columns7[1].trim();
			String percentile4value = columns7[2].trim();
			if(!percentile4.equals("Percentile ")) {
				dsResults.addRow();
				dsResults.setValue(6, "sdcid", "Sample");
				dsResults.setValue(6, "keyid1", sampleId);
				dsResults.setValue(6, "paramlistid", paramListId);
				dsResults.setValue(6, "paramlistversionid", paramListVersionId);
				dsResults.setValue(6, "variantid", "TPA Microtrac");
				dsResults.setValue(6, "instrumentfield",percentile4);
				dsResults.setValue(6, "dataset", dataSet4Props);
				dsResults.setValue(6, "replicateid", "1");
				dsResults.setValue(6, "value", percentile4value);
				logMessage(percentile4 + " = " + percentile4value);
			}
			
			//Extracting Percentile 5
			String[] columns8 = rows[29].split(",");
			String percentile5 = "Percentile " + columns8[1].trim();
			String percentile5value = columns8[2].trim();
			if(!percentile5.equals("Percentile ")) {
				dsResults.addRow();
				dsResults.setValue(7, "sdcid", "Sample");
				dsResults.setValue(7, "keyid1", sampleId);
				dsResults.setValue(7, "paramlistid", paramListId);
				dsResults.setValue(7, "paramlistversionid", paramListVersionId);
				dsResults.setValue(7, "variantid", "TPA Microtrac");
				dsResults.setValue(7, "instrumentfield",percentile5);
				dsResults.setValue(7, "dataset", dataSet4Props);
				dsResults.setValue(7, "replicateid", "1");
				dsResults.setValue(7, "value", percentile5value);
				logMessage(percentile5 + " = " + percentile5value);
			}
			
			//Extracting Percentile 6
			String[] columns9 = rows[30].split(",");
			String percentile6 = "Percentile " + columns9[1].trim();
			String percentile6value = columns9[2].trim();
			if(!percentile6.equals("Percentile ")) {
				dsResults.addRow();
				dsResults.setValue(8, "sdcid", "Sample");
				dsResults.setValue(8, "keyid1", sampleId);
				dsResults.setValue(8, "paramlistid", paramListId);
				dsResults.setValue(8, "paramlistversionid", paramListVersionId);
				dsResults.setValue(8, "variantid", "TPA Microtrac");
				dsResults.setValue(8, "instrumentfield",percentile6);
				dsResults.setValue(8, "dataset", dataSet4Props);
				dsResults.setValue(8, "replicateid", "1");
				dsResults.setValue(8, "value", percentile6value);
				logMessage(percentile6 + " = " + percentile6value);
			}
			
			//Extracting Percentile 7
			String[] columns10 = rows[31].split(",");
			String percentile7 = "Percentile " + columns10[1].trim();
			String percentile7value = columns10[2].trim();
			if(!percentile7.equals("Percentile ")) {
				dsResults.addRow();
				dsResults.setValue(9, "sdcid", "Sample");
				dsResults.setValue(9, "keyid1", sampleId);
				dsResults.setValue(9, "paramlistid", paramListId);
				dsResults.setValue(9, "paramlistversionid", paramListVersionId);
				dsResults.setValue(9, "variantid", "TPA Microtrac");
				dsResults.setValue(9, "instrumentfield",percentile7);
				dsResults.setValue(9, "dataset", dataSet4Props);
				dsResults.setValue(9, "replicateid", "1");
				dsResults.setValue(9, "value", percentile7value);
				logMessage(percentile7 + " = " + percentile7value);
			}
			
			//Extracting Percentile 8
			String[] columns11 = rows[32].split(",");
			String percentile8 = "Percentile " + columns11[1].trim();
			String percentile8value = columns11[2].trim();
			if(!percentile8.equals("Percentile ")) {
				dsResults.addRow();
				dsResults.setValue(10, "sdcid", "Sample");
				dsResults.setValue(10, "keyid1", sampleId);
				dsResults.setValue(10, "paramlistid", paramListId);
				dsResults.setValue(10, "paramlistversionid", paramListVersionId);
				dsResults.setValue(10, "variantid", "TPA Microtrac");
				dsResults.setValue(10, "instrumentfield",percentile8);
				dsResults.setValue(10, "dataset", dataSet4Props);
				dsResults.setValue(10, "replicateid", "1");
				dsResults.setValue(10, "value", percentile8value);
				logMessage(percentile8 + " = " + percentile8value);
			}
			
			//Extracting Percentile 9
			String[] columns12 = rows[33].split(",");
			String percentile9 = "Percentile " + columns12[1].trim();
			String percentile9value = columns12[2].trim();
			if(!percentile9.equals("Percentile ")) {
				dsResults.addRow();
				dsResults.setValue(11, "sdcid", "Sample");
				dsResults.setValue(11, "keyid1", sampleId);
				dsResults.setValue(11, "paramlistid", paramListId);
				dsResults.setValue(11, "paramlistversionid", paramListVersionId);
				dsResults.setValue(11, "variantid", "TPA Microtrac");
				dsResults.setValue(11, "instrumentfield",percentile9);
				dsResults.setValue(11, "dataset", dataSet4Props);
				dsResults.setValue(11, "replicateid", "1");
				dsResults.setValue(11, "value", percentile9value);
				logMessage(percentile9 + " = " + percentile9value);
			}
			
			//Extracting Percentile 10
			String[] columns13 = rows[34].split(",");
			String percentile10 = "Percentile " + columns13[1].trim();
			String percentile10value = columns13[2].trim();
			if(!percentile10.equals("Percentile ")) {
				dsResults.addRow();
				dsResults.setValue(12, "sdcid", "Sample");
				dsResults.setValue(12, "keyid1", sampleId);
				dsResults.setValue(12, "paramlistid", paramListId);
				dsResults.setValue(12, "paramlistversionid", paramListVersionId);
				dsResults.setValue(12, "variantid", "TPA Microtrac");
				dsResults.setValue(12, "instrumentfield",percentile10);
				dsResults.setValue(12, "dataset", dataSet4Props);
				dsResults.setValue(12, "replicateid", "1");
				dsResults.setValue(12, "value", percentile10value);
				logMessage(percentile10 + " = " + percentile10value);
			}
			
			//Extracting DB Rec#
			String[] columns14 = rows[5].split(",");
			String dbrec = columns14[1].trim();
			dsResults.addRow();
			dsResults.setValue(13, "sdcid", "Sample");
			dsResults.setValue(13, "keyid1", sampleId);
			dsResults.setValue(13, "paramlistid", paramListId);
			dsResults.setValue(13, "paramlistversionid", paramListVersionId);
			dsResults.setValue(13, "variantid", "TPA Microtrac");
			dsResults.setValue(13, "instrumentfield", "DB Rec#");
			dsResults.setValue(13, "dataset", dataSet4Props);
			dsResults.setValue(13, "replicateid", "1");
			dsResults.setValue(13, "value", dbrec);
			logMessage("DB Rec# = " + dbrec);
						
			//END TPA
			
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

    private PropertyList getMaxDataSetNumber(String sampleId, String instrFieldID) {
        PropertyList pl = new PropertyList();
        StringBuffer sql = new StringBuffer();
        sql.append("select sd.SOURCEWORKITEMID, sd.SOURCEWORKITEMINSTANCE, max(sd.dataset) maxdataset");
        sql.append(" from sdidata sd");
        sql.append(" where sd.sdcid = 'Sample' and sd.variantid = 'TPA Microtrac' ");
        sql.append(" and sd.keyid1 = '").append(sampleId).append("' ");
        sql.append(" group by sd.sourceworkitemid, sd.sourceworkiteminstance order by sd.SOURCEWORKITEMINSTANCE desc");

        DataSet maxdata = getQueryProcessor().getSqlDataSet(sql.toString());
        pl.setProperty("maxdataset", maxdata.getValue(0, "maxdataset"));
        pl.setProperty("sourceworkitemid", maxdata.getValue(0, "sourceworkitemid"));
        pl.setProperty("sourceworkiteminstance", maxdata.getValue(0, "sourceworkiteminstance"));
        return pl;
    }

   
}
