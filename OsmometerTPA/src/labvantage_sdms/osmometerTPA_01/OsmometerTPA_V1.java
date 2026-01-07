package labvantage_sdms.osmometerTPA_01;

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

// TPA Osmotech XT parsing for whole database export

public class OsmometerTPA extends BaseAttachmentHandler {
    private static final String SAMPLE_ID_COLUMN = "Sample ID";
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
            //logMessage("Instrument ID = " + instrumentId);
            //logMessage("Data Capture ID = " + datacaptureId);

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
        String fieldForNullCheck = "mOsm/kg";
        String analystidFieldName = "User ID";
        logMessage("instrumentId = " + instrumentId);
        //logMessage("fieldForNullCheck = " + fieldForNullCheck);
        //logMessage("analystidFieldName = " + analystidFieldName);

        String sampleId = "", paramListId = "", paramListVersionId = "", variantId = "";
        String replicateInput = "1";
        String analystIdInFile = "";

        String tempLine = "", instrumentField = "", analystId = "",
                isSampleValid = "", sampleCreatedBy = "";
        String sourceWorkItemId = "", sourceWorkItemInstance = "";
        String line;
        String[] lineSplits, sampleUserSeqDataSet;

        int dsRow = -1, dataSetInput = 0, samplePosition = 0, lengthOfArray = 0, analystPosition = 0, availableDataSet = 0, maxDataSet = 0;
        boolean isRawDataHeaderFound = false;

        //Main HashMaps needed for Parsing
        Map<String, Integer> paramNameAndPosition = new HashMap<String, Integer>();
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
        Map<String, String> instrumentFields = sdmsDao.getInstrumentFields(instrumentId);
        //logMessage("Instrument Fields = " + sdmsDao.getInstrumentFields(instrumentId));

        //Get All Valid Users
        Map<String, String> validUsers = sdmsDao.getAllUsers();
        BufferedReader reader = null;

        DataSet dsResults = new DataSet();
        commonUtil.addRequiredColumnsToResultSet(dsResults);
        int testNo = 0;

        try {
            //Reading CSV file
            InputStream bomStrippedStream = removeUTF8BOM(inputStream);
        	reader = new BufferedReader(new InputStreamReader(bomStrippedStream));

			// Step 1: Read all lines into a list
			List<String> allLines = new ArrayList<>();
			while ((line = reader.readLine()) != null) {
				if (!StringUtils.isBlank(line)) {
					allLines.add(line);
				}
			}

			// Step 2: Find header line and map column positions
			String headerLine = null;
			int headerIndex = -1;
			for (int i = 0; i < allLines.size(); i++) {
				if (allLines.get(i).toLowerCase().contains(SAMPLE_ID_COLUMN.toLowerCase())) {
					headerLine = allLines.get(i);
					headerIndex = i;
					break;
				}
			}

			if (headerLine == null) {
				throw new SapphireException("Header line with '" + SAMPLE_ID_COLUMN + "' not found in the file.");
			}

			samplePosition = getParamNameAndPosition(paramNameAndPosition, headerLine);

			// Step 3: Get first data line (excluding header)
			List<String> dataLines = new ArrayList<>();
			for (int i = headerIndex + 1; i < allLines.size(); i++) {
				dataLines.add(allLines.get(i));
			}
			int endIndex = Math.min(dataLines.size(), 1);
			List<String> firstLine = dataLines.subList(0, endIndex);

			// Step 4: Now parse just the first line
			for (String dataLine : firstLine) {
				testNo += 1;
				logMessage("Test No. " + testNo);
				logMessage("Line No. " + testNo + " = " + dataLine);

				tempLine = dataLine.replaceAll("[µ~`^|\\[\\]\\{\\}\"\\\\]","");
				lineSplits = StringUtil.split(tempLine, FIELD_SEPARATOR, true);

                logMessage("Getting Sample Details");
                //get the Sample details by splitting data from sample ID field
                sampleUserSeqDataSet = lineSplits[samplePosition].trim().split(";");
                lengthOfArray = sampleUserSeqDataSet.length;

                //Separates values in sample id column and assigns values accordingly
                logMessage("Separating values in sample id column and assigning values accordingly");
                switch (lengthOfArray) {
                    // Case 1 only gets sample Id
                    case 1:
                        sampleId = sampleUserSeqDataSet[0].trim();
                        dataSetInput = 1; //sdmsDAO.getDataSetNumber(sampleId, instrumentId, field_for_null_check);
                        replicateInput = "1";
                        logMessage("Completed switch(lengthOfArray) case 1");
                        break;
                    case 2:
                        sampleId = sampleUserSeqDataSet[0].trim();
                        dataSetInput = Integer.parseInt(sampleUserSeqDataSet[1].trim());
                        replicateInput = "1";
                        logMessage("Completed switch(lengthOfArray) case 2");
                        break;
                    case 3:
                        sampleId = sampleUserSeqDataSet[0].trim();
                        dataSetInput = Integer.parseInt(sampleUserSeqDataSet[1].trim());
                        replicateInput = sampleUserSeqDataSet[2].trim();
                        logMessage("Completed switch(lengthOfArray) case 3");
                        break;
                    default:
                        break;
                }

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
                    sourceWorkItemId = maxDatlengthOfArrayaSetDetails.getProperty("sourceworkitemid");
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
                } else {
                    continue;
                }

                //Get Analyst Id from File
                logMessage("Getting Analyst Id From File");
                analystPosition = paramNameAndPosition.get(analystidFieldName);
                analystIdInFile = lineSplits[analystPosition];

                if(!StringUtils.isBlank(analystIdInFile) && validUsers.get(analystIdInFile.toLowerCase()) != null){
                    analystId = validUsers.get(analystIdInFile.toLowerCase());
                } else {
                    analystId = sampleCreatedBy;
                }
                logMessage("Analyst Position = " + analystPosition);
                logMessage("Analyst ID in File = " + analystIdInFile);
                logMessage("Analyst ID = " + analystId);

                logMessage("Executing Map.Entry For-Loop");
                for(Map.Entry<String, Integer> entry : paramNameAndPosition.entrySet()) {
                    instrumentField = instrumentFields.get(entry.getKey().toLowerCase());
                    
                    //get the parameterlist details
                    if(StringUtils.isBlank(instrumentField)) {
                    	continue;
                    }
                        
                    PropertyList properties = sdmsDao.getParamListWithoutDataSet(sampleId, instrumentId, instrumentField);

                    paramListId = properties.getProperty("paramlistid");
                    paramListVersionId = properties.getProperty("paramlistversionid");
                    variantId = properties.getProperty("variantid");

                    //logMessage("Param List Id = " + paramListId);
                    //logMessage("Param List Version Id = " + paramListVersionId);
                    //logMessage("Variant Id = " + variantId);

                    if(StringUtils.isBlank(paramListId)){
                        logMessage("Missing Param List ID for sampleId = " + sampleId + ", instrumentfield = " + instrumentField);
                    	continue;
                    }
                    dsRow = dsResults.addRow();
                    dsResults.setValue(dsRow, "sdcid", "Sample");
                    dsResults.setValue(dsRow, "keyid1", sampleId);
                    dsResults.setValue(dsRow, "paramlistid", paramListId);
                    dsResults.setValue(dsRow, "paramlistversionid", paramListVersionId);
                    dsResults.setValue(dsRow, "variantid", variantId);
                    dsResults.setValue(dsRow, "instrumentfield", instrumentField);
                    dsResults.setValue(dsRow, "dataset", availableDataSet + "");
                    dsResults.setValue(dsRow, "replicateid", replicateInput);
                    //Set value to NA if blank
                    String rawValue = "";
                    int valueIndex = entry.getValue();
                    if (valueIndex < lineSplits.length) {
                        rawValue = lineSplits[valueIndex].trim();
                    }
                    if (StringUtils.isBlank(rawValue)) {
                        rawValue = "NA";
                    }
                    dsResults.setValue(dsRow, "value", rawValue);
                    dsResults.setValue(dsRow, "s_analystid", analystId);
                }
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

    private InputStream removeUTF8BOM(InputStream is) throws IOException {
        PushbackInputStream pushbackInputStream = new PushbackInputStream(is, 3);
        byte[] bom = new byte[3];
        int bytesRead = pushbackInputStream.read(bom, 0, bom.length);

        if (bytesRead == 3) {
            if (!(bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF)) {
                pushbackInputStream.unread(bom, 0, bytesRead);
            }
        } else if (bytesRead > 0) {
            pushbackInputStream.unread(bom, 0, bytesRead);
        }

        return pushbackInputStream;
    }

	private int getParamNameAndPosition(Map<String, Integer> paramNameAndPosition, String line) {
	    String lineWithoutSpecialChars = line.replaceAll("[µ~`^|\\[\\]\\{\\}\"\\\\]", "");
	    String[] headerSplits = StringUtil.split(lineWithoutSpecialChars, FIELD_SEPARATOR, true);
	    String paramName = "";
	
	    //logMessage("lineWithoutSpecialChars = " + lineWithoutSpecialChars);
	    //logMessage("headerSplits = " + Arrays.toString(headerSplits));
	    //logMessage("headerSplits.length = " + headerSplits.length);
	
	    int samplePos = -1;
	    for(int arrayIndex = 0; arrayIndex < headerSplits.length; arrayIndex++){
	        paramName = headerSplits[arrayIndex].trim();
	        logMessage("paramName = " + paramName);
	
	        if(SAMPLE_ID_COLUMN.equalsIgnoreCase(paramName)){
	            samplePos = arrayIndex;
	        } else {
	            paramNameAndPosition.put(paramName, arrayIndex);
	        }
	    }
	
	    return samplePos;
	}
}
