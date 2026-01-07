package com.labvantage.training.sdms.utl;
/**
 * Copyright (c) 2020 LABVANTAGE.  All rights reserved.
 * <p>
 * This software and documentation is the confidential and proprietary
 * information of LabVantage. ("Confidential Information").
 * You shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement you
 * entered into with LabVantage.
 * <p>
 * If you are not authorized by LabVantage to utilize this
 * software and/or documentation, you must immediately discontinue any
 * further use or viewing of this software and documentation.
 * Violaters will be prosec uted to the fullest extent of the law by
 * LabVantage.
 * <p>
 * Developed by LABVANTAGE Solutions.
 * 265 Davidson Avenue Suite 220
 * Somerset, NJ 08873 USA
 * <p>
 */

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.labvantage.training.sdms.dao.SDMSDao;
import com.labvantage.sapphire.BaseCustom;
import com.labvantage.sapphire.FileUtil;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import sapphire.accessor.*;
import sapphire.action.EditDataItem;
import sapphire.util.*;

import com.labvantage.sapphire.modules.sdms.util.ResultDataGrid;
import com.labvantage.sapphire.services.ConnectionInfo;
import sapphire.SapphireException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sapphire.util.ResultDataGrid.CoreColumns;
import sapphire.xml.PropertyList ;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;


public class SDMSCommonUtil extends BaseCustom {

    SDMSDao dao = new SDMSDao(this.getConnectionId());
    private ActionProcessor ap = null;


    public SDMSCommonUtil(SDMSDao dao) {

        this.ap = getActionProcessor();
        this.dao =dao;
    }


    public static String extractStringByPatternGroup(String str,
                                                     String pattern, int group) {

        String sReturnValue = "";

        Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(str);

        if (m.matches()) {

            if (m.groupCount() >= group) {
                sReturnValue = m.group(group);
            }
        }

        return sReturnValue;
    }

    /**
     * Formats a date string in MM/dd/yyyy HH:mm:ss
     *
     * @param date
     *            Date to be formatted
     * @param inputdatepattern
     *            Input date pattern
     * @return the transformed date string
     */
    public  static String getFormatedDate(String date, String inputdatepattern) {

        String sFormattedDate = date;

        if (date != null && !date.equals("") && inputdatepattern != null
                && !inputdatepattern.equals("")) {

            Date dDate = null;

            SimpleDateFormat sdf = new SimpleDateFormat(inputdatepattern);

            try {

                dDate = sdf.parse(date);

            } catch (ParseException e) {
				/*System.out.println("Warn: Failed to parse date: " + date
						+ " using pattern " + inputdatepattern + ". "
						+ e.getMessage());*/
                dDate = null;
            }

            if (dDate != null) {

                SimpleDateFormat sdf2 = new SimpleDateFormat(
                        "MM/dd/yyyy HH:mm:ss");

                sFormattedDate = sdf2.format(dDate);

            }
        }

        return sFormattedDate;
    }

    public static String getDateTime() {
        Calendar cal = Calendar.getInstance();
        Date date = cal.getTime();
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String formattedDate = dateFormat.format(date);
        return formattedDate;
    }

    /**
     * This method is introduced to get the PropertyValue Map from Property Tree
     *
     * @param propertyTree  Property Tree as String
     *
     * @return      Required Map with Property & Value is returned
     */
    public Map<String, String> getPropertyValueMapFromPropertyTree(String propertyTree){
        Map<String, String> tagIdAndValueMap = new HashMap<String, String>();

        try {
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document xmlDocument = documentBuilder.parse(new InputSource(new StringReader(propertyTree)));
            Node node = null;
            Element element = null;

            NodeList properties = xmlDocument.getElementsByTagName("property");
            for(int i = 0; i < properties.getLength(); i++){
                node = properties.item(i);
                if(node == null || (node.getNodeType() != Node.ELEMENT_NODE)){
                    continue;
                }

                element = (Element) node;
                if(element!= null){
                    tagIdAndValueMap.put(element.getAttribute("id"), element.getTextContent());
                }
            }

        } catch (ParserConfigurationException ex) {
            ex.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return tagIdAndValueMap;
    }
    /**
     *  This method is introduced to get the PropertyValue Map from Property Tree , nodeId, propertyName
     * @param propertyTree  Property Tree as String
     * @param nodeId  node id as String.
     * @param propertyName  Property Name as String
     * @return      Required Map with Property & Value is returned
     */

    public String getPropertyValueFromPropertyTree(String propertyTree, String nodeId, String propertyName){
        String propertyValue = "";


        try {
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document xmlDocument = documentBuilder.parse(new InputSource(new StringReader(propertyTree)));
            Node node = null;
            Element element = null;

            NodeList properties = xmlDocument.getElementsByTagName("node");
            for(int i = 0; i < properties.getLength(); i++){
                node = properties.item(i);
                if(node == null || (node.getNodeType() != Node.ELEMENT_NODE)){
                    continue;
                }

                element = (Element) node;
                if(element!= null && nodeId.equalsIgnoreCase(element.getAttribute("id"))){
                    propertyValue = getPropertyValue(element.getElementsByTagName("property"), propertyName);
                    break;
                }
            }

        } catch (ParserConfigurationException ex) {
            ex.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return propertyValue;
    }

    /**
     *  This method is introduced to get the PropertyValue from NodeList,  propertyName
     * @param property  Node list as NodeList
     * @param propertyName  Property Name as String
     * @return      Required Property  Value
     */
    private String getPropertyValue(NodeList property, String propertyName) {
        String propertyValue = "";
        Node node = null;
        Element element = null;
        for(int i = 0; i < property.getLength(); i++){
            node = property.item(i);
            if(node == null || (node.getNodeType() != Node.ELEMENT_NODE)){
                continue;
            }

            element = (Element) node;
            if(element!= null && propertyName.equalsIgnoreCase(element.getAttribute("id"))){
                propertyValue = element.getTextContent();
                break;
            }
        }

        return propertyValue;
    }

    /**
     * This method will get the OS Path & UNC Path defined at NetworkFileRepository and concatenate instrumentId with the paths to create Instrument wise directory for intermediate csv files
     *
     * @param configurationProcessor        ConfigurationProcessor object
     * @param instrumentId                  Instrument Id
     * @return      Instrument wise directory for intermediate csv files
     * @throws SapphireException
     */
    public PropertyList getIntermediateCSVStoragePath(ConfigurationProcessor configurationProcessor, String instrumentId) throws SapphireException {
        PropertyList storagePaths = new PropertyList();
        String osPath = "", uncPath = "";
        try {
            osPath = configurationProcessor.getPolicy("NetworkFileRepository", "os_path").getProperty("locationpath");
            uncPath = configurationProcessor.getPolicy("NetworkFileRepository", "unc_path").getProperty("locationpath");

            osPath = getFinalPath(osPath, instrumentId, true);
            uncPath = getFinalPath(uncPath, instrumentId, false);

            storagePaths.setProperty("ospath", osPath);
            storagePaths.setProperty("uncpath", uncPath);
        } catch (SapphireException e) {
            throw new SapphireException("Error occured during getting Intermediate CSV Path!!!");
        }

        return storagePaths;
    }

    /**
     * This method will get the OS Path & UNC Path defined at NetworkFileRepository and concatenate instrumentId with the paths to create Instrument wise directory for intermediate csv files
     *
     * @param instrumentId   Instrument Id
     * @return      Instrument wise directory for intermediate csv files
     */
    public PropertyList getIntermediateCSVStoragePath(String instrumentId) {
        PropertyList storagePaths = new PropertyList();
        String osPath = "", uncPath = "";
            String networkFileRepository = dao.getPropertyTreeValue("NetworkFileRepository");
            osPath = getPropertyValueFromPropertyTree(networkFileRepository, "os_path", "locationpath");
            uncPath = getPropertyValueFromPropertyTree(networkFileRepository, "unc_path", "locationpath");
            osPath = getFinalPath(osPath, instrumentId, true);
            uncPath = getFinalPath(uncPath, instrumentId, false);

            storagePaths.setProperty("ospath", osPath);
            storagePaths.setProperty("uncpath", uncPath);

        return storagePaths;
    }

    /**
     *  This method will create a path from combining the firstPart ,secondPart
     * @param firstPart  First folder location of the path
     * @param secondPart   folder/folders inside the firstPath.
     * @param isRequiredToCreate to create directories if needed
      @return	String the newly created path .
     */
    public static String getFinalPath(String firstPart, String secondPart, boolean isRequiredToCreate){
        String finalPath = null;
        if(firstPart!= null){
            if(firstPart.contains("\\")){
                finalPath = firstPart + "\\" + secondPart + "\\";
            }else{
                finalPath = firstPart + "/" + secondPart + "/";
            }
        }

        if(isRequiredToCreate && finalPath!= null){
            File fileParentPath = new File(finalPath);
            if(!fileParentPath.exists()){
                //Create the directory if not exists
                fileParentPath.mkdirs();
            }
        }

        return finalPath;
    }



    /**
     * Checks whether the string is numeric or not
     *
     * @param str
     *            String to test
     * @return true if numeric else false
     */
    public static boolean isNumericString(String str) {

        boolean b = false;

        String sRegex = "(\\+?)(-?)(\\d+)(,?)(.?)(\\d*)";

        Pattern pattern = Pattern.compile(sRegex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(str);

        if (matcher.matches())
            b = true;

        return b;
    }

    /**
     *  This method is introduced to check if the given str matches the pattern
     * @param str  String to be matched
     * @param pattern  Pattern to match
     * @return      true/false
     */
    public static boolean isPatternMatch(String str, String pattern) {

        Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(str);

        return m.matches();

    }

    /*public static String getInstrumentRootPath(TalendUtil ws,String instrumentid) {
        System.out.println("In getInstrumentRootPath");
        StringBuffer sql = new StringBuffer();
        sql.append(" select collectorvaluetree from instrument where");
        sql.append(" instrumentid = '");
        sql.append(instrumentid).append("' ");
        System.out.println("sql ="+ sql);

        DataSet value = ws.getSQLDataSet(sql.toString());
        System.out.println( "value =" +  value.getValue(0, "collectorvaluetree"));

        if(value.getRowCount() > 0)
           return value.getValue(0, "collectorvaluetree");



      return "";
    }
    */


    /**
     *  This method will create a path from combining the storagePath ,instrumentId  and folder
     * @param storagePath  First folder location of the path
     * @param instrumentId   folder with this name wil be created in storagePath.
     * @param folder subfolder.
     @return	String the newly created path .
     */
    public  String createDirectory(String storagePath,String instrumentId, String folder) {

        String finalPath =null;
        if(storagePath.contains("\\")){
            finalPath = storagePath + "\\" + instrumentId + "\\"+folder +"\\";
        }else{
            finalPath = storagePath + "/" + instrumentId + "/"+folder +"/";
        }

        //String finalPath = storagePath + "\\"+instrumentId+"\\"+folder +"\\";
        System.out.println("finalPath =" + finalPath);
        File dir = new File(finalPath);
        if (!dir.exists()) {
            dir.mkdirs();
            System.out.println("Created directory structure ");

        }
        return finalPath;
    }


    /**
     *  This method will get the instrument root folder as per configuration in the DB
     * @param instrumentID  First folder location of the path
     @return	String the root folder.
     */
    public  String getInstrumentRootFolder(String instrumentID) throws SapphireException {
        System.out.println("In getInstrumentRootFolder");
        HashMap props = new HashMap();
        props.put("instrumentid",instrumentID);
        props.put("instrumentRootFolder","");
        this.ap.processAction("SDMS_GetInstrumentRootFolder", "1", props);
        return props.get("instrumentRootFolder").toString();
    }

    public  String callSDMS_PGCalcScope(DataSet dsData ) throws SapphireException {

        PropertyList props = new PropertyList();

        // props.put("sdcid", dsData.getColumnValues( "sdcid", ";"));
        props.put("sdcid", dsData.getValue(0,"sdcid"));//dsData.getColumnValues( "sdcid", ";"));
        props.put("keyid1",dsData.getColumnValues( "keyid1", ";"));
        props.put("paramid", dsData.getColumnValues( "paramid", ";"));
        props.put("value", dsData.getColumnValues( "value", ";"));
        props.put("replicateid", dsData.getColumnValues( "replicateid", ";"));
        props.put("dataset", dsData.getColumnValues("dataset", ";"));
        props.put("paramlistid", dsData.getColumnValues( "paramlistid", ";"));
        props.put("paramlistversionid",  dsData.getColumnValues( "paramlistversionid", ";"));
        props.put("variantid",  dsData.getColumnValues( "variantid", ";"));
        props.put("notes",  dsData.getColumnValues( "notes", ";"));
        props.put("keyid2",  dsData.getColumnValues( "keyid2", ";"));
        props.put("keyid3",  dsData.getColumnValues( "keyid3", ";"));
        props.put( "calculatemodifiedtestsonly", "N" );

        dao.process_PGCalcScope(props);
        return (String)props.get("calculatemodifiedtestsonly");
    }


    /**
     *  This method will get the add all the required columns to the result DataSet.
     * @param resultSet  Dataset to which all the columns will be added.
     @return	.
     */
    public  void addRequiredColumnsToResultSet1(DataSet resultSet) {
        resultSet.addColumn("sdcid", DataSet.STRING);
        resultSet.addColumn("keyid1", DataSet.STRING);
        resultSet.addColumn("keyid2", DataSet.STRING);
        resultSet.addColumn("keyid3", DataSet.STRING);
        resultSet.addColumn("paramid", DataSet.STRING);
        resultSet.addColumn("paramtype", DataSet.STRING);
        resultSet.addColumn("value", DataSet.STRING);
        resultSet.addColumn("replicateid", DataSet.STRING);
        resultSet.addColumn("paramlistid", DataSet.STRING);
        resultSet.addColumn("paramlistversionid", DataSet.STRING);
        resultSet.addColumn("variantid", DataSet.STRING);
        resultSet.addColumn("dataset", DataSet.STRING);
        resultSet.addColumn("notes", DataSet.STRING);
        resultSet.addColumn("instrumentid", DataSet.STRING);
        resultSet.addColumn("s_analystid", DataSet.STRING);
        // JIRA #PG07735-6915. Displatunits is set to null. Commented below code
//        resultSet.addColumn("displayunits", DataSet.STRING);
        resultSet.addColumn("datacaptureid", DataSet.STRING);
    }


     public void addRequiredColumnsToResultSet(DataSet resultSet) {
         resultSet.addColumn("sdcid", DataSet.STRING);
         resultSet.addColumn("keyid1", DataSet.STRING);
         resultSet.addColumn("paramlistid", DataSet.STRING);
         resultSet.addColumn("paramlistversionid", DataSet.STRING);
         resultSet.addColumn("variantid", DataSet.STRING);
         resultSet.addColumn("dataset", DataSet.STRING);
         resultSet.addColumn("replicateid", DataSet.STRING);
         resultSet.addColumn("instrumentfield", DataSet.STRING);
         resultSet.addColumn("value", DataSet.STRING);
         resultSet.addColumn("notes", DataSet.STRING);
         resultSet.addColumn("s_analystid", DataSet.STRING);
         // JIRA #PG07735-6915. Displatunits is set to null. Commented below code
//         resultSet.addColumn("displayunits", DataSet.STRING);
    }

   /* public int getParamNameAndPosition(Row headerRow, Map<Integer, String> paramPositionAndName, Map<String, Integer> paramNameAndPosition) {
        int elementLabelPos = 0;
        String parameterName = null;
        for(int cellNo2 = 0; cellNo2<headerRow.getLastCellNum(); cellNo2++){
            parameterName = headerRow.getCell(cellNo2).getStringCellValue();
            if("Element Label".equalsIgnoreCase(parameterName.trim())){
                elementLabelPos = cellNo2;
            }
            if(!"Label".equalsIgnoreCase(parameterName)){
                paramPositionAndName.put(cellNo2, (parameterName.length() > 40 ? parameterName.substring(0, 40) : parameterName)); //Take only 40 characters if more than 40 chars found in the parameter name
//				System.out.println(cellNo2+" : "+parameterName);
                paramNameAndPosition.put((parameterName.length() > 40 ? parameterName.substring(0, 40) : parameterName), cellNo2);

            }
        }

        return elementLabelPos;
    }*/

    public  boolean isStrAlphabets(String str) {

        return str != null && str.matches("^[a-zA-Z]*$");
    }

    /**
     *
     * @param resultSet
     * @param properties
     * @param connectionInfo
     * @param sdcid
     * @return
     * @author Veena
     */
    public ResultDataGrid createResultGrid(DataSet resultSet, PropertyList properties, ConnectionInfo connectionInfo,String sdcid) {
        ResultDataGrid resultGrid = new ResultDataGrid(connectionInfo);
        ResultGridOptions resultGridOptions = new ResultGridOptions();
        resultGridOptions.setSdcId(sdcid);
        resultGridOptions.setAutoAddSDI(properties.getProperty("autoaddsdi").equals("") ? 
                ResultGridOptions.AutoAddSDI.NEVER : ResultGridOptions.AutoAddSDI.valueOf(properties.getProperty("autoaddsdi").toUpperCase()));
        resultGridOptions.setAutoAddWorkItem(properties.getProperty("autoaddsdiworkitem").equals("") ? 
                ResultGridOptions.AutoAddWorkItem.NEVER : ResultGridOptions.AutoAddWorkItem.valueOf(properties.getProperty("autoaddsdiworkitem").toUpperCase()));
        resultGridOptions.setAutoAddDataset(properties.getProperty("autoadddataset").equals("") ? 
                ResultGridOptions.AutoAddDataSet.NEVER : ResultGridOptions.AutoAddDataSet.valueOf(properties.getProperty("autoadddataset").toUpperCase()) );
        resultGridOptions.setAutoAddReplicate(properties.getProperty("autoaddreplicate").equals("") ? 
                ResultGridOptions.AutoAddReplicate.NEVER : ResultGridOptions.AutoAddReplicate.valueOf(properties.getProperty("autoaddreplicate").toUpperCase()));
        resultGridOptions.setAutoAddParameter(properties.getProperty("autoaddparameter").equals("") ? 
                ResultGridOptions.AutoAddParameter.NEVER : ResultGridOptions.AutoAddParameter.valueOf(properties.getProperty("autoaddparameter").toUpperCase()));
        resultGridOptions.setReleaseHandlingRule(properties.getProperty("releasehandlingrule").equals("") ? 
                ResultGridOptions.ReleaseHandlingRule.ERROR : ResultGridOptions.ReleaseHandlingRule.valueOf(properties.getProperty("releasehandlingrule").toUpperCase()));
        resultGridOptions.setAutoRelease("Y".equalsIgnoreCase(properties.getProperty("autorelease")));

        resultGridOptions.setDefaultDataSet(properties.getProperty("defaultdataset").equals("") ? 
                ResultGridOptions.DefaultDataSet.FIRST_AVAILABLE : ResultGridOptions.DefaultDataSet.valueOfDefaultDataSet(properties.getProperty("defaultdataset")));
        resultGridOptions.setDefaultReplicateId(properties.getProperty("defaultreplicate").equals("") ? 
                ResultGridOptions.DefaultReplicateId.FIRST_AVAILABLE : ResultGridOptions.DefaultReplicateId.valueOfDefaultReplicateId(properties.getProperty("defaultreplicate")));
        resultGridOptions.setApplyLock(false); // need to stop sending aplylock as false to allow data entry for multiple Tests at the same time.
        logger.debug("ApplyLock has been set to false in ResultGridOptions.");
        resultGridOptions.setMissingDataErrorHandling(properties.getProperty("missingdataerrorhandling").equals("") ?
                ResultGridOptions.MissingDataErrorHandling.IGNORE : ResultGridOptions.MissingDataErrorHandling.valueOf(properties.getProperty("missingdataerrorhandling").toUpperCase()));

        PropertyList enterdataitemprops = new PropertyList();

        /*
        // JIRA #PG07735-6898. Property is set depending on the property sent in the driver.
        if (properties.getProperty("calculatemodifiedtestsonly") == "Y") {
            enterdataitemprops.setProperty("calculatemodifiedtestsonly", "Y");
        } else {
            enterdataitemprops.setProperty("calculatemodifieddatasetsonly", "Y");
        }
        */

        HashMap<CoreColumns, String> resultFields = new HashMap<CoreColumns, String>();
        HashMap<String, String> additionalFields = new HashMap<String, String>();
        //int resultGridIndex = -1;

        for (int i = 0; i < resultSet.getRowCount(); ++i) {
            resultFields.put(CoreColumns.SDCID, resultSet.getValue(i, "sdcid"));
            resultFields.put(CoreColumns.KEYID1, resultSet.getValue(i, "keyid1"));
            resultFields.put(CoreColumns.KEYID2, resultSet.getValue(i, "keyid2"));
            resultFields.put(CoreColumns.KEYID3, resultSet.getValue(i, "keyid3"));
            resultFields.put(CoreColumns.PARAMLISTID, resultSet.getValue(i, "paramlistid"));
            resultFields.put(CoreColumns.PARAMLISTVERSIONID, resultSet.getValue(i, "paramlistversionid"));
            resultFields.put(CoreColumns.VARIANTID, resultSet.getValue(i, "variantid"));
            resultFields.put(CoreColumns.DATASET, resultSet.getValue(i, "dataset"));
            resultFields.put(CoreColumns.REPLICATEID, resultSet.getValue(i, "replicateid"));
            resultFields.put(CoreColumns.PARAMID, resultSet.getValue(i, "paramid"));
            resultFields.put(CoreColumns.PARAMTYPE, resultSet.getValue(i, "paramtype"));
            resultFields.put(CoreColumns.VALUE, resultSet.getValue(i, "value"));
            additionalFields.put("notes", resultSet.getValue(i, "notes"));
            additionalFields.put("s_analystid", resultSet.getValue(i, "s_analystid"));
            additionalFields.put("instrumentid", resultSet.getValue(i, "instrumentid"));

            resultGrid.addResult(resultFields, additionalFields);
            resultFields.clear();
            additionalFields.clear();
        }

        //add enterdataitem properties to resultgrid
        resultGridOptions.setEnterDataItemProperties(enterdataitemprops);
        resultGrid.setOptions(resultGridOptions);
        return resultGrid;
    }

    /**
     * This method will first save the resultgrid data and then invoke the EditDataItem action for the records present in the
     * resultSetForResultGrid dataset
     *
     * @param resultDataGrid            ResultDataGrid object on which save function to be performed
     * @param resultSetForResultGrid    DataSet object for which EditDataItem action to be invoked
     *
     * @param actionProcessor
     * @throws SapphireException        It will re-throw the exception which are thrown by the operation performed in this method
     * @author jaynals
     *
     */
    public void saveAndUpdateDataItem(ResultDataGrid resultDataGrid, DataSet resultSetForResultGrid, ActionProcessor actionProcessor) throws SapphireException {
        try {

        	
        //save the result with auto release false
        resultDataGrid.save();

        //call the EditDataItem to update the data
        invokeEditDataItem(resultSetForResultGrid, actionProcessor);
        }catch (Exception e){
            throw new SapphireException(e.getMessage());
            
        }
    }


    /**
     *  This method will get the add propsmatch as Y and sdcid as Sample to the DataSet .
     *  This will be used by resultGrid.
     * @param resultSetForResultGrid  Dataset which will be eventually used by ResultGrid.
     @return	.
     */
    private void invokeEditDataItem(DataSet resultSetForResultGrid, ActionProcessor actionProcessor) throws SapphireException {
        PropertyList actionProperties = new PropertyList();
        for(DataSet dataSet : resultSetForResultGrid.getSplitDataSets(1000)){
            for(String column : dataSet.getColumns()){
                if(column.equalsIgnoreCase("sdcid") || column.equalsIgnoreCase("value")){
                    continue;
                }
                actionProperties.setProperty(column.toLowerCase(), dataSet.getColumnValues(column.toLowerCase(), ";"));
            }
            try {
                actionProperties.setProperty("sdcid", "Sample");
                //actionProperties.setProperty("autorelease", "Y");
                actionProperties.setProperty("propsmatch", "Y");

                actionProcessor.processAction(EditDataItem.ID, EditDataItem.VERSIONID, actionProperties);
            } catch (ActionException actionException) {
                throw new SapphireException(actionException.getMessage());
            }

            actionProperties.clear();
        }
    }

    public void copyFileAndGenerateHashManifest(File source, String target) throws Exception {
        FileTransferOptions options = new FileTransferOptions();
        options.setValidateHashValue(true);
        options.setDeleteSourceOnSuccessfullTransfer(true);
        options.setReplaceTarget(true);
        File fp = new File(target);
        FileTransfer.safeFileTransfer(source, fp, options);
        long hasHash2 = options.getTargetHashValue();
        String mfFileName = StringUtil.replaceAll(target,".csv",".mf");
        if(target.contains("tsv")){
            mfFileName = StringUtil.replaceAll(target,".tsv",".mf");
        }
        PropertyList pl = new PropertyList();
        pl.setProperty("hash",hasHash2+"");
        Files.write(Paths.get(mfFileName), pl.toXMLString().getBytes());

    }

    public static boolean verifyHash(String fileName) throws Exception {

        BufferedReader reader =null;
        Logger.logInfo( "In verifyHash "+fileName );
        try {
            FileTransferOptions options = new FileTransferOptions();
            options.setValidateHashValue(true);
            long hash = FileTransfer.getHashValue(new File(fileName), options);
            System.out.println("File hash = " + hash);
            Logger.logInfo( "File hash "+hash );
            String mfFileName = StringUtil.replaceAll(fileName, ".csv", ".mf");
            reader = new BufferedReader(new FileReader(mfFileName));

            String line = null;
            StringBuilder xmlString = new StringBuilder();
            XPathFactory xpathFactory = XPathFactory.newInstance();
            XPath xpath = xpathFactory.newXPath();
            String xpathExpression = "";

            while ((line = reader.readLine()) != null)
                xmlString.append(line);

            xpathExpression = "/propertylist/property[contains(@id,'hash')]/text()";
            XPathExpression expr = xpath.compile(xpathExpression);
            Document document = getDocument(xmlString.toString());
            NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
            String nodeValue = nodes.item(0).getNodeValue();
            Logger.logInfo( "nodeValue "+nodeValue );
            System.out.println("comparison is " + nodeValue.compareTo(hash + ""));
            Logger.logInfo( "comparison is  "+nodeValue.compareTo(hash + "") );

            if (nodeValue != null && nodeValue.compareTo(hash + "") == 0)
                return true;
            else
                throw new Exception("hash value of intermediate filename does not match the earlier hash");

        }catch(Exception e){
            Logger.logInfo( "****verifyHash-ERROR: IO Error - " + "Exception: " + e.getMessage() );
            Logger.logInfo( " " );
            throw e;

        } finally {
            try {
                reader.close();
            }catch(Exception e){
                e.printStackTrace();
                throw e;
            }
         }



    }

    private static Document getDocument(String xmlStr) throws Exception
    {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        InputStream inputStream = new ByteArrayInputStream(xmlStr.getBytes());
        Document doc = builder.parse(inputStream);
        return doc;
    }




    public static String getUniqueValue(String colValues,String seperator) {
        TreeSet<String> uniqueSet = new TreeSet<String>();
        String valueList = "";
        String[] splitBySemiColon = colValues.split(";");
        for (int i = 0; i < splitBySemiColon.length; i++) {
            if (!splitBySemiColon[i].equalsIgnoreCase("")) {
                uniqueSet.add(splitBySemiColon[i]);
            }
        }

        for (String value : uniqueSet) {
            valueList += value + seperator;
        }
        if (valueList.endsWith(seperator)) {
            valueList = valueList.substring(0, valueList.length() - 1);
        }
        return valueList;
    }


    /**
     *  This method will get the add sinlge quotes so that the String can be used in queries.
     *  'a','ab' etc'.
     * @param colValues

     @return	String.
     */
    public static String addQuotesForEachValue(String colValues) {
        TreeSet<String> uniqueSet = new TreeSet<String>();
        String valueList = "";
        String[] splitBySemiColon = colValues.split(";");
        for (int i = 0; i < splitBySemiColon.length; i++) {
            if (!splitBySemiColon[i].equalsIgnoreCase("")) {
                uniqueSet.add(splitBySemiColon[i]);
            }
        }

        for (String value : uniqueSet) {
            valueList +="'"+ value +"'"+ ",";
        }
        if (valueList.endsWith(",")) {
            valueList = valueList.substring(0, valueList.length() - 1);
        }
        return valueList;
    }


    public void createFAILFile(String instrumentRootFolder, String originalName, String targetName) throws SapphireException {

        try{
            String storagePath =instrumentRootFolder;
            String failFolder = createDirectory(storagePath,"FAIL");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
            String filename = FilenameUtils.getBaseName(targetName) + "_"+sdf.format(new Date()).replace(".","")+ "." +FilenameUtils.getExtension(targetName);
            String path= FilenameUtils.concat(failFolder,filename);
            FileUtil.copyFile(new File(originalName),new File(path));
        } catch(Exception e){
            throw new SapphireException(e.getMessage());
        }


    }
    public  String getInstrumentID(String dataCaptureID) {
        String sql = " select instrumentid from datacapture where datacaptureid = '" + dataCaptureID +"'";
        DataSet dsInstrumnet = this.getQueryProcessor().getSqlDataSet(sql);
        if(dsInstrumnet.getRowCount() > 0)
            return dsInstrumnet.getValue(0, "instrumentid");
        return "";
    }


    public  String getCollectorStoragePath(String instrumentid) {
        String sql = "";
        sql += "select storagepathlocal  from sdmscollector coll join instrument ins on coll.sdmscollectorid = ins.sdmscollectorid and";
        sql += " ins.instrumentid = '"+ instrumentid +"'";
        DataSet value =  this.getQueryProcessor().getSqlDataSet(sql);
        if(value.getRowCount() > 0)
            return value.getValue(0, "storagepathlocal");
        return "";
    }

    /**
     *  This method get the root path of the instrument.
     *  'a','ab' etc'.
     * @param instID Instrument ID

     @return	String.
     */
    public  String getInstrumentRootPath(String instID) throws SapphireException {
        String sql = "select  collectorvaluetree  FROM instrument WHERE instrumentid='" + instID + "'";
        DataSet dS = this.getQueryProcessor().getSqlDataSet(sql, true);
        if (dS != null && dS.size() > 0) {
            String val = dS.getClob(0, "collectorvaluetree");
            XPathFactory xpathFactory = XPathFactory.newInstance();
            XPath xpath = xpathFactory.newXPath();
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            String xpathExpression = "";
            try {
                xpathExpression = "/propertylist/property[contains(@id,'instrumentremoteroot')]/text()";
                XPathExpression expr = xpath.compile(xpathExpression);
                DocumentBuilder builder = factory.newDocumentBuilder();
                InputStream inputStream = new ByteArrayInputStream(val.getBytes());
                Document doc = builder.parse(inputStream);
                NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
                String nodeValue = nodes.item(0).getNodeValue();
                if (nodeValue.contains("\\"))
                    nodeValue = nodeValue.substring(0, nodeValue.lastIndexOf("\\"));
                else
                    nodeValue = nodeValue.substring(0, nodeValue.lastIndexOf("/"));

                return nodeValue;
            } catch (XPathExpressionException e) {
                e.printStackTrace();
                throw new SapphireException(e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
                throw new SapphireException(e.getMessage());

            }

        }
        throw new SapphireException("Querry failed to get the instrumentroot folder information from instrument table");
    }


    public DataSet getResultSetForResultGrid(PropertyList propertyList, DataSet resultSet) {


        HashSet<String> uniqueIds = new HashSet<String>();
        String sampleIds = resultSet.getColumnValues("keyid1", ";");
        uniqueIds.addAll(Arrays.asList(sampleIds.split(";")));
        sampleIds = uniqueIds.toString();
        sampleIds = sampleIds.replaceAll(", ", "','");
        sampleIds = sampleIds.replaceAll("[\\[\\]]","");
        sampleIds = "'" + sampleIds + "'";
        uniqueIds.clear();

        String instrumentFields = resultSet.getColumnValues("instrumentfield", ";");
        uniqueIds.addAll(Arrays.asList(instrumentFields.split(";")));
        instrumentFields = uniqueIds.toString();
        instrumentFields = instrumentFields.replaceAll(", ", "','");
        instrumentFields = instrumentFields.replaceAll("[\\[\\]]","");
        instrumentFields = "'" + instrumentFields + "'";
        uniqueIds.clear();

        HashMap<String, String> paramDetails = dao.getParamDetailsForMultipleSamples(sampleIds, propertyList.getProperty("instrumentid"), instrumentFields);
        DataSet resultSetForResultGrid = new DataSet();
        addRequiredColumnsToResultSet1(resultSetForResultGrid);
        int rowNum = 0;
        for(int dsIndex = 0; dsIndex < resultSet.getRowCount(); dsIndex++){

            // Edited for PG07735-8235 for issue related to unique paramid and common instrumentfield mapping in PL
            // Construct the key using the same format as in getParamDetailsForMultipleSamples
            String key = resultSet.getValue(dsIndex, "paramlistid") + "_"
                    + resultSet.getValue(dsIndex, "paramlistversionid") + "_"
                    + resultSet.getValue(dsIndex, "variantid") + "_"
                    + resultSet.getValue(dsIndex, "instrumentfield");

            // if(paramDetails.get(resultSet.getValue(dsIndex, "instrumentfield"))!= null){
            if(paramDetails.get(key) != null){
                rowNum = resultSetForResultGrid.addRow();
                resultSetForResultGrid.setValue(rowNum, "sdcid", "Sample");
                resultSetForResultGrid.setValue(rowNum, "keyid1", resultSet.getValue(dsIndex, "keyid1"));
                resultSetForResultGrid.setValue(rowNum, "keyid2", resultSet.getValue(dsIndex, "keyid2", "(null)"));
                resultSetForResultGrid.setValue(rowNum, "keyid3", resultSet.getValue(dsIndex, "keyid3", "(null)"));
                resultSetForResultGrid.setValue(rowNum, "paramlistid", resultSet.getValue(dsIndex, "paramlistid"));
                resultSetForResultGrid.setValue(rowNum, "paramlistversionid", resultSet.getValue(dsIndex, "paramlistversionid"));
                resultSetForResultGrid.setValue(rowNum, "variantid", resultSet.getValue(dsIndex, "variantid"));
                resultSetForResultGrid.setValue(rowNum, "dataset", resultSet.getValue(dsIndex, "dataset"));
                resultSetForResultGrid.setValue(rowNum, "replicateid", resultSet.getValue(dsIndex, "replicateid"));

                // Edited for PG07735-8235 for issue related to unique paramid and common instrumentfield mapping in PL
                String[] paramDetailsValue = paramDetails.get(key).split("~");
                resultSetForResultGrid.setValue(rowNum, "paramid", paramDetailsValue[0]);
                resultSetForResultGrid.setValue(rowNum, "paramtype", paramDetailsValue[1]);
                //resultSetForResultGrid.setValue(rowNum, "paramid", paramDetails.get(resultSet.getValue(dsIndex, "instrumentfield")).split("~")[0]);
                //resultSetForResultGrid.setValue(rowNum, "paramtype", paramDetails.get(resultSet.getValue(dsIndex, "instrumentfield")).split("~")[1]);


                resultSetForResultGrid.setValue(rowNum, "value", resultSet.getValue(dsIndex, "value"));
                resultSetForResultGrid.setValue(rowNum, "notes", resultSet.getValue(dsIndex, "notes"));
                resultSetForResultGrid.setValue(rowNum, "s_analystid", resultSet.getValue(dsIndex, "s_analystid"));
                resultSetForResultGrid.setValue(rowNum, "datacaptureid", propertyList.getProperty("datacaptureid"));
                resultSetForResultGrid.setValue(rowNum, "instrumentid", propertyList.getProperty("instrumentid"));
            }
        }

        return resultSetForResultGrid;
    }

    public void releaseDataItem(PropertyList propertyList, DataSet resultSetForResultGrid, boolean autRelease,
                                ActionProcessor actionProcessor, ConfigurationProcessor configurationProcessor) throws SapphireException {
        //Do the autorelease as per flag for raw data
        if (autRelease) {
            PropertyList policy = configurationProcessor.getPolicy("DataEntryPolicy", "Sapphire Custom");
            String allowreleaseblank = policy.getProperty("allowreleaseblank");

            HashMap hm = new HashMap();

            hm.put("sdcid", resultSetForResultGrid.getValue(0, "sdcid"));
            hm.put("keyid1", resultSetForResultGrid.getColumnValues("keyid1", ";"));
            hm.put("keyid2", resultSetForResultGrid.getColumnValues("keyid2", ";"));
            hm.put("keyid3", resultSetForResultGrid.getColumnValues("keyid3", ";"));
            hm.put("paramlistid", resultSetForResultGrid.getColumnValues("paramlistid", ";"));
            hm.put("paramlistversionid", resultSetForResultGrid.getColumnValues("paramlistversionid", ";"));
            hm.put("variantid", resultSetForResultGrid.getColumnValues("variantid", ";"));
            hm.put("dataset", resultSetForResultGrid.getColumnValues("dataset", ";"));
            hm.put("paramid", resultSetForResultGrid.getColumnValues("paramid", ";"));
            hm.put("paramtype", resultSetForResultGrid.getColumnValues("paramtype", ";"));
            hm.put("replicateid", resultSetForResultGrid.getColumnValues("replicateid", ";"));


            hm.put("calculatemodifiedtestsonly", propertyList.getPropertyValue("calculatemodifiedtestsonly"));

            if (allowreleaseblank.equals("N"))
                hm.put("allowmandatorynulls", "N");
            else
                hm.put("allowmandatorynulls", "Y");

            hm.put("propsmatch", "Y");

            actionProcessor.processAction("ReleaseDataItem", "1", hm);

            hm.clear();
            //}

            //call UpdateDatasetStatus
            hm.clear();
            hm.put("sdcid", resultSetForResultGrid.getValue(0, "sdcid"));
            hm.put("keyid1", getUniqueValue(resultSetForResultGrid.getColumnValues("keyid1", ";"), ";"));

            actionProcessor.processAction("UpdateDatasetStatus", "1", hm);
            actionProcessor.processAction("SyncSDIDataSetStatus", "1", hm);

        }
    }

    public void executePostDataEntryAction(PropertyList propertyList, DataSet resultSetForResultGrid, ActionProcessor actionProcessor) throws ActionException {
        String actionId = "", actionVersionId = "";
        if (propertyList.get("postdataentryactionid") != null) {
            actionId = propertyList.get("postdataentryactionid").toString();
        }
        if (StringUtils.isBlank(actionId)) {
            logger.info("No PostAction configured.");
            return;
        }

        if(actionId.contains("|")){
            String[] actionIdSplit = actionId.split("\\|");
            actionId = actionIdSplit[0];
            actionVersionId = actionIdSplit[1];
        }
        if(StringUtils.isBlank(actionVersionId)){
            actionVersionId = "1";
        }

        HashMap props = new HashMap<>();
        props.put("xmldataset", resultSetForResultGrid.toXML());
        props.put("instrumentid", propertyList.getProperty("instrumentid"));

        actionProcessor.processAction(actionId, "1", props);

    }

    public static String createDirectory(String storagePath, String folder) {

        String finalPath =null;
        if(storagePath.contains("\\")){
            finalPath = storagePath + "\\" + folder +"\\";
        }else{
            finalPath = storagePath + "/" + folder +"/";
        }
        System.out.println("finalPath =" + finalPath);
        File dir = new File(finalPath);
        if (!dir.exists()) {
            dir.mkdirs();
            System.out.println("Created directory structure ");

        }
        return finalPath;
    }



    public void processResultGridAndUpdateDataItem(DataSet resultSet, PropertyList propertyList, ConnectionInfo connectionInfo,ActionProcessor actionProcessor,boolean runPostDataAction) throws SapphireException {

        try
        {
            String calculationScope = propertyList.getProperty("de_auto_calculation_scope");
            logger.info("CommonUtil calculationScope ="+calculationScope );
                    //common in every attachment handler,Result Grid
            DataSet resultSetForResultGrid = getResultSetForResultGrid(propertyList, resultSet);
            logger.info("resultSetForResultGrid =" +resultSetForResultGrid.toString());

           // logger.info("CommonUtil In properties calculatemodifieddatasetsonly ="+propertyList.getProperty("calculatemodifieddatasetsonly") );
           // logger.info("CommonUtil In properties calculatemodifiedtestsonly ="+propertyList.getProperty("calculatemodifiedtestsonly") );

            // Adding this for #PG07735-7240
            if ( calculationScope.isEmpty() || calculationScope == null ){
                propertyList.setProperty( "calculatemodifieddatasetsonly", callSDMS_PGCalcScope(resultSetForResultGrid));
            }else {
                propertyList.setProperty( calculationScope, "Y");
            }

            logger.info("CommonUtil In properties calculatemodifieddatasetsonly ="+propertyList.getProperty("calculatemodifieddatasetsonly") );
            logger.info("CommonUtil In properties calculatemodifiedtestsonly ="+propertyList.getProperty("calculatemodifiedtestsonly") );


            ResultDataGrid resultDataGrid = createResultGrid(resultSetForResultGrid, propertyList, connectionInfo, "Sample");
            logger.info("resultDataGrid ="+resultDataGrid.getDataSet().toString());


            //save the result with auto release false
            resultDataGrid.save();
            logger.info("resultDataGrid.save");

            //call the EditDataItem to update the data
            invokeEditDataItem(resultSetForResultGrid, actionProcessor);
            //Post Data Action.
//            if(runPostDataAction)
//                executePostDataEntryAction(propertyList,resultSetForResultGrid,actionProcessor);

        }catch (Exception e){
            throw new SapphireException(e.getMessage());
        }
    }


    public boolean isNumericResult(String source){
        boolean result = false;
        if(!StringUtils.isBlank(source)) {
            Pattern pattern1, pattern2;
            //Pattern pattern = Pattern.compile("[0-9]+.[0-9]+"); //correct pattern for both float and integer.
            pattern1 = Pattern.compile("\\d+.\\d+"); //correct pattern for both float and integer.
            pattern2 = Pattern.compile("\\d");

            result = pattern1.matcher(source).matches()||pattern2.matcher(source).matches();
        }

        return result;
    }

    public ResultDataGrid createResultGridForParamIDs(DataSet resultSet, PropertyList properties, ConnectionInfo connectionInfo, String sdcid) {
        ResultDataGrid resultGrid = new ResultDataGrid(connectionInfo);
        ResultGridOptions resultGridOptions = new ResultGridOptions();
        resultGridOptions.setSdcId(sdcid);
        resultGridOptions.setAutoAddSDI(properties.getProperty("autoaddsdi").equals("") ?
                ResultGridOptions.AutoAddSDI.NEVER : ResultGridOptions.AutoAddSDI.valueOf(properties.getProperty("autoaddsdi").toUpperCase()));
        resultGridOptions.setAutoAddWorkItem(properties.getProperty("autoaddsdiworkitem").equals("") ?
                ResultGridOptions.AutoAddWorkItem.NEVER : ResultGridOptions.AutoAddWorkItem.valueOf(properties.getProperty("autoaddsdiworkitem").toUpperCase()));
        resultGridOptions.setAutoAddDataset(properties.getProperty("autoadddataset").equals("") ?
                ResultGridOptions.AutoAddDataSet.NEVER : ResultGridOptions.AutoAddDataSet.valueOf(properties.getProperty("autoadddataset").toUpperCase()));
        resultGridOptions.setAutoAddReplicate(properties.getProperty("autoaddreplicate").equals("") ?
                ResultGridOptions.AutoAddReplicate.NEVER : ResultGridOptions.AutoAddReplicate.valueOf(properties.getProperty("autoaddreplicate").toUpperCase()));
        resultGridOptions.setAutoAddParameter(properties.getProperty("autoaddparameter").equals("") ?
                ResultGridOptions.AutoAddParameter.NEVER : ResultGridOptions.AutoAddParameter.valueOf(properties.getProperty("autoaddparameter").toUpperCase()));
        resultGridOptions.setReleaseHandlingRule(properties.getProperty("releasehandlingrule").equals("") ?
                ResultGridOptions.ReleaseHandlingRule.ERROR : ResultGridOptions.ReleaseHandlingRule.valueOf(properties.getProperty("releasehandlingrule").toUpperCase()));
        resultGridOptions.setAutoRelease("Y".equalsIgnoreCase(properties.getProperty("autorelease")));

        resultGridOptions.setDefaultDataSet(properties.getProperty("defaultdataset").equals("") ?
                ResultGridOptions.DefaultDataSet.FIRST_AVAILABLE : ResultGridOptions.DefaultDataSet.valueOfDefaultDataSet(properties.getProperty("defaultdataset")));
        resultGridOptions.setDefaultReplicateId(properties.getProperty("defaultreplicate").equals("") ?
                ResultGridOptions.DefaultReplicateId.FIRST_AVAILABLE : ResultGridOptions.DefaultReplicateId.valueOfDefaultReplicateId(properties.getProperty("defaultreplicate")));
        resultGridOptions.setApplyLock(false); // need to stop sending aplylock as false to allow data entry for multiple Tests at the same time.
        logger.debug("ApplyLock has been set to false in ResultGridOptions.");
        resultGridOptions.setMissingDataErrorHandling(properties.getProperty("missingdataerrorhandling").equals("") ?
                ResultGridOptions.MissingDataErrorHandling.IGNORE : ResultGridOptions.MissingDataErrorHandling.valueOf(properties.getProperty("missingdataerrorhandling").toUpperCase()));

        PropertyList enterdataitemprops = new PropertyList();

        // JIRA #PG07735-6898. Property is set depending on the property sent in the driver.
        if (properties.getProperty("calculatemodifiedtestsonly") == "Y") {
            enterdataitemprops.setProperty("calculatemodifiedtestsonly", "Y");
        } else {
            enterdataitemprops.setProperty("calculatemodifieddatasetsonly", "Y");
        }

        HashMap<sapphire.util.ResultDataGrid.CoreColumns, String> resultFields = new HashMap<sapphire.util.ResultDataGrid.CoreColumns, String>();
        HashMap<String, String> additionalFields = new HashMap<String, String>();
        //int resultGridIndex = -1;

        for (int i = 0; i < resultSet.getRowCount(); ++i) {
            resultFields.put(sapphire.util.ResultDataGrid.CoreColumns.SDCID, resultSet.getValue(i, "sdcid"));
            resultFields.put(sapphire.util.ResultDataGrid.CoreColumns.KEYID1, resultSet.getValue(i, "keyid1"));
            resultFields.put(sapphire.util.ResultDataGrid.CoreColumns.KEYID2, resultSet.getValue(i, "keyid2"));
            resultFields.put(sapphire.util.ResultDataGrid.CoreColumns.KEYID3, resultSet.getValue(i, "keyid3"));
            resultFields.put(sapphire.util.ResultDataGrid.CoreColumns.PARAMLISTID, resultSet.getValue(i, "paramlistid"));
            resultFields.put(sapphire.util.ResultDataGrid.CoreColumns.PARAMLISTVERSIONID, resultSet.getValue(i, "paramlistversionid"));
            resultFields.put(sapphire.util.ResultDataGrid.CoreColumns.VARIANTID, resultSet.getValue(i, "variantid"));
            resultFields.put(sapphire.util.ResultDataGrid.CoreColumns.DATASET, resultSet.getValue(i, "dataset"));
            resultFields.put(sapphire.util.ResultDataGrid.CoreColumns.REPLICATEID, resultSet.getValue(i, "replicateid"));
            resultFields.put(sapphire.util.ResultDataGrid.CoreColumns.PARAMID, resultSet.getValue(i, "paramid"));
            resultFields.put(sapphire.util.ResultDataGrid.CoreColumns.PARAMTYPE, resultSet.getValue(i, "paramtype"));
            resultFields.put(sapphire.util.ResultDataGrid.CoreColumns.VALUE, resultSet.getValue(i, "value"));
            additionalFields.put("notes", resultSet.getValue(i, "notes"));
            additionalFields.put("s_analystid", resultSet.getValue(i, "s_analystid"));
            additionalFields.put("instrumentid", resultSet.getValue(i, "instrumentid"));
            additionalFields.put("datacaptureid", resultSet.getValue(i, "datacaptureid"));

            resultGrid.addResult(resultFields, additionalFields);
            resultFields.clear();
            additionalFields.clear();
        }

        //add enterdataitem properties to resultgrid
        resultGridOptions.setEnterDataItemProperties(enterdataitemprops);
        resultGrid.setOptions(resultGridOptions);
        return resultGrid;
    }

    public void processResultGridAndUpdateDataItemForParamIDs(DataSet resultSet, PropertyList propertyList, ConnectionInfo connectionInfo,ActionProcessor actionProcessor,boolean runPostDataAction) throws SapphireException {

        try
        {
            String calculationScope = propertyList.getProperty("de_auto_calculation_scope");
            logger.info("CommonUtil calculationScope ="+calculationScope );
            //common in every attachment handler,Result Grid
            //DataSet resultSetForResultGrid = getResultSetForResultGrid(propertyList, resultSet);
            //logger.info("resultSetForResultGrid =" +resultSetForResultGrid.toString());

            // logger.info("CommonUtil In properties calculatemodifieddatasetsonly ="+propertyList.getProperty("calculatemodifieddatasetsonly") );
            // logger.info("CommonUtil In properties calculatemodifiedtestsonly ="+propertyList.getProperty("calculatemodifiedtestsonly") );

            // Adding this for #PG07735-7240
            if ( calculationScope.isEmpty() || calculationScope == null ){
                propertyList.setProperty( "calculatemodifieddatasetsonly", callSDMS_PGCalcScope(resultSet));
            }else {
                propertyList.setProperty( calculationScope, "Y");
            }

            logger.info("CommonUtil In properties calculatemodifieddatasetsonly ="+propertyList.getProperty("calculatemodifieddatasetsonly") );
            logger.info("CommonUtil In properties calculatemodifiedtestsonly ="+propertyList.getProperty("calculatemodifiedtestsonly") );


            ResultDataGrid resultDataGrid = createResultGridForParamIDs(resultSet, propertyList, connectionInfo, "Sample");
            logger.info("resultDataGrid ="+resultDataGrid.getDataSet().toString());


            //save the result with auto release false
            resultDataGrid.save();
            logger.info("resultDataGrid.save");

            //call the EditDataItem to update the data
            invokeEditDataItem(resultSet, actionProcessor);
            //Post Data Action.
            if(runPostDataAction)
                executePostDataEntryAction(propertyList,resultSet,actionProcessor);

        }catch (Exception e){
            throw new SapphireException(e.getMessage());
        }
    }


}

