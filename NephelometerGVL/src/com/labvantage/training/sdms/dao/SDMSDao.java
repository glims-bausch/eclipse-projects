package com.labvantage.training.sdms.dao;

import com.labvantage.training.sdms.utl.SDMSCommonUtil;
import com.labvantage.sapphire.BaseCustom;
import org.apache.commons.lang.StringUtils;
import sapphire.SapphireException;
import sapphire.util.DataSet;
import sapphire.util.SafeSQL;
import sapphire.xml.PropertyList;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;



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
 * Violaters will be prosecuted to the fullest extent of the law by
 * LabVantage.
 * <p>
 * Developed by LABVANTAGE Solutions.
 * 265 Davidson Avenue Suite 220
 * Somerset, NJ 08873 USA
 * <p>
 */

//Commented out all "stepno" instances - rfrondilla
public class SDMSDao extends BaseCustom {

    //private String connectionId="";
    // private QueryProcessor qp;
    // private ActionProcessor ap;
    // private Logger logger;
    // private DAMProcessor dam;

   /* public SDMSDao(QueryProcessor qp , ActionProcessor ap,DAMProcessor dam,Logger logger) {
         this.qp = qp;
         this.ap = ap;
         this.logger = logger;
         this.dam = dam;
    }*/

    public SDMSDao(String connectionId) {
        super.setConnectionId(connectionId);
    }

    public Properties getInstParams(String instConnId) {

        SafeSQL safeSQL = new SafeSQL();
        String sql = "SELECT parameter, value FROM u_instparam ip WHERE ip.instconnectid = '" + instConnId +"'";
//        DataSet ds =  getQueryProcessor().getSqlDataSet(sql.toString());
        DataSet ds = getQueryProcessor().getSqlDataSet(sql.toString());
        Properties props = new Properties();
        for (int i = 0; i < ds.getRowCount(); i++) {
            props.put(ds.getValue(i, "parameter"), ds.getValue(i, "value"));
        }

        return props;
    }

    public  Properties getProtFolderNames( String instrumentid) {

        SafeSQL safeSQL = new SafeSQL();
        String sql = "SELECT importfoldername, backupfoldername, exportfoldername, failfoldername, logsfoldername FROM u_instconnect ic, u_instprotocol ip " +
                "WHERE ip.u_instprotocolid = ic.instprotocolid AND ic.u_instconnectid = '"  +instrumentid +"'";
        DataSet ds =  getQueryProcessor().getSqlDataSet(sql.toString());


        Properties props = new Properties();
        for (int i = 0; i < ds.getRowCount(); i++) {
            props.put("import", ds.getValue(i, "importfoldername"));
            props.put("backup", ds.getValue(i, "backupfoldername"));
            props.put("export", ds.getValue(i, "exportfoldername"));
            props.put("fail", ds.getValue(i, "failfoldername"));
            props.put("logs", ds.getValue(i, "logsfoldername"));
        }

        return props;
    }

    /**
     * This method will get the csv output path from database

     * @param instConnectionId        Instrument Connection Id to be appended with path defined in DB to get final path
     * @return property object that will contain the csvoutpath property
     */
    public  String getRawDataOutDirectory( String instConnectionId){
        //Properties outDirectories = new Properties();
        String outPathFromDB = null, outPathFinal = null;
        String sqlStr = "select notes from u_instvariable v where v.u_instvariableid = 'RawDataOutRootDirectory'";

        DataSet dsGetData =  getQueryProcessor().getSqlDataSet(sqlStr);
        if(dsGetData.getRowCount()>0){
            outPathFromDB = dsGetData.getValue(0, "notes");
        }
        if(outPathFromDB!= null){
            if(outPathFromDB.contains("\\")){
                outPathFinal = outPathFromDB + "\\" + instConnectionId + "\\";
            }else{
                outPathFinal = outPathFromDB + "/" + instConnectionId + "/";
            }
            //outDirectories.setProperty("csvoutpath", outPathFinal);
        }

        if(outPathFinal!= null){
            File outputPath = new File(outPathFinal);
            if(!outputPath.exists()){
                //System.out.println("Creating folder : " + outPathFinal);
                outputPath.mkdirs();
            }
        }

        return outPathFinal;
    }


    /**
     * This method will get the csv output OS path from database
     * @param instConnectionId        Instrument Connection Id to be appended with path defined in DB to get final path
     * @return property object that will contain the csvoutpath property. This is updated in the value for File Name parameter.
     */
    public  Properties getRawDataOutUNCAndOSPath( String instConnectionId){
        String sqlStr = "select Upper(b.itemkey) keyvalue, b.itemvalue itemvalue from u_instvariable a join u_instvaritem b on a.u_instvariableid = b.instvariableid " +
                "and u_instvariableid = 'RawDataFilePath' "+
                "and Upper(b.itemkey) in ('OS PATH', 'UNC PATH')";

        DataSet dsGetData =  getQueryProcessor().getSqlDataSet(sqlStr);
        Properties props = new Properties();
        for (int i = 0; i < dsGetData.getRowCount(); i++) {
            if (dsGetData.getValue(i, "keyvalue").equals("OS PATH")){
                props.put("ospath", SDMSCommonUtil.getFinalPath(dsGetData.getValue(i, "itemvalue"), instConnectionId, true));
            } else {
                props.put("uncpath", SDMSCommonUtil.getFinalPath(dsGetData.getValue(i, "itemvalue"), instConnectionId, false));
            }
        }

        return props;
    }

    /* This method will get the csv output UNC path from database
     * @param instConnectionId		Instrument Connection Id to be appended with path defined in DB to get final path
     * @return	property object that will contain the csvoutpath property. This data would be updated in notes field.
     */
    public  String getRawDataOutUNCPath( String instConnectionId) {

        String outPathFromDB = null, outPathFinal = null;

        String sqlStr = "select b.itemvalue uncpath from u_instvariable a join u_instvaritem b on a.u_instvariableid = b.instvariableid " +
                "and u_instvariableid = 'RawDataFilePath' " +
                "and Upper(b.itemkey) = 'UNC PATH'";
        DataSet dsGetData = getQueryProcessor().getSqlDataSet(sqlStr);

        if (dsGetData.getRowCount() > 0) {
            outPathFromDB = dsGetData.getValue(0, "uncpath");
        }
        if (outPathFromDB != null) {
            if (outPathFromDB.contains("\\")) {
                outPathFinal = outPathFromDB + "\\" + instConnectionId + "\\";
            } else {
                outPathFinal = outPathFromDB + "/" + instConnectionId + "/";
            }
        }
        return outPathFinal;
    }

    /**
     * This method will get storagepathlocal from the instrument configuration

     * @param instrumentid        Instrument Id
     * @return storagepathlocal
     */
    public  String getCollectorStoragePath(String instrumentid) {
        System.out.println("In getCollectorStoragePath");
        SafeSQL safeSQL = new SafeSQL();
        StringBuffer sql = new StringBuffer();

        sql.append("select storagepathlocal  from sdmscollector coll join instrument ins on coll.sdmscollectorid = ins.sdmscollectorid and");
        sql.append(" ins.instrumentid = '") .append(instrumentid).append("'");

        DataSet value =   getQueryProcessor().getSqlDataSet(sql.toString());
        System.out.println( "storagepathlocal =" +  value.getValue(0, "storagepathlocal"));

        if(value.getRowCount() > 0)
            return value.getValue(0, "storagepathlocal");



        return "";
    }

    /**
     * This method will get instrument id given the datacapture id

     * @param dataCaptureID        dataCapture Id
     * @return instrumentid
     */
    public  String getInstrumentID(String dataCaptureID) {

        SafeSQL safeSQL = new SafeSQL();
        String sql = " select instrumentid from datacapture where datacaptureid = '" + dataCaptureID +"'";
        logger.debug("sql is "+sql);
        DataSet dsInstrumnet =  getQueryProcessor().getSqlDataSet(sql.toString());
        if(dsInstrumnet !=null && dsInstrumnet.getRowCount() > 0)
            return dsInstrumnet.getValue(0, "instrumentid");


        return "";
    }
    
    

    /**
     * This method will get the parameter list details given the sample id, instrument id ,mandatory field ,wiUserSequence present in the parameter
     * wiUserSequence  can be passed as null and WON'T BE USED and a different sql without workitemid is used.
     * @param sampleID  sample ID
     * @param instrumentID instrument id.
     * @param instrFieldID mandatory field from instrument that is present in the parameter list.
     *@param wiUserSequence  optional step number, can be a null , if present will be used
     * @return property object that will contain the parameter list details.
     */


    public  PropertyList getParamList(String sampleID, String instrumentID, String instrFieldID, String wiUserSequence) {

        String sparamlistid ="";
        String sparamlistversionid ="";
        String svariantid ="";
        StringBuffer sql = new StringBuffer();
        PropertyList pl = new PropertyList();

        if(wiUserSequence != null) {
            sql.append(" select di.paramlistid, di.paramlistversionid, di.variantid");
            sql.append(" from  sdidataitem di  join sdidata sd");
            sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
            sql.append(" and sd.paramlistid = di.paramlistid");
            sql.append(" and sd.paramlistversionid = di.paramlistversionid");
            sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
            sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
            sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
            sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
            sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
            sql.append(" and sd.variantid = pl.variantid");
            sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
            sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
            sql.append(" where di.sdcid = 'Sample'");
            sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
            sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
            sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
//            sql.append(" and wi.u_stepno  = '").append(wiUserSequence).append("' ");
            DataSet dsParamlist = getQueryProcessor().getSqlDataSet(sql.toString());
            if (dsParamlist.getRowCount() > 0) {
                sparamlistid = dsParamlist.getValue(0, "paramlistid");
                sparamlistversionid = dsParamlist.getValue(0, "paramlistversionid");
                svariantid = dsParamlist.getValue(0, "variantid");

                pl.setProperty("sdcid", "Sample");
                pl.setProperty("keyid1", sampleID);
                pl.setProperty("keyid2", "(null)");
                pl.setProperty("keyid3", "(null)");
                pl.setProperty("paramlistid", sparamlistid);
                pl.setProperty("paramlistversionid", sparamlistversionid);
                pl.setProperty("variantid", svariantid);
                logger.debug("ParamID: " + sparamlistid +" ParamlistVersionID: "+ sparamlistversionid +" Variant ID: "+ svariantid);
            }
        }
        else {

            sql.append(" select distinct di.paramlistid, di.paramlistversionid, di.variantid");
            sql.append(" from  sdidataitem di  join sdidata sd");
            sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
            sql.append(" and sd.paramlistid = di.paramlistid");
            sql.append(" and sd.paramlistversionid = di.paramlistversionid");
            sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
            sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
            sql.append(" and sd.variantid = pl.variantid");
            sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
            sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
            sql.append(" where di.sdcid = 'Sample'");
            sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
            sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
            sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");

            DataSet dsParamlist = getQueryProcessor().getSqlDataSet(sql.toString());
            if (dsParamlist.getRowCount() > 0) {
                pl.setProperty("paramlistid", dsParamlist.getValue(0, "paramlistid"));
                pl.setProperty("paramlistversionid", dsParamlist.getValue(0, "paramlistversionid"));
                pl.setProperty("variantid", dsParamlist.getValue(0, "variantid"));
            }

        }

        logger.debug("SQL for getting paramlist for sample "+  sampleID + " and usersequence "+ wiUserSequence + " is "+ sql.toString());


        return pl;

    }


    // Do note that though getParamListWithoutDataSet and getParamListByDataSet could be combined into one, they are not combined as
    // as these are existing methods in the already released handlers.

    /**
     *  This method will get the parameter list details from the sample id, instrument id ,mandatory field present in the parameter
     * list .
     * @param sampleID  sample ID
     * @param instrumentID instrument id, can be null
     * @param instrFieldID mandatory field from instrument that is present in the parameter list.
     * @return	property object that will contain the parameter list details.
     */
    public  PropertyList getParamListWithoutDataSet(String sampleID, String instrumentID, String instrFieldID) {

        String sparamlistid ="";
        String sparamlistversionid ="";
        String svariantid ="";

        PropertyList pl = new PropertyList();

        StringBuffer sql = new StringBuffer();
        sql.append(" select di.paramlistid, di.paramlistversionid, di.variantid");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        if(instrumentID != null){
            sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        }
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");

        logger.debug("getParamListWithoutDataSet:: " + sql.toString());

        DataSet dsParamlist = getQueryProcessor().getSqlDataSet(sql.toString());

        //System.out.println("No. of rows: "+ dsParamlist.getRowCount());

        if (dsParamlist.getRowCount() > 0) {
            sparamlistid = dsParamlist.getValue(0, "paramlistid");
            sparamlistversionid = dsParamlist.getValue(0, "paramlistversionid");
            svariantid = dsParamlist.getValue(0, "variantid");

            pl.setProperty("sdcid", "Sample");
            pl.setProperty("keyid1", sampleID);
            pl.setProperty("keyid2", "(null)");
            pl.setProperty("keyid3", "(null)");
            pl.setProperty("paramlistid", sparamlistid);
            pl.setProperty("paramlistversionid", sparamlistversionid);
            pl.setProperty("variantid", svariantid);
        }

        //System.out.println("ParamID: " + sparamlistid +" ParamlistVersionID: "+ sparamlistversionid +" Variant ID: "+ svariantid);

        return pl;

    }

    /**
     This method will get the parameter list details given the sample id, instrument id ,mandatory field present in the parameter
     * list and dataset.
     * @param sampleID  sample ID
     * @param instrumentID instrument id.
     * @param instrFieldID mandatory field from instrument that is present in the parameter list.
     * @param dataset   number.
     * @return	property object that will contain the parameter list details.
     */
    public PropertyList getParamListByDataSet(String sampleID, String instrumentID, String instrFieldID, String dataset) {

        String sparamlistid ="";
        String sparamlistversionid ="";
        String svariantid ="";

        PropertyList pl = new PropertyList();

        StringBuffer sql = new StringBuffer();
        sql.append(" select di.paramlistid, di.paramlistversionid, di.variantid");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
        sql.append(" and di.dataset  = ").append(dataset).append(" ");


        //System.out.println("SQL for getting paramlist for sample "+  sampleID + " and usersequence "+ wiUserSequence + " is "+ sql.toString());

        DataSet dsParamlist = getQueryProcessor().getSqlDataSet(sql.toString());

        //System.out.println("No. of rows: "+ dsParamlist.getRowCount());

        if (dsParamlist.getRowCount() > 0) {
            sparamlistid = dsParamlist.getValue(0, "paramlistid");
            sparamlistversionid = dsParamlist.getValue(0, "paramlistversionid");
            svariantid = dsParamlist.getValue(0, "variantid");

            pl.setProperty("paramlistid", sparamlistid);
            pl.setProperty("paramlistversionid", sparamlistversionid);
            pl.setProperty("variantid", svariantid);
        }

        //System.out.println("ParamID: " + sparamlistid +" ParamlistVersionID: "+ sparamlistversionid +" Variant ID: "+ svariantid);

        return pl;

    }

    /**
     *  This method will get the parameter list version id given the sample id, parameter list id, variant and instrument field.
     * @param sampleID  sample ID
     * @param paramListId  parameter list.
     * @param variantId  parameter list variant.
     * @param instrFieldID   number.
     * @return	parameter list version .
     */
    public PropertyList getParamListVersionId(String sampleID, String paramListId, String variantId, String instrFieldID) {

        String sparamlistversionid ="";

        PropertyList pl = new PropertyList();

        StringBuffer sql = new StringBuffer();
        sql.append(" select di.paramlistversionid");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and trim(di.instrumentfieldid)  = '").append(instrFieldID).append("' ");

        //System.out.println("getParamListVersionId : \n"+ sql.toString());

        DataSet dsParamlist = getQueryProcessor().getSqlDataSet(sql.toString());
        if (dsParamlist.getRowCount() > 0) {
            sparamlistversionid = dsParamlist.getValue(0, "paramlistversionid");

            pl.setProperty("paramlistversionid", sparamlistversionid);
        } else {
            pl.setProperty("paramlistversionid", "NA");
        }
        //System.out.println(" ParamlistVersionID: "+ sparamlistversionid);
        return pl;

    }


    /**
     *  This method will get the parameter list version id given the sample id, instrument id ,parameter list id, variant
     * list and dataset.
     * @param sampleId  sample ID
     * @param instrumentId instrument id.
     * @param rawdataPlist  parameter list.
     * @param rawdataVar  parameter list variant.
     * @param dataset   number.
     * @return	parameter list version .
     */
    public String getParamListVersionId(String sampleId, String instrumentId, String rawdataPlist, String rawdataVar,
                                        int dataset){
        //System.out.println("getParamListVersionId ::: Utils : " + utils);
        String paramListVersionId = null;
        String queryToGetParamListversionId =
                "select di.paramlistversionid paramlistversionid "+
                        "from  sdidataitem di  join sdidata sd on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 "+
                        "and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3 and sd.paramlistid = di.paramlistid "+
                        "and sd.paramlistversionid = di.paramlistversionid and sd.variantid = di.variantid "+
                        "and sd.dataset = di.dataset join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid " +
                        "and wi.workiteminstance = sd.sourceworkiteminstance and wi.keyid1 = sd.keyid1 " +
                        "and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid " +
                        "join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid " +
                        "and sd.variantid = pl.variantid join instrument i on i.instrumenttype = pl.s_instrumenttype " +
                        "and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null) " +
                        "where di.sdcid = 'Sample' " +
                        "and di.keyid1 = '" + sampleId + "' " +
                        "and i.instrumentid = '"+ instrumentId + "' " +
                        "and sd.paramlistid = '" + rawdataPlist + "' " +
                        "and lower(sd.variantid) = '" + rawdataVar.trim().toLowerCase() + "' " +
                        "and di.dataset  = '" + dataset + "' ";

        //System.out.println("queryToGetParamListversionId : " + queryToGetParamListversionId);


        DataSet ds = getQueryProcessor().getSqlDataSet(queryToGetParamListversionId);
        if(ds.getRowCount()>0){
            paramListVersionId = ds.getValue(0, "paramlistversionid");
        }


        return paramListVersionId;
    }

    /**
     *  This method will get the param list id, version and variant for the sample,  instrument id ,variant and step number .
     * @param sSampleID  sample IDs
     * @param sInstrumentID  instrument ID
     * @param sVariantID  variant of the parameter list.
     * @param userSequence step number.
     * @return	dataset.
     */
    public DataSet getParameterListInfo(String sSampleID, String sInstrumentID, String sVariantID, String userSequence) {

        StringBuffer sbPLInfo = new StringBuffer();
        sbPLInfo.append("select pl.paramlistid, pl.paramlistversionid, pl.variantid ");
        sbPLInfo.append("from  sdidataitem di ");
        sbPLInfo.append("join sdidata sd on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 ");
        sbPLInfo.append("and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3 and sd.paramlistid = di.paramlistid ");
        sbPLInfo.append("and sd.paramlistversionid = di.paramlistversionid and sd.variantid = di.variantid and sd.dataset = di.dataset ");
        sbPLInfo.append("join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid ");
        sbPLInfo.append("and wi.workiteminstance = sd.sourceworkiteminstance and wi.keyid1 = sd.keyid1 ");
        sbPLInfo.append("and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid ");
        sbPLInfo.append("and (wi.workitemid != wi.groupid or wi.groupid is null) ");
        sbPLInfo.append("join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid ");
        sbPLInfo.append("and sd.variantid = pl.variantid ");
        sbPLInfo.append("join instrument i on i.instrumenttype = pl.s_instrumenttype ");
        sbPLInfo.append("and i.instrumentmodelid = pl.s_instrumentmodel or pl.s_instrumentmodel is null ");
        sbPLInfo.append("where di.sdcid = 'Sample' ");
        sbPLInfo.append("and di.keyid1 = '");
        sbPLInfo.append(sSampleID);
        sbPLInfo.append("' ");
        sbPLInfo.append("and i.instrumentid = '");
        sbPLInfo.append(sInstrumentID);
        sbPLInfo.append("' ");
        sbPLInfo.append("and pl.variantid = '");
        sbPLInfo.append(sVariantID);
        sbPLInfo.append("' ");
        sbPLInfo.append("and upper(di.instrumentfieldid)  = 'FILE NAME' ");
//        sbPLInfo.append("and wi.u_stepno  = '");
        sbPLInfo.append(userSequence);
        sbPLInfo.append("'");
        //System.out.println("Query to get Parameter List : \n"+sbPLInfo.toString());
        DataSet dsPLInfo = getQueryProcessor().getSqlDataSet(sbPLInfo.toString());

        return dsPLInfo;

    }



    /**
     *  This method will get the parameters, paramtypes and the instrumentfields mapping given the sample id, mandatory instrument field id ,
     * parameter list id, variant id and version id.
     * @param sampleID  sample ID
     * @param instrumentField instrumentfield id.
     * @param paramListId  parameter list.
     * @param variantid  parameter list variant.
     * @param versionId   parameter list version .
     * @return	dataset containing parameter list details from reference mapping .
     */
    public DataSet getParamListDetails(String sampleID, String instrumentField, String paramListId, String versionId, String variantid) {

        String sql = "select sdi.paramid, sdi.paramtype, sdi.instrumentfieldid " +
                "from sdidata sd " +
                "join sdidataitem sdi on sdi.sdcid = sd.sdcid and sdi.keyid1 = sd.keyid1 " +
                "and sdi.keyid2 = sd.keyid2 and  sdi.keyid3 = sd.keyid3 " +
                "and sdi.paramlistid = sd.paramlistid and sd.paramlistversionid = sd.paramlistversionid  " +
                "and sdi.variantid = sd.variantid and sd.dataset = sd.dataset " +
                "where sdi.keyid1 = '" + sampleID + "' " +
                //"and sd.sourceworkitemid = '" + workItemId + "' " +
                "and sdi.instrumentfieldid in ('" + instrumentField + "') "+
                "and sdi.paramlistid = '" +paramListId +"' "+
                "and sdi.paramlistversionid='"+ versionId+"' "+
                "and sdi.variantid = '" +variantid +"' ";

        logger.debug("SQL for getting paramlist is "+ sql);

        DataSet dsParamlist = getQueryProcessor().getSqlDataSet(sql);

//        if (dsParamlist.getRowCount() > 0) {
//            paramListDetails.setProperty("paramid", dsParamlist.getValue(0, "paramid"));
//            paramListDetails.setProperty("paramtype", dsParamlist.getValue(0, "paramtype"));
//        }
//        return paramListDetails;

        return dsParamlist;

    }


    /**
     * This method will check if the given dataset/replicate id is present in the system for the sample.<br><br>
     * Optional parameters are wiUserSequence,replicateId,dataset and instrFieldID. pass for each of these null incase they don't have actual values.
     * @param sampleID  sample ID
     * @param instrumentID instrument id.
     * @param instrFieldID  mandatory field of instrument type present in the pl, can be null.
     * @param wiUserSequence  step number, can be null.
     * @param dataset   number, can be null.
     * @param replicateId   replicate id, can be null.
     * @return	true/false.
     */
    public boolean  isDataSetOrReplicateValid(String sampleID, String instrumentID, String instrFieldID,
                                              String wiUserSequence, String dataset, String replicateId) {
        //System.out.println("Inside isDataSetOrReplicateValid :::: ");
        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();
        sql.append(" select count(1) datasetcount");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
//            if(wiUserSequence!= null){
//                sql.append(" and wi.u_stepno  = '").append(wiUserSequence).append("' ");
//            }
        if(dataset!= null){
            sql.append(" and di.dataset = '").append(dataset).append("' ");
        }
        if(replicateId!= null){
            sql.append(" and di.replicateid = '").append(replicateId).append("' ");
        }
        if(instrFieldID!= null){
            sql.append(" and di.instrumentfieldid in ('").append(instrFieldID).append("') ");
        }

        logger.debug("Inside isDataSetOrReplicateValid :::: " + sql.toString());

        DataSet cntdata = getQueryProcessor().getSqlDataSet(sql.toString());
        dataItemCnt = cntdata.getInt(0, "datasetcount");
        if (dataItemCnt > 0){
            return true;
        } else {
            return false;
        }
    }

    /**
     *  This method will check if the given dataset id empty.
     * @param sampleID  sample ID
     * @param instrumentField  mandatory field of instrument type present in the pl.
     * @param dataset   number.
     * @return	true/false .
     */
    public boolean isDataSetEmpty(String sampleID,  String dataset, String instrumentField ) {

        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();
        sql.append(" select count(1) datasetcount");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and di.dataset = '"+dataset+"' ");
        sql.append(" and di.instrumentfieldid  = '").append(instrumentField).append("' ");
        sql.append(" and di.enteredtext is null");

        logger.debug("isDataSetEmpty :: \n" + sql.toString());

        DataSet cntdata = getQueryProcessor().getSqlDataSet(sql.toString());
        dataItemCnt = cntdata.getInt(0, "datasetcount");
        if (dataItemCnt > 0){
            return true;
        } else {
            return false;
        }
    }

    /**
     *  This method will check if the given dataset id empty given the sampleid, instrumentid, dataset, instrument field, replicate id. <br>
     *  <br>
     *  Params InstrumentID , replicateId can be passed as null
     * @param sampleID  sample ID
     * @param instrumentField  mandatory field of instrument type present in the pl.
     * @param dataset   number.
     * @param replicateId   replicate id , can be null.
     * @param instrumentID   instrument ID, can be null.
     * @return	true/false .
     */

    public boolean isDataSetEmpty(String sampleID, String instrumentID, String dataset, String instrumentField,
                                  String replicateId) {
        //System.out.println("Inside isDataSetOrReplicateValid :::: ");
        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();
        sql.append(" select count(1) datasetcount");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");

        sql.append(" and di.dataset = '"+dataset+"' ");

        sql.append(" and di.instrumentfieldid  = '").append(instrumentField).append("' ");
        sql.append(" and di.enteredtext is null");

        //System.out.println("isDataSetEmpty :: \n" + sql.toString());

        DataSet cntdata = getQueryProcessor().getSqlDataSet(sql.toString());
        dataItemCnt = cntdata.getInt(0, "datasetcount");
        if (dataItemCnt > 0){
            return true;
        } else {
            return false;
        }
    }
    /**
     *  This method will check if the given dataset id empty given the sampleid, instrumentid, dataset, instrument field, step no, replicate id. <br>
     *  <br>
     *  Params userSeq , replicateId can be passed as null
     * @param sampleID  sample ID
     * @param instrumentField  mandatory field of instrument type present in the pl.
     * @param dataset   number.
     * @param replicateId   replicate id , can be null.
     * @param instrumentID   instrument ID, can be null.
     * @param userSeq   step number.
     * @return	true/false .
     */

    public boolean isDataSetEmpty(String sampleID, String instrumentID, String dataset, String instrumentField, String userSeq, String replicateId) {

        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();
        sql.append(" select count(1) datasetcount");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        sql.append(" and di.dataset = '"+dataset+"' ");
//        if(userSeq!= null){
//            sql.append(" and wi.u_stepno  = '").append(userSeq).append("' ");
//        }
        if(replicateId!= null){
            sql.append(" and di.replicateid = '"+replicateId+"' ");
        }
        sql.append(" and di.instrumentfieldid  = '").append(instrumentField).append("' ");
        sql.append(" and di.enteredtext is null");

        logger.debug("Inside isDataSetEmpty :::: \n" + sql.toString());

        DataSet cntdata = getQueryProcessor().getSqlDataSet(sql.toString());
        dataItemCnt = cntdata.getInt(0, "datasetcount");
        if (dataItemCnt > 0){
            return true;
        } else {
            return false;
        }
    }


    /**
     *  This method will get the minimum empty dataset number available for the sample .<br><br>
     *  instrumentID IS NOT USED IN THIS

     * @param sampleID  sample ID
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @return	available dataset number .
     */
    public int getDataSetNumber(String sampleID, String instrumentID, String instrFieldID) {

        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();

        sql.append(" select nvl(min(di.dataset), 0) mindataset");
        sql.append(" from  sdidataitem di");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
        sql.append(" and di.enteredtext is null");

        logger.debug("getDataSetNumber :: " +sql);
        DataSet mindata = getQueryProcessor().getSqlDataSet(sql.toString());
        dataItemCnt = Integer.parseInt(mindata.getValue(0, "mindataset"));

        return dataItemCnt;
    }

    /**
     *  This method will get the minimum empty dataset number available for the sample and the step number
     * @param sampleID  sample ID
     * @param wiUserSequence  step number,can be null
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @return	available dataset number .
     */
    public  int getDataSetNumber(String sampleID, String instrumentID, String instrFieldID, String wiUserSequence) {

        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();
        SafeSQL safeSQL = new SafeSQL();

        sql.append(" select nvl(min(di.dataset), 0) mindataset");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '"+sampleID+"' ");
        sql.append(" and i.instrumentid = '"+instrumentID+"' ");
        sql.append(" and di.instrumentfieldid  = '"+instrFieldID+"' ");
//        if(wiUserSequence != null)
//            sql.append(" and wi.u_stepno  = '").append(wiUserSequence).append("' ");
        sql.append(" and di.enteredtext is null");

        DataSet mindata =  getQueryProcessor().getSqlDataSet(sql.toString());
        //dsRequests.showData();
        dataItemCnt = Integer.parseInt(mindata.getValue(0, "mindataset"));


        return dataItemCnt;
    }

    /**
     *  This method will get the minimum dataset number available for the sample , parameter list and instrument field .
     *  If checkForEmptyDataSet is true then will return the min empty dataset <br><br>
     *  instrumentID is not used in the below function
     * @param sampleID  sample ID
     * @param paramListID  parameter list id.
     * @param paramlistVersionID  parameter list version id.
     * @param variantID  parameter variant id.
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @checkForEmptyDataSet     will check if the dataset is empty
     * @return	available dataset number .
     */
    public int getDataSetNumber(String sampleID, String instrumentID, String paramListID, String paramlistVersionID, String variantID, String instrFieldID,boolean checkForEmptyDataSet) {

        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();
        sql.append(" select nvl(min(di.dataset), 0) mindataset");
        sql.append(" from  sdidataitem di");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        if(paramListID!= null){
            sql.append(" and di.paramlistid = '").append(paramListID).append("' ");
        }
        if(paramlistVersionID!= null){
            sql.append(" and di.paramlistversionid = '").append(paramlistVersionID).append("' ");
        }
        if(variantID!= null){
            sql.append(" and di.variantid = '").append(variantID).append("' ");
        }
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
        if(checkForEmptyDataSet){
            sql.append(" and di.enteredtext is null");
        }
        logger.debug("getDataSetNumber :: " +sql);
        DataSet mindata = getQueryProcessor().getSqlDataSet(sql.toString());
        dataItemCnt = Integer.parseInt(mindata.getValue(0, "mindataset"));

        return dataItemCnt;
    }

    /**
     *  This method will get the max empty replicate  number available for the sample and dataset .<br><br>
     *  instrumentID IS NOT USED IN THIS

     * @param sampleID  sample ID
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @param dataSet
     * @return	replicate id .
     */
    public int getMaxReplicateId(String sampleID, String instrumentID, String instrFieldID, int dataSet) {

        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();
        sql.append(" select nvl(max(di.replicateid), 0) maxreplicate");
        sql.append(" from  sdidataitem di");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
        sql.append(" and di.dataset  = ").append(dataSet).append(" ");
        logger.debug("getMaxReplicateId : " + sql.toString());
        DataSet mindata = getQueryProcessor().getSqlDataSet(sql.toString());
        dataItemCnt = Integer.parseInt(mindata.getValue(0, "maxreplicate"));
        return dataItemCnt;
    }

    /**
     *  This method will get the max empty replicate  number available for the sample ,dataset,paramListId,paramListVersionId and variantId
     * @param sampleID  sample ID
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @param paramListId  parameter list id.
     * @param paramListVersionId  parameter list version id.
     * @param variantId  parameter variant id.
     * @param dataSet
     * @return	replicate id .
     */
    public int getMaxReplicateId(String sampleID, String instrFieldID, int dataSet, String paramListId, String paramListVersionId, String variantId) {
        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();
        sql.append(" select nvl(max(di.replicateid), 0) maxreplicate");
        sql.append(" from  sdidataitem di");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
        sql.append(" and di.dataset  = ").append(dataSet).append(" ");
        sql.append(" and di.paramlistid  = '").append(paramListId).append("' ");
        sql.append(" and di.paramlistversionid  = '").append(paramListVersionId).append("' ");
        sql.append(" and di.variantid  = '").append(variantId).append("'");

        DataSet mindata = getQueryProcessor().getSqlDataSet(sql.toString());
        dataItemCnt = Integer.parseInt(mindata.getValue(0, "maxreplicate"));

        return dataItemCnt;
    }

    //    /**
//     *  This method will get the max empty replicate  number available for the sample , stepNo,instrumentField,dataset .
//     * @param sampleId  sample ID
//     * @param instrumentField  mandatory field of instrument type present in the pl.
//     * @param dataset
//     * @param stepNo  step no
//     * @return	replicate id .
//     */
//    public int getMaxReplicateIdUsingStepNo(String sampleId, String instrumentField, int dataset ){
//        int maxReplicate = 0;
//        String queryStr = "select max(sdi.replicateid) maxrep from sdidataitem sdi " +
//                "join sdidata sd on sd.keyid1 = sdi.keyid1 and sd.dataset = sdi.dataset and sd.PARAMLISTID = sdi.PARAMLISTID " +
//                "and sd.paramlistversionid = sdi.paramlistversionid and sd.variantid = sdi.variantid " +
//                "join sdiworkitem swi on swi.keyid1 = sd.keyid1 and swi.workitemid = sd.sourceworkitemid " +
//                "and swi.workiteminstance = sd.sourceworkiteminstance " +
//                "where sdi.sdcid = 'Sample' and swi.u_stepno = '" + stepNo + "' " +
//                "and sdi.keyid1 = '" + sampleId + "' and sdi.INSTRUMENTFIELDID = '" + instrumentField + "' " +
//                "and sdi.dataset = " + dataset;
//        DataSet dsQuery = getQueryProcessor().getSqlDataSet(queryStr);
//
//        if(dsQuery.getRowCount()> 0){
//            maxReplicate = Integer.parseInt(dsQuery.getValue(0, "maxrep"));
//        }
//
//        return maxReplicate;
//    }
    /*

    public HashMap<String, String> getSDIDataItemId(String sampleId, String paramListId, String paramListVersionId,
                                                    String variantId, String dataSet){
        HashMap<String, String> fieldAndDataItemIdMap = new HashMap<String, String>();

        StringBuffer queryStr = new StringBuffer();

        queryStr.append("select distinct keyid1 ||'_'|| instrumentfieldid ||'_'|| dataset ||'_'|| replicateid instrumentfieldid, sdidataitemid from sdidataitem ");
        queryStr.append("where sdcid = 'Sample' and keyid1 in (" + sampleId + ") ");
        queryStr.append("and paramlistid = '" + paramListId + "' and paramlistversionid = '" + paramListVersionId + "' ");
        queryStr.append("and variantid = '" + variantId + "' and dataset in (" + dataSet + ") ");

        //System.out.println("getSDIDataItemId : \n" + queryStr);

        DataSet dsSdiDataItemId = getQueryProcessor().getSqlDataSet(queryStr.toString());

        for(int dsIndex = 0; dsIndex < dsSdiDataItemId.getRowCount(); dsIndex++){
            fieldAndDataItemIdMap.put(dsSdiDataItemId.getValue(dsIndex, "instrumentfieldid"), dsSdiDataItemId.getValue(dsIndex, "sdidataitemid"));
        }

        return fieldAndDataItemIdMap;
    }

*/
    public  HashMap<String, String> getSDIDataItemId(String sampleId, String paramListId, String paramListVersionId, String variantId, String dataSet){
        HashMap<String, String> fieldAndDataItemIdMap = new HashMap<String, String>();

        StringBuffer queryStr = new StringBuffer();

        queryStr.append("select instrumentfieldid, sdidataitemid from sdidataitem ");
        queryStr.append("where sdcid = 'Sample' and keyid1 = '" + sampleId + "' ");
        queryStr.append("and paramlistid = '" + paramListId + "' and paramlistversionid = '" + paramListVersionId + "' ");
        queryStr.append("and variantid = '" + variantId + "' and dataset = '" + dataSet + "' ");
        logger.debug(" : \n" + queryStr);
        DataSet dsSdiDataItemId = getQueryProcessor().getSqlDataSet(queryStr.toString());
        for(int dsIndex = 0; dsIndex < dsSdiDataItemId.getRowCount(); dsIndex++){
            fieldAndDataItemIdMap.put(dsSdiDataItemId.getValue(dsIndex, "instrumentfieldid"), dsSdiDataItemId.getValue(dsIndex, "sdidataitemid"));
        }

        return fieldAndDataItemIdMap;
    }



    public HashMap<String, String> getSDIDataItemDetails(String sampleId, String paramListId, String paramListVersionId
            ,String variantId, String dataSet){
        HashMap<String, String> fieldAndDataItemDetailsMap = new HashMap<String, String>();

        StringBuffer queryStr = new StringBuffer();
        queryStr.append("select instrumentfieldid, paramid, paramtype from sdidataitem ");
        queryStr.append("where sdcid = 'Sample' and keyid1 = '" + sampleId + "' ");
        queryStr.append("and paramlistid = '" + paramListId + "' and paramlistversionid = '" + paramListVersionId + "' ");
        queryStr.append("and variantid = '" + variantId + "' and dataset = '" + dataSet + "' ");
        logger.debug("getSDIDataItemDetails : \n" + queryStr);
        DataSet dsSdiDataItemId = getQueryProcessor().getSqlDataSet(queryStr.toString());
        for(int dsIndex = 0; dsIndex < dsSdiDataItemId.getRowCount(); dsIndex++){
            fieldAndDataItemDetailsMap.put(dsSdiDataItemId.getValue(dsIndex, "instrumentfieldid"),
                    dsSdiDataItemId.getValue(dsIndex, "paramid") + "~" + dsSdiDataItemId.getValue(dsIndex, "paramtype"));
        }

        return fieldAndDataItemDetailsMap;
    }



    /**
     *  This method will get the max empty dataset number available for the sample , the step number
     *  instrumentID and the instrument field id present in the pl.
     * @param sampleID  sample ID
     * @param wiUserSequence  step number
     * @param instrumentID  instrument ID
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @return	available dataset number .
     */
    public  int checkRetestDataset(String sampleID, String instrumentID, String instrFieldID, String wiUserSequence) {

        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();
        SafeSQL safeSQL = new SafeSQL();

        sql.append(" select nvl(max(di.dataset), 0) maxdataset");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" and di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '"+sampleID+"' ");
        sql.append(" and i.instrumentid = '"+instrumentID+"' ");
        sql.append(" and di.instrumentfieldid  = '"+instrFieldID+"' ");
//        sql.append(" and wi.u_stepno  = '").append(wiUserSequence).append("' ");
        sql.append(" and di.enteredtext is null");

        DataSet mindata =  getQueryProcessor().getSqlDataSet(sql.toString());
        //dsRequests.showData();
        dataItemCnt = Integer.parseInt(mindata.getValue(0, "maxdataset"));
        //System.out.println("dataItemCnt =" +dataItemCnt);

        return dataItemCnt;
    }

    /**
     *  This method will get the max dataset number available for the sample , the step number
     *  instrumentID and the instrument field id present in the pl.
     * @param sampleID  sample ID
     * @param wiUserSequence  step number
     * @param instrumentID  instrument ID
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @return	available dataset number .
     */
    public int getMaxDataSetNumber(String sampleID, String instrumentID, String instrFieldID, String wiUserSequence) {
        StringBuffer sql = new StringBuffer();

        sql.append(" select nvl(max(di.dataset), 0) maxdataset");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" and di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
//        sql.append(" and wi.u_stepno  = '").append(wiUserSequence).append("' ");
        //sql.append(" and di.enteredtext is null");
        logger.debug("getMaxDataSetNumber :: " + sql);
        DataSet maxDataSet = getQueryProcessor().getSqlDataSet(sql.toString());

        return Integer.parseInt(maxDataSet.getValue(0, "maxdataset"));
    }

    /**
     *  This method will get the max dataset number, source workitem id and source workitem instance available for the sample , the step number
     *  instrumentID and the instrument field id present in the pl.
     * @param sampleID  sample ID
     * @param wiUserSequence  step number
     * @param instrumentID  instrument ID
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @return	propertylist dataset number,SOURCEWORKITEMID, SOURCEWORKITEMINSTANCE.
     */
    public PropertyList getMaxDataSetNumberAndWorkItem(String sampleID, String instrumentID, String instrFieldID, String wiUserSequence) {
        PropertyList pl = new PropertyList();
        StringBuffer sql = new StringBuffer();
        sql.append("select sd.SOURCEWORKITEMID, sd.SOURCEWORKITEMINSTANCE, max(sd.dataset) maxdataset");
        /*      Commented for ALM #5440. Get only for the step no given and for the PL with instrument field
        Added additional tables and where clause
        sql.append(" from sdidata sd");
        sql.append(" where sd.sdcid = 'Sample'");
        sql.append(" and sd.keyid1 = '").append(sampleID).append("' ");
        sql.append(" group by sd.sourceworkitemid, sd.sourceworkiteminstance order by sd.SOURCEWORKITEMINSTANCE desc");
        */
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '"+sampleID+"' ");
        sql.append(" and i.instrumentid = '"+instrumentID+"' ");
        sql.append(" and di.instrumentfieldid  = '"+instrFieldID+"' ");
        //sql.append(" and wi.u_stepno  = '").append(wiUserSequence).append("' ");
        sql.append(" and wi.usersequence  = '"+wiUserSequence+"' ");
        sql.append(" group by sd.sourceworkitemid, sd.sourceworkiteminstance order by sd.SOURCEWORKITEMINSTANCE desc");


        DataSet maxdata = getQueryProcessor().getSqlDataSet(sql.toString());
        pl.setProperty("maxdataset", maxdata.getValue(0, "maxdataset"));
        pl.setProperty("sourceworkitemid", maxdata.getValue(0, "sourceworkitemid"));
        pl.setProperty("sourceworkiteminstance", maxdata.getValue(0, "sourceworkiteminstance"));
        return pl;
    }


    /**
     *  This method will get the max dataset number available for the sample ,
     *  instrumentID and the instrument field id present in the pl.
     * emptyDS if used will return the empty dataset number.
     * @param sampleID  sample ID
     * @param emptyDS  if true will return the empty dataset number.
     * @param instrumentID  instrument ID
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @return	available dataset number .
     */
    public int getMaxDataSetNumber(String sampleID, String instrumentID, String instrFieldID, boolean emptyDS) {
        StringBuffer sql = new StringBuffer();

        sql.append(" select nvl(max(di.dataset), 0) maxdataset");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" and di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
        if (emptyDS) {
            sql.append(" and di.enteredtext is null");
        }
        logger.debug("getMaxDataSetNumber :: " + sql);
        DataSet maxDataSet = getQueryProcessor().getSqlDataSet(sql.toString());

        return Integer.parseInt(maxDataSet.getValue(0, "maxdataset"));
    }

    /**
     *  This method will get the max dataset number,sourceworkitemid and sourceworkiteminstance  available for the sample ,
     *  instrumentID and the instrument field id present in the pl.  INSTRUMENTID can be null, if null won't be used.
     * emptyDS if used will return the empty dataset number.
     * @param sampleID  sample ID
     * @param instrumentID  instrument ID , can be passed as "null"
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @return	propertylist.
     */
    public PropertyList getMaxDataSetNumber(String sampleID, String instrumentID, String instrFieldID) {
        PropertyList pl = new PropertyList();
        int dataSet = 0;
        StringBuffer sql = new StringBuffer();
        sql.append(" select sd.sourceworkitemid, sd.sourceworkiteminstance, nvl(max(di.dataset), 0) maxdataset");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
        sql.append(" group by sd.sourceworkitemid, sd.sourceworkiteminstance ");
        logger.debug("getMaxDataSetNumber :: " + sql);
        DataSet maxdata = getQueryProcessor().getSqlDataSet(sql.toString());
        //dsRequests.showData();
        pl.setProperty("maxdataset", maxdata.getValue(0, "maxdataset"));
        pl.setProperty("sourceworkitemid", maxdata.getValue(0, "sourceworkitemid"));
        pl.setProperty("sourceworkiteminstance", maxdata.getValue(0, "sourceworkiteminstance"));
        //dataSet = Integer.parseInt(maxdata.getValue(0, "maxdataset"));
        return pl;
    }

    /**
     *  This method will get the max dataset number,sourceworkitemid and sourceworkiteminstance  available for the sample ,instrument field.<br><br>
     *  instrFieldID can be passed as null.
     * @param sampleID  sample ID
     * @param instrFieldID  mandatory field of instrument type present in the pl, can be null.
     * @return	propertylist.
     */
    public PropertyList getMaxDataSetNumber(String sampleID, String instrFieldID) {
        PropertyList pl = new PropertyList();
        StringBuffer sql = new StringBuffer();

        if(instrFieldID != null) {
            sql.append(" select sd.sourceworkitemid, sd.sourceworkiteminstance, nvl(max(di.dataset), 0) maxdataset");
            sql.append(" from  sdidataitem di  join sdidata sd");
            sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
            sql.append(" and sd.paramlistid = di.paramlistid");
            sql.append(" and sd.paramlistversionid = di.paramlistversionid");
            sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
            sql.append(" where di.sdcid = 'Sample'");
            sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
            sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
            sql.append(" group by sd.sourceworkitemid, sd.sourceworkiteminstance ");
        }
        else {
            sql.append("select sd.SOURCEWORKITEMID, sd.SOURCEWORKITEMINSTANCE, max(sd.dataset) maxdataset");
            sql.append(" from sdidata sd");
            sql.append(" where sd.sdcid = 'Sample'");
            sql.append(" and sd.keyid1 = '").append(sampleID).append("' ");
            sql.append(" group by sd.sourceworkitemid, sd.sourceworkiteminstance order by sd.SOURCEWORKITEMINSTANCE desc");

        }
        logger.debug("getMaxDataSetNumber :: " + sql);
        DataSet maxdata = getQueryProcessor().getSqlDataSet(sql.toString());
        pl.setProperty("maxdataset", maxdata.getValue(0, "maxdataset"));
        pl.setProperty("sourceworkitemid", maxdata.getValue(0, "sourceworkitemid"));
        pl.setProperty("sourceworkiteminstance", maxdata.getValue(0, "sourceworkiteminstance"));
        return pl;
    }

    /**
     *  This method will get the max dataset number,sourceworkitemid and sourceworkiteminstance  available for the sample , parameter list
     *  instrumentID and the instrument field id present in the pl.<br><br>
     * instrumentID can be passed as null.
     * @param sampleID  sample ID
     * @param paramListId parameter list id
     * @param paramListVersionId parameter list version
     * @param variantId parameter list variant
     * @param instrumentID  instrument ID , if not used pass null.
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @return	available dataset number .
     */
    public PropertyList getMaxDataSetNumber(String sampleID, String instrumentID, String instrFieldID, String paramListId, String paramListVersionId, String variantId) {
        PropertyList pl = new PropertyList();
        StringBuffer sql = new StringBuffer();
        sql.append(" select sd.sourceworkitemid, sd.sourceworkiteminstance, nvl(max(di.dataset), 0) maxdataset");
        sql.append(" from  sdidataitem di");
        sql.append(" join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and di.paramlistid = '").append(paramListId).append("' ");
        sql.append(" and di.paramlistversionid = '").append(paramListVersionId).append("' ");
        sql.append(" and di.variantid = '").append(variantId).append("' ");
        sql.append(" and trim(di.instrumentfieldid)  = '").append(instrFieldID).append("' ");

        sql.append(" group by sd.sourceworkitemid, sd.sourceworkiteminstance ");
        logger.debug("getMaxDataSetNumber :: " + sql);
        DataSet maxdata = getQueryProcessor().getSqlDataSet(sql.toString());
        pl.setProperty("maxdataset", maxdata.getValue(0, "maxdataset"));
        pl.setProperty("sourceworkitemid", maxdata.getValue(0, "sourceworkitemid"));
        pl.setProperty("sourceworkiteminstance", maxdata.getValue(0, "sourceworkiteminstance"));

        return pl;
    }



    /**
     *  This method will get the max empty dataset number available for the sample , instrumentID ,paramID,wiStepNum ,variantID combination
     *  instrumentID and the instrument field id present in the pl.<br><br>
     * @param sampleID  sample ID
     * @param wiStepNum  step number
     * @param variantID parameter list variant
     * @param instrumentID  instrument ID , if not used pass null.
     * @param paramID  mandatory field of instrument type present in the pl.
     * @return	available dataset number .
     */
    public int getAvailableDataSets(String sampleID, String instrumentID, String paramID, String wiStepNum, String variantID) {

        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();
        sql.append(" select count(*) datasetcount");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" and di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(paramID).append("' ");
        //sql.append(" and di.paramid  = '").append(paramID).append("' ");
        sql.append(" and di.variantid = '").append(variantID).append("' ");
//        sql.append(" and wi.u_stepno  = '").append(wiStepNum).append("' ");
        // sql.append(" and di.variantid = '").append(variantID).append("' ");
        sql.append(" and di.enteredtext is null");

        DataSet cntdata = getQueryProcessor().getSqlDataSet(sql.toString());
        //System.out.println("=======================================================");
        //cntdata.showData();
        dataItemCnt = cntdata.getInt(0, "datasetcount");
        return dataItemCnt;
    }


    /**
     *  This method will get the max dataset number available for the sample ,instrumentID and the instrument field id present in the pl.
     * @param sampleID  sample ID
     * @param instrumentID  instrument ID , can be passed as "null"
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @return	maxdataset dataset number.
     */
    public int getMaxDataSetNumberOnly(String sampleID, String instrumentID, String instrFieldID) {
        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();

        sql.append(" select nvl(max(di.dataset), 0) maxdataset");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" and di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
        logger.debug("getMaxDataSetNumberOnly :: " + sql);
        DataSet mindata = getQueryProcessor().getSqlDataSet(sql.toString());
        dataItemCnt = Integer.parseInt(mindata.getValue(0, "maxdataset"));
        return dataItemCnt;
    }

    /**
     *  This method will get the max dataset number available for the sample  the instrument field id present in the pl.
     * @param sampleID  sample ID
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @return	maxdataset dataset number.
     */
    public int getMaxDataSetNumberOnly(String sampleID, String instrFieldID) {
        StringBuffer sql = new StringBuffer();
        sql.append("select max(sd.dataset) maxdataset ");
        sql.append("from sdidataitem sdi ");
        sql.append("join sdidata sd on sd.keyid1 = sdi.keyid1 and sd.dataset = sdi.dataset and sd.paramlistid = sdi.paramlistid ");
        sql.append("and sd.paramlistversionid = sdi.paramlistversionid and sd.variantid = sdi.variantid ");
        sql.append(" where sd.sdcid = 'Sample'");
        sql.append(" and sd.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and sdi.instrumentfieldid = '").append(instrFieldID).append("' ");
        logger.debug("getMaxDataSetNumberOnly :: " + sql);
        DataSet maxdata = getQueryProcessor().getSqlDataSet(sql.toString());
        return Integer.parseInt(maxdata.getValue(0, "maxdataset"));
    }


    /**
     *  This method will get the max dataset number,sourceworkitemid and sourceworkiteminstance  available for the sample ,
     *  instrumentID and the instrument field id present in the pl and step no.
     * @param sampleID  sample ID
     * @param instrumentID  instrument ID
     * @param wiUserSequence  step number
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @return	propertylist.
     */
    public PropertyList getMaxDataSetDetails(String sampleID, String instrumentID, String instrFieldID, String wiUserSequence) {
        PropertyList pl = new PropertyList();
        int dataSet = 0;
        StringBuffer sql = new StringBuffer();
        sql.append(" select sd.sourceworkitemid, sd.sourceworkiteminstance, nvl(max(di.dataset), 0) maxdataset");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" and di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
//        sql.append(" and wi.u_stepno  = '").append(wiUserSequence).append("' group by sd.sourceworkitemid, sd.sourceworkiteminstance ");

        DataSet maxdata = getQueryProcessor().getSqlDataSet(sql.toString());
        pl.setProperty("maxdataset", maxdata.getValue(0, "maxdataset"));
        pl.setProperty("sourceworkitemid", maxdata.getValue(0, "sourceworkitemid"));
        pl.setProperty("sourceworkiteminstance", maxdata.getValue(0, "sourceworkiteminstance"));

        return pl;
    }

    /**
     *  This method will get the max replicate number available for the sample , dataset
     *  instrumentID and the instrument field id present in the pl.
     * @param sampleID  sample ID
     * @param dataset
     * @param instrumentID  instrument ID
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @return	available replicate number .
     */
    public int getMaxReplicateNumber(String sampleID, String instrumentID, String instrFieldID, String dataset) {

        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();
        sql.append(" select nvl(max(di.replicateid), 0) maxreplicate");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" and di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
        sql.append(" and di.dataset = '"+dataset+"' ");

        //System.out.println("getMaxDataSetNumber : " + sql.toString());

        DataSet mindata = getQueryProcessor().getSqlDataSet(sql.toString());

        dataItemCnt = Integer.parseInt(mindata.getValue(0, "maxreplicate"));

        return dataItemCnt;
    }

    /**
     *  This method will get the max replicate number available for the sample , dataset, instrumentID and the instrument field id present in the pl.
     *  If retest then will return the empty replicate number.
     * @param sampleID  sample ID
     * @param dataset
     * @param instrumentID  instrument ID
     * @param instrFieldID  mandatory field of instrument type present in the pl.
     * @param retest  if true, will check if the enteredtext is not null for this replicate number.
     * @return	available replicate number .
     */
    public int getMaxReplicateNumber(String sampleID, String instrumentID, String instrFieldID, String dataset, boolean retest) {

        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();
        sql.append(" select nvl(max(di.replicateid), 0) maxreplicate");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" and di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
        sql.append(" and di.dataset = '"+dataset+"' ");
        if (retest) {
            sql.append(" and di.enteredtext is not null");
        } else {
            sql.append(" and di.enteredtext is null");
        }
        logger.debug("getMaxDataSetNumber : " + sql.toString());
        DataSet mindata = getQueryProcessor().getSqlDataSet(sql.toString());
        dataItemCnt = Integer.parseInt(mindata.getValue(0, "maxreplicate"));

        return dataItemCnt;
    }


    /**
     *  This method will get the min dataset number available for the sample , sParameterList ,sParameterListVersion,sParameterListVariant ,userSequence
     *  instrumentID .
     * @param sSampleID  sample ID
     * @param sParameterList parameter list id
     * @param sParameterListVersion parameter list version
     * @param sParameterListVariant parameter list variant
     * @param userSequence step number
     * @param sInstrumentID  instrument ID  can be null
     * @return	interger  mindataset number .
     */

    public Integer getMinDataset(String sSampleID, String sInstrumentID, String sParameterList, String sParameterListVersion, String sParameterListVariant, String userSequence) {

        StringBuffer sbPLInfo = new StringBuffer();
        sbPLInfo.append("select nvl(min(di.dataset), 0) as mindataset ");
        sbPLInfo.append("from  sdidataitem di ");
        sbPLInfo.append("join sdidata sd on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 ");
        sbPLInfo.append("and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3 ");
        sbPLInfo.append("and sd.paramlistid = di.paramlistid and sd.paramlistversionid = di.paramlistversionid ");
        sbPLInfo.append("and sd.variantid = di.variantid and sd.dataset = di.dataset ");
        sbPLInfo.append("join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance ");
        sbPLInfo.append("and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid ");
        sbPLInfo.append("and (wi.workitemid != wi.groupid or wi.groupid is null) ");
        sbPLInfo.append("join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid ");
        sbPLInfo.append("and sd.variantid = pl.variantid join instrument i on i.instrumenttype = pl.s_instrumenttype ");
        sbPLInfo.append("and i.instrumentmodelid = pl.s_instrumentmodel or pl.s_instrumentmodel is null ");
        sbPLInfo.append("where upper(di.instrumentfieldid) = 'FILE NAME' ");
        sbPLInfo.append("and di.sdcid = 'Sample' ");
        sbPLInfo.append("and di.keyid1 = '");
        sbPLInfo.append(sSampleID);
        sbPLInfo.append("' ");
        sbPLInfo.append("and di.paramlistid = '");
        sbPLInfo.append(sParameterList);
        sbPLInfo.append("' ");
        sbPLInfo.append("and di.paramlistversionid = '");
        sbPLInfo.append(sParameterListVersion);
        sbPLInfo.append("' ");
        sbPLInfo.append("and di.variantid = '");
        sbPLInfo.append(sParameterListVariant);
        sbPLInfo.append("' ");
//        sbPLInfo.append("and wi.u_stepno  = '");
        sbPLInfo.append(userSequence);
        sbPLInfo.append("' ");
        sbPLInfo.append("and di.enteredtext is null");
        logger.debug("\nQuery to get Min Dataset : "+sbPLInfo.toString());
        DataSet dsPLInfo = getQueryProcessor().getSqlDataSet(sbPLInfo.toString());

        Integer iMinDataset = dsPLInfo.getInt(0, "mindataset");

        return iMinDataset;

    }

    /**
     *  This method will get the min empty dataset number available for the sample ,wiStepNum,instrumentID,paramID and variantID
     * @param sampleID  sample ID
     * @param paramID  mandatory instrument field present in pl.
     * @param wiStepNum step number
     * @param variantID parameter list variant
     * @param instrumentID  instrument ID
     * @return	interger  mindataset number .
     */

    public Integer getMinDatasetNum(String sampleID, String instrumentID, String paramID, String wiStepNum, String variantID) {

        StringBuffer sql = new StringBuffer();
        sql.append(" select nvl(min(di.dataset), 0) mindataset");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" and di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(paramID).append("' ");
        //sql.append(" and di.paramid  = '").append(paramID).append("' ");
        sql.append(" and di.variantid = '").append(variantID).append("' ");
//        sql.append(" and wi.u_stepno  = '").append(wiStepNum).append("' ");
        sql.append(" and di.enteredtext is null");

        DataSet mindata = getQueryProcessor().getSqlDataSet(sql.toString());
        //dsRequests.showData();
        return Integer.parseInt(mindata.getValue(0, "mindataset"));
    }

    /**
     *  This method will get the min empty dataset number available for the sample ,instrumentID,paramID
     * @param sampleID  sample ID
     * @param instrFieldID  mandatory instrument field present in pl.
     * @param instrumentID  instrument ID
     * @return	interger  mindataset number .
     */

    public Integer getMinDatasetNum(String sampleID, String instrumentID, String instrFieldID) {
        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();

        sql.append(" select nvl(min(di.dataset), 0) mindataset");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" and di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
        sql.append(" and di.enteredtext is null");

        DataSet mindata = getQueryProcessor().getSqlDataSet(sql.toString());
        dataItemCnt = Integer.parseInt(mindata.getValue(0, "mindataset"));
        return dataItemCnt;
    }



    /**
     *  This method will get the min dataset number,step number available for the sample , instrument field
     *  instrumentID .
     * @param sampleId  sample ID
     * @param instrumentField instruemnt field of the instrument present in the pl
     * @param instrumentId  instrument ID
     * @return	PropertyList  usersequence , dataset .
     */
    public PropertyList getMinUserSequenceAndDataSet(String sampleId, String instrumentId, String instrumentField) {
        String userSequenceNo = "";
        String dataset = "";

        PropertyList pl = new PropertyList();

        StringBuilder query = new StringBuilder();
        query.append("select nvl(min(wi.usersequence), 0) usersequence, min(di.dataset) dataset from SDIWORKITEM wi ");
        query.append("join sdidata sd on wi.workitemid = sd.sourceworkitemid and wi.keyid1 = sd.keyid1 ");
        query.append("join sdidataitem di on sd.keyid1 = di.keyid1 and sd.dataset = di.dataset and sd.paramlistid = di.paramlistid and sd.paramlistversionid = di.paramlistversionid and sd.variantid = di.variantid ");
        query.append("join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid and sd.variantid = pl.variantid ");
        query.append("join instrument i on i.instrumenttype = pl.s_instrumenttype and (i.instrumentmodelid = pl.s_instrumentmodel or pl.s_instrumentmodel is null) ");
        query.append("where wi.sdcid ='Sample' and wi.keyid1 = '" + sampleId + "' and i.instrumentid = '" + instrumentId + "' and di.instrumentfieldid = '" + instrumentField + "' ");
        query.append("and di.enteredtext is null");

        DataSet queryResults = getQueryProcessor().getSqlDataSet(query.toString());

        if (queryResults.getRowCount() > 0) {
            userSequenceNo = queryResults.getValue(0, "usersequence");
            dataset = queryResults.getValue(0, "dataset");

            pl.setProperty("usersequence", userSequenceNo);
            pl.setProperty("dataset", dataset);
        }

        return pl;
    }

    /**
     *  This method will get the min filled dataset number for the sample , instrument field
     * @param sampleID  sample ID
     * @param instrFieldID instrument field of the instrument present in the pl
     * @return	integer   dataset .
     */
    public int getMinParsedDataSetNumber(String sampleID, String instrFieldID) {
        int dataSetNumber = 0;

        StringBuffer sql = new StringBuffer();
        sql.append("select nvl(min(di.dataset), 0) minparseddataset");
        sql.append(" from sdidataitem di");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and di.instrumentfieldid = '").append(instrFieldID).append("' ");
        sql.append(" and di.enteredtext is not null");

        DataSet mindata = getQueryProcessor().getSqlDataSet(sql.toString());
        dataSetNumber = Integer.parseInt(mindata.getValue(0, "minparseddataset"));

        return dataSetNumber;
    }


    /**
     *  This method will check if the dataset (and replicate) is filled  for the sample , the step number ,instrument , dataset,
     * replicate id can be null
     * the instrument field id present in the pl  can be null.
     * @param sampleID  sample ID
     * @param wiUserSequence  step number can be NULL
     * @param instrumentID  instrument ID
     * @param replicateId can be NULL
     * @param instrFieldID  mandatory field of instrument type present in the pl can be NULL.
     * @return	boolean .
     */
    public boolean isRerunOrReTest(String sampleID, String instrumentID, String instrFieldID, String wiUserSequence, String dataset, String replicateId) {

        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();
        sql.append(" select count(1) datasetcount");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" and di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        //
        if(instrFieldID != null)
            sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
        //sql.append(" and wi.usersequence  = '").append(wiUserSequence).append("' ");
//        if(wiUserSequence != null)
//            sql.append(" and wi.u_stepno  = '").append(wiUserSequence).append("' ");
        sql.append(" and di.dataset = '"+dataset+"' ");
        if(replicateId!= null){
            sql.append(" and di.replicateid = '"+replicateId+"' ");
        }
        sql.append(" and di.enteredtext is not null");
        logger.debug("Inside isRerunOrReTest :::: " + sql.toString());
        DataSet cntdata =getQueryProcessor().getSqlDataSet(sql.toString());
        dataItemCnt = cntdata.getInt(0, "datasetcount");
        if (dataItemCnt > 0){
            return true;
        } else {
            return false;
        }
    }



    /**
     *  This method will check if the given sample exists in the system.
     * @param sampleID  sample ID
     * @return	true/false.
     */
    public  boolean isValidSample(String sampleID) {
        if(StringUtils.isBlank(sampleID)){
            return false;
        }

        StringBuffer sql = new StringBuffer();
        sql.append(" select s_sampleid");
        sql.append(" from s_sample");
        sql.append(" where s_sampleid = '");
        sql.append(sampleID).append("' ");
        //System.out.println(sql);
        DataSet dsSample = getQueryProcessor().getSqlDataSet(sql.toString());


        if (dsSample.getRowCount()<= 0){
            return false;
        }

        return true;


    }

    /**
     *  This method will get the createby information for the given sample and check the existence of the sample in the system.
     * @param sampleID  sample ID
     * @return	PropertyList containing the details.
     */
    public PropertyList getSampleDetails(String sampleID) {
        PropertyList sampleDetails = new PropertyList();
        sampleDetails.setProperty("isvalidsample", "No");
        sampleDetails.setProperty("analystid", "No_Record_Found");

        if(!StringUtils.isBlank(sampleID)){
            //Updated on the 4th of April 2023 to make the sampleID case-insensitive on Jira Item #PG07735-6701
            String sampleIdUpperCase = sampleID.toUpperCase();
            StringBuffer sql = new StringBuffer();
            sql.append("select createby");
            sql.append(" from s_sample");
            sql.append(" where s_sampleid = '");
            sql.append(sampleIdUpperCase).append("'");

            DataSet dsSample = getQueryProcessor().getSqlDataSet(sql.toString());

            if (dsSample.getRowCount() > 0) {

                // Changed by jdaluz for JIRA #PG07735-8658 : Added filter for "(system)" user to prevent SDMS Drivers from failing when parsing Analyst ID field to samples created by Instruments
                Map<String,String> validUsers = new HashMap<String,String>();
                validUsers = getAllUsers();
                String createBy = null;

                //*
                if(validUsers.get(dsSample.getValue(0, "createby").toLowerCase()) != null){
                    createBy = dsSample.getValue(0, "createby");
                }
                //*/

                sampleDetails.setProperty("isvalidsample", "Yes");
                sampleDetails.setProperty("createby", createBy);
                //sampleDetails.setProperty("createby", dsSample.getValue(0, "createby"));
            }

        }
        return sampleDetails;
    }


    /**
     *  This method will get the securitydepartment,securityuser information for the given sample .
     * @param sampleID  sample ID
     * @return	PropertyList containing the details.
     */
    public PropertyList getSecurityDetails(String sampleID) {
        PropertyList pl = new PropertyList();
        StringBuffer sql = new StringBuffer();
        sql.append("select sd.securitydepartment, sd.securityuser");
        sql.append(" from sdidata sd");
        sql.append(" where sd.sdcid = 'Sample'");
        sql.append(" and sd.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and sd.dataset  = 1 ");

        //System.out.println("getSecurityDetails :: " + sql);

        DataSet maxdata = getQueryProcessor().getSqlDataSet(sql.toString());
        //dsRequests.showData();
        pl.setProperty("securitydepartment", maxdata.getValue(0, "securitydepartment"));
        pl.setProperty("securityuser", maxdata.getValue(0, "securityuser"));

        return pl;
    }

//    /**
//     *  This method will check if the step no and sample combination is valid .
//     * @param sampleID  sample ID
//     * @return	true/false.
//     */
//    public  boolean isValidStepNo(String sampleID,String stepNo) {
//        boolean isValidSaple = true;
//        SafeSQL safeSQL = new SafeSQL();
//
//        StringBuffer sql = new StringBuffer();
//        sql.append(" select keyid1");
//        sql.append(" from sdiworkitem");
//        sql.append(" where sdcid = 'Sample'");
//        sql.append(" and keyid1 = '").append(sampleID).append("'");
//        //sql.append(" and u_stepno  = '").append(stepNo).append("' ");
//        //sql.append(" and usersequence  = '").append(stepNo).append("'");
//        //System.out.println(sql);
//
//
//        //System.out.println(sql);
//        DataSet dsSample =  getQueryProcessor().getSqlDataSet(sql.toString());
//
//        if (dsSample.getRowCount()<= 0){
//            isValidSaple =  false;
//        }
//
//        return isValidSaple;
//    }


    /**
     *  This method will get the number of methods in the sample .
     * @param sampleId  sample ID
     * @return	int.
     */
    public  int getTestMethodCount(String sampleId) {
        StringBuffer sql = new StringBuffer();
        sql.append("select wi.workitemid from SDIWORKITEM wi ");
        sql.append("where wi.sdcid ='Sample' and wi.KEYID1 = '"+ sampleId +"'");

        //System.out.println(sql+" : "+ utils);
        DataSet dsSample =  getQueryProcessor().getSqlDataSet(sql.toString());
        return dsSample.getRowCount();
    }


    /**
     *  This method will get the number of methods in the given sample ,instrument and having the instrument field .<br>
     *  If emptyText then will return only the methods which has datasets empty.
     * @param sampleId  sample ID
     * @param instrumentId  instrument Id
     * @param instrumentField  instrument Field.
     * @param emptyText  boolean
     * @return	int.
     */
    public int getTestMethodCount(String sampleId, String instrumentId, String instrumentField,boolean emptyText) {
        StringBuffer sql = new StringBuffer();
        sql.append("select distinct wi.workitemid, wi.workiteminstance from sdiworkitem wi ");
        sql.append("join sdidata sd on wi.workitemid = sd.sourceworkitemid ");
        sql.append("join sdidataitem di on sd.keyid1 = di.keyid1 and sd.dataset = di.dataset and sd.paramlistid = di.paramlistid and sd.paramlistversionid = di.paramlistversionid and sd.variantid = di.variantid ");
        sql.append("join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid and sd.variantid = pl.variantid ");
        sql.append("join instrument i on i.instrumenttype = pl.s_instrumenttype and (i.instrumentmodelid = pl.s_instrumentmodel or pl.s_instrumentmodel is null) ");
        sql.append("where wi.sdcid ='Sample' and wi.keyid1 = '" + sampleId + "' and i.instrumentid = '" + instrumentId + "' ");
        sql.append("and di.instrumentfieldid = '" + instrumentField + "' ");
        if(emptyText)
            sql.append("and di.enteredtext is null");

        DataSet dsSample =  getQueryProcessor().getSqlDataSet(sql.toString());
        return dsSample.getRowCount();
    }


    /**
     *  This method will return all the instrument fields under the instrument .
     * @param instrumentId  instrument Id
     * @return	map containing the instrument fields.
     */
    public Map<String, String> getInstrumentFields(String instrumentId){
        Map<String, String> instrumentFields = new HashMap<String, String>();
        String queryToGetFields = "select f.INSTRUMENTFIELDID field from INSTRUMENT i " +
                "join INSTRUMENTTYPE t on i.INSTRUMENTTYPE = t.INSTRUMENTTYPEID " +
                "join INSTRUMENTTYPEFIELD f on f.INSTRUMENTTYPEID = i.INSTRUMENTTYPE " +
                "where i.INSTRUMENTID = '" +  instrumentId +"'";

        //System.out.println("queryToGetFields : " + queryToGetFields);

        DataSet fields =  getQueryProcessor().getSqlDataSet(queryToGetFields.toString());
        for (int i = 0; i < fields.getRowCount(); i++) {
            instrumentFields.put(fields.getValue(i, "field").toLowerCase(), fields.getValue(i, "field"));
        }


        return instrumentFields;
    }

    /**
     * This method will return all the instrument fields under the instrument and which are mapped to PL.
     * @param instrumentId  instrument Id
     * @return	map containing the instrument fields.
     */
    public Map<String, String> getInstrumentFieldsMappedtoPL(String instrumentId) {
        Map<String, String> instrumentFields = new HashMap<>();
        String queryToGetFields = "select pli.instrumentfieldid field from instrument i " +
                "join paramlist pl on i.instrumenttype = pl.s_instrumenttype " +
                "join paramlistitem pli on pl.paramlistid = pli.paramlistid " +
                "where i.instrumentid = '" + instrumentId + "'";

        DataSet fields = getQueryProcessor().getSqlDataSet(queryToGetFields);
        for (int i = 0; i < fields.getRowCount(); i++) {
            instrumentFields.put(fields.getValue(i, "field").toLowerCase(), fields.getValue(i, "field"));
        }

        return instrumentFields;
    }

    /**
     *  This method will return all the instrument fields  which are mapped to Sample.
     * @param sampleId  sample Id
     * @return	map containing the instrument fields.
     */
    public Map<String, String> getInstrumentFieldsForSample(String sampleId) {
        Map<String, String> instrumentFields = new HashMap<>();
        String queryToGetFields = "select instrumentfieldid field from sdidataitem where sdcid ='Sample' and keyid1 = '" + sampleId +
                "' and instrumentfieldid is not null";

        DataSet fields = getQueryProcessor().getSqlDataSet(queryToGetFields);
        for (int i = 0; i < fields.getRowCount(); i++) {
            instrumentFields.put(fields.getValue(i, "field").toLowerCase(), fields.getValue(i, "field"));
        }

        return instrumentFields;
    }

    public void process_PGCalcScope( PropertyList props ) throws SapphireException {


        logger.debug("In process_PGCalcScope");
        String sdcid = props.getProperty( "sdcid" );
        String keyid1 = props.getProperty( "keyid1" );
        String keyid2 = props.getProperty( "keyid2" );
        String keyid3 = props.getProperty( "keyid3" );
        String paramlistid = props.getProperty( "paramlistid" );
        String paramlistversionid = props.getProperty( "paramlistversionid" );
        String variantid = props.getProperty( "variantid" );
        String dataset = props.getProperty( "dataset" );
        //The following query is figuring out the value of DESCREENAUTOCALCSCOPE flag.
        System.out.println("1 process_PGCalcScope");

        String sqlWIInfo = "SELECT wi.workitemid, case when wi.U_DESCREENAUTOCALCSCOPE IS NOT NULL then 'Y' ELSE 'N'  END CALCSCOPE " +
                "FROM sdidataitem sdi, sdidata sd, sdiworkitem swi, workitem wi, RSETITEMSDS rsids " +
                "WHERE " +
                "rsids.RSETID = ? " +
                "AND sdi.SDCID = rsids.SDCID " +
                "AND sdi.KEYID1 = rsids.KEYID1 " +
                "AND sdi.KEYID2 = rsids.KEYID2 " +
                "AND sdi.KEYID3 = rsids.KEYID3 " +
                "AND sdi.PARAMLISTID = rsids.PARAMLISTID " +
                "AND sdi.PARAMLISTVERSIONID = rsids.PARAMLISTVERSIONID " +
                "AND sdi.VARIANTID = rsids.VARIANTID " +
                "AND sdi.DATASET = rsids.DATASET " +
                //
                "AND sdi.SDCID = sd.SDCID " +
                "AND sdi.KEYID1 = sd.KEYID1 " +
                "AND sdi.KEYID2 = sd.KEYID2 " +
                "AND sdi.KEYID3 = sd.KEYID3 " +
                "AND sdi.PARAMLISTID = sd.PARAMLISTID " +
                "AND sdi.PARAMLISTVERSIONID = sd.PARAMLISTVERSIONID " +
                "AND sdi.VARIANTID = sd.VARIANTID " +
                "AND sdi.DATASET = sd.DATASET " +
                // "AND sdi.REPLICATEID = sd.REPLICATEID " +
                "AND sdi.SDCID = swi.SDCID " +
                "AND sdi.KEYID1 = swi.KEYID1 " +
                "AND sdi.KEYID2 = swi.KEYID2 " +
                "AND sdi.KEYID3 = swi.KEYID3 " +
                "AND sd.SOURCEWORKITEMID = swi.WORKITEMID " +
                "AND sd.SOURCEWORKITEMINSTANCE = swi.WORKITEMINSTANCE " +
                "AND swi.WORKITEMID = wi.WORKITEMID " +
                "AND swi.WORKITEMVERSIONID = wi.WORKITEMVERSIONID";


        DataSet dsWICalculationScope = new DataSet();


//        String rsetId = "";
//        try {
//            rsetId = getDAMProcessor().createRSetDS( sdcid, keyid1, keyid2, keyid3, paramlistid, paramlistversionid,
//                    variantid, dataset, true );
//
////            if ( rsetId != null ) {
////                dsWICalculationScope = getQueryProcessor().getPreparedSqlDataSet( sqlWIInfo, new Object[] { rsetId } );
////                dsWICalculationScope.showData();
////                // logger.debug("dsWICalculationScope = " + dsWICalculationScope.getRowCount());
////
////            }
//        }
//        catch ( Exception exp ) {
//            String msg = "Failed to create RSET for Sample. Cannot proceed. Contact Administrator.";
//
//            throw new SapphireException( "RsetCreation_Error", SapphireException.TYPE_FAILURE, msg );
//        }
//        finally {
//            getDAMProcessor().clearRSet( rsetId );
//        }

        //if ( dsWICalculationScope.getRowCount() > 0 ) {
        HashMap<String, String> hmFiltProp = new HashMap<String, String>();
        DataSet dsFilt = new DataSet();

        hmFiltProp.put( "calcscope", "Y" );
        dsFilt = dsWICalculationScope.getFilteredDataSet( hmFiltProp );
        logger.debug( "SDMS_PGEnterDataItem::  dsFilt.getRowCount()  =" + dsFilt.getRowCount() );


        //If any of the Test Methods have DESCREENAUTOCALCSCOPE value set then we will call EnterDataItem action with  calculatemodifiedtestsonly=Y.
        if ( dsFilt.getRowCount() > 0 ) {


            props.setProperty( "calculatemodifiedtestsonly", "Y" );
        }
        logger.debug("Out process_PGCalcScope calculatemodifiedtestsonly = " + props.getProperty( "calculatemodifiedtestsonly"));
    }

    /**
     *  This method will get the instrumentfieldids for the sample, parameter list ,step number and instrument id.
     * @param sampleID  sample ID
     * @param instrumentID  instrument ID  can be null
     * @param paramListId  param List Id
     * @param paramListVersionId  param List VersionId
     * @param variantId  param List variant Id
     * @param userSeq  user Seq
     * @return	true/false.
     */
    public Map<String, String> getInstrumentFieldsFromParamList(String sampleID, String instrumentID,
                                                                String paramListId, String paramListVersionId, String variantId, String userSeq) {

        Map<String, String> instrumentFields = new HashMap<String, String>();

        StringBuffer sql = new StringBuffer();
        sql.append(" select distinct di.instrumentfieldid field");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        //sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        //sql.append(" and sd.variantid = pl.variantid");
        //sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        //sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        //if(instrumentID != null )
        //    sql.append(" and di.instrumentid = '").append(instrumentID).append("' ");
        sql.append(" and di.instrumentfieldid is not null ");
        sql.append(" and di.paramlistid = '"+paramListId+"' ");
        sql.append(" and di.paramlistversionid = '"+paramListVersionId+"' ");
        sql.append(" and di.variantid = '"+variantId+"' ");
//        sql.append(" and wi.u_stepno  = '"+userSeq+"' ");
        logger.debug("SQL for getting Instrument Field from ParamList for sample "+  sampleID + " and usersequence "+ userSeq + " is :: "+ sql.toString());
        DataSet fields = getQueryProcessor().getSqlDataSet(sql.toString());
        logger.debug("No. of rows: "+ fields.getRowCount());
        String fieldName = null;
        for (int i = 0; i < fields.getRowCount(); i++) {
            fieldName = fields.getValue(i, "field");
            //if(fieldName!= null){
            instrumentFields.put((fieldName.split("\\@")[0]).toLowerCase(), fieldName);
            //}
        }

        return instrumentFields;

    }




    /**
     * This method will return HashMap object containing All Users details
     * @return
     */
    public HashMap<String, String> getAllUsers(){
        HashMap<String, String> allUsers = new HashMap<String, String>();
        String queryStr = "select su.sysuserid, su.logonname from sysuser su order by su.sysuserid";
        DataSet dsUsers = getQueryProcessor().getSqlDataSet(queryStr);

        for(int recIndex = 0; recIndex<dsUsers.getRowCount(); recIndex++ ){
            allUsers.put(dsUsers.getValue(recIndex, "sysuserid").toLowerCase(), dsUsers.getValue(recIndex, "sysuserid"));
            //populate the logon name as well
            if(!StringUtils.isBlank(dsUsers.getValue(recIndex, "logonname"))
                    && !(dsUsers.getValue(recIndex, "logonname")).equalsIgnoreCase(dsUsers.getValue(recIndex, "sysuserid"))){
                allUsers.put(dsUsers.getValue(recIndex, "logonname").toLowerCase(), dsUsers.getValue(recIndex, "sysuserid"));
            }
        }
        return allUsers;
    }

    /**
     * This method will get the report parameters which are marked as hidden & having default value
     *
     * @param reportId          Report Id
     * @param reportVersionId   Report Version Id
     *
     * @return      HashMap object containing parameter name as Key & default value as value
     */
    public HashMap<String, String> getParamsWithDefaultValue(String reportId, String reportVersionId){
        HashMap<String, String> paramsWithValue = new HashMap<String, String>();
        String queryStr = "select rp.paramid, rp.paramvalue from reportparam rp where rp.reportid = '" + reportId + "' and rp.reportversionid = '" + reportVersionId + "' and rp.paramtype = 'hidden' and rp.paramvalue is not null";
        DataSet dsParams = getQueryProcessor().getSqlDataSet(queryStr);

        for(int recIndex = 0; recIndex<dsParams.getRowCount(); recIndex++ ){
            paramsWithValue.put(dsParams.getValue(recIndex, "paramid"), dsParams.getValue(recIndex, "paramvalue"));
        }
        return paramsWithValue;
    }

    /**
     * This method will get file details defined at Instrument Model page
     *
     * @param instrumentId      Instrument Id for which File details to be fetched
     *
     * @return      HashMap object containing file details
     */
    public HashMap<String, String> getFileDetailsFromModel(String instrumentId) {
        HashMap<String, String> propertyList = new HashMap<String, String>();

        StringBuffer sql = new StringBuffer();
        sql.append("select im.u_sdmsfiletype filetype, im.u_sdmsfilename filename, im.u_sdmsfileextension fileextension ");
        sql.append("from instrument i join instrumentmodel im ");
        sql.append("on i.instrumentmodelid = im.instrumentmodelid and i.instrumenttype = im.instrumenttypeid ");
        sql.append("where i.instrumentid = '" + instrumentId + "'");

        //System.out.println("getFileDetailsFromModel: " + sql.toString());

        DataSet dsFileDetails = getQueryProcessor().getSqlDataSet(sql.toString());

        if (dsFileDetails.getRowCount() > 0) {
            propertyList.put("filetype", dsFileDetails.getValue(0, "filetype", "Static"));
            propertyList.put("filename", dsFileDetails.getValue(0, "filename", "Sequence"));
            propertyList.put("fileextension", dsFileDetails.getValue(0, "fileextension", "csv"));
        }

        return propertyList;
    }




    /**
     *  This method will get the instrumentfieldids,paramid,paramtype,paramlistid,paramlistversionid,variantid for all the samples,  instrument id and instrument fields.
     * @param sampleIdsForInclause  sample IDs
     * @param instrumentId  instrument ID
     * @param fieldsForInClause  instrument fields  present in all the samples
     * @return	HashMap.  <paramlistid_paramlistversionid_variantid_instrumentfieldid, paramid_paramtype >
     */
    public HashMap<String, String> getParamDetailsForMultipleSamples(String sampleIdsForInclause, String instrumentId, String fieldsForInClause){
        HashMap<String, String> fieldParamIdAndParamType = new HashMap<String, String>();
        StringBuffer queryToGetParamDetails = new StringBuffer();
        queryToGetParamDetails.append("select distinct di.instrumentfieldid, di.paramid, di.paramtype, pl.paramlistid, pl.paramlistversionid, pl.variantid ");
        queryToGetParamDetails.append("from  sdidataitem di  " );
        queryToGetParamDetails.append("join sdidata sd on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 ");
        queryToGetParamDetails.append("and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3 and sd.paramlistid = di.paramlistid ");
        queryToGetParamDetails.append( "and sd.paramlistversionid = di.paramlistversionid and sd.variantid = di.variantid ");
        queryToGetParamDetails.append("and sd.dataset = di.dataset " );
        queryToGetParamDetails.append("join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid " );
        queryToGetParamDetails.append("and sd.variantid = pl.variantid " );
        if(instrumentId != null && !instrumentId.isEmpty()) {
            queryToGetParamDetails.append( "join instrument i on i.instrumenttype = pl.s_instrumenttype  and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null) " );
        }
        queryToGetParamDetails.append("where di.sdcid = 'Sample' " );
        queryToGetParamDetails.append( "and di.keyid1 in (" + sampleIdsForInclause + ") " );
        if(instrumentId != null && !instrumentId.isEmpty()) {
        	queryToGetParamDetails.append("and i.instrumentid = '"+ instrumentId + "' " );
        }
        queryToGetParamDetails.append( "and di.instrumentfieldid in (" + fieldsForInClause + ") " );
        logger.debug("queryToGetParamDetails: \n" + queryToGetParamDetails);
        DataSet dsParamDetails = this.getQueryProcessor().getSqlDataSet(queryToGetParamDetails.toString());

        // Edited for PG07735-8235 for issue related to unique paramid and common instrumentfield mapping in PL
//        for(int dsIndex = 0; dsIndex < dsParamDetails.getRowCount(); dsIndex++){
//            fieldParamIdAndParamType.put(dsParamDetails.getValue(dsIndex, "instrumentfieldid"), dsParamDetails.getValue(dsIndex, "paramid")
//                    + "~" + dsParamDetails.getValue(dsIndex, "paramtype"));
//        }
        for (int dsIndex = 0; dsIndex < dsParamDetails.getRowCount(); dsIndex++) {
            String key = dsParamDetails.getValue(dsIndex, "paramlistid") + "_"
                    + dsParamDetails.getValue(dsIndex, "paramlistversionid") + "_"
                    + dsParamDetails.getValue(dsIndex, "variantid") + "_"
                    + dsParamDetails.getValue(dsIndex, "instrumentfieldid");
            String value = dsParamDetails.getValue(dsIndex, "paramid") + "~" + dsParamDetails.getValue(dsIndex, "paramtype");
            fieldParamIdAndParamType.put(key, value);
        }

        //System.out.println("fieldParamIdAndParamType : \n" + fieldParamIdAndParamType);

        return fieldParamIdAndParamType;
    }


    // NEED TO CHECK THIS, DONT USE THE BELOW>
    public DataSet getParamDetails(String sampleId, String instrumentId, String fieldsForInClause){

        String queryToGetParamDetails =
                "select di.instrumentfieldid, di.paramlistid, di.paramlistversionid, di.variantid, di.paramid, di.paramtype  "+
                        "from  sdidataitem di  join sdidata sd on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 "+
                        "and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3 and sd.paramlistid = di.paramlistid "+
                        "and sd.paramlistversionid = di.paramlistversionid and sd.variantid = di.variantid "+
                        "and sd.dataset = di.dataset join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid " +
                        "and wi.workiteminstance = sd.sourceworkiteminstance and wi.keyid1 = sd.keyid1 " +
                        "and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid " +
                        "join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid " +
                        "and sd.variantid = pl.variantid join instrument i on i.instrumenttype = pl.s_instrumenttype " +
                        "and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null) " +
                        "where di.sdcid = 'Sample' " +
                        "and di.keyid1 = '" + sampleId + "' " +
                        "and i.instrumentid = '"+ instrumentId + "' " +
                        "and di.instrumentfieldid in (" + fieldsForInClause + ") " ;

        logger.debug("queryToGetParamDetails : \n" + queryToGetParamDetails);

        DataSet dsParamDetailsFromDB = this.getQueryProcessor().getSqlDataSet(queryToGetParamDetails);
        DataSet dsParamDetails = new DataSet();
        HashMap<String, String> uniqueFieldMap = new HashMap<>();
        for(int ds=0; ds<dsParamDetailsFromDB.getRowCount(); ds++ ){
            if(uniqueFieldMap.get(dsParamDetailsFromDB.getValue(ds,"instrumentfieldid")+"_"+dsParamDetailsFromDB.getValue(ds,"paramlistid"))==null) {
                dsParamDetails.copyRow(dsParamDetailsFromDB,ds,1);
                uniqueFieldMap.put(dsParamDetailsFromDB.getValue(ds,"instrumentfieldid")+"_"+dsParamDetailsFromDB.getValue(ds,"paramlistid"),dsParamDetailsFromDB.getValue(ds,"instrumentfieldid")+"_"+dsParamDetailsFromDB.getValue(ds,"paramlistid"));
            }
        }
        //System.out.println("dsParamDetails : \n" + dsParamDetails);

        return dsParamDetails;
    }


    /**
     *  This method will get the paramid,paramtype for  the sample, dataset  instrument id and instrument fields.
     * @param sampleID  sample IDs
     * @param instrumentID  instrument ID
     * @param instrFieldID  instrument field ids ,pass as null if not using.
     * @param dataset
     * @return	dataset.  paramid, paramtype
     */

    public  DataSet getParamIdAndType(String sampleID, String instrumentID, String instrFieldID, String wiUserSequence, String dataset) {

        StringBuffer sql = new StringBuffer();
        sql.append(" select distinct di.paramid, di.paramtype");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" and di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        if(instrFieldID != null)
            sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
//        sql.append(" and wi.u_stepno  = '").append(wiUserSequence).append("' ");
        sql.append(" and di.dataset = '"+dataset+"' ");

        logger.debug("SQL for getting paramlist for sample "+  sampleID + " and usersequence "+ wiUserSequence + " is "+ sql.toString());

        DataSet dsParamDetails = getQueryProcessor().getSqlDataSet(sql.toString());

        return dsParamDetails;

    }

    /**
     *  This method will get the paramid,paramtype for  the sample, dataset  and parameter list.
     * @param sampleID  sample IDs
     * @param paramListId  param List Id
     * @param paramListVersionId  param List VersionId
     * @param variantId  param List variant Id
     * @param dataset
     * @return	dataset.  paramid, paramtype
     */
    public DataSet getParamIdAndTypeFromPL(String sampleID, String dataset, String paramListId, String paramListVersionId, String variantId) {

        StringBuilder sql = new StringBuilder();
        sql.append(" select distinct di.paramid, di.paramtype, di.instrumentfieldid");
        sql.append(" from  sdidataitem di ");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and di.paramlistid = '").append(paramListId).append("' ");
        sql.append(" and di.paramlistversionid = '").append(paramListVersionId).append("' ");
        sql.append(" and di.variantid = '").append(variantId).append("' ");
        sql.append(" and di.dataset = '").append(dataset).append("' ");
        logger.debug("getParamIdAndTypeFromPL : \n" + sql);
        DataSet dsParamDetails = getQueryProcessor().getSqlDataSet(sql.toString());
        return dsParamDetails;

    }

    /**
     *  This method will get the paramid,paramtype for  the sample, dataset  and instrument.
     * @param sampleID  sample IDs
     * @param instrumentId  instrument Id
     * @param dataset
     * @return	dataset.  paramid, paramtype
     */
    public DataSet getParamIdAndType(String sampleID, String instrumentId, String dataset) {
        //HashMap<String, String> paramIdDetails = new HashMap<>();
        StringBuffer sql = new StringBuffer();
        sql.append(" select distinct di.instrumentfieldid, di.paramid, di.paramtype");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" and di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentId).append("' ");
        sql.append(" and di.dataset = '"+dataset+"' ");
        logger.debug("getParamIdAndType: \n" + sql);
        DataSet dsParamDetails = getQueryProcessor().getSqlDataSet(sql.toString());
        /*for(int dsIndex = 0; dsIndex < dsParamDetails.getRowCount(); dsIndex++){
            paramIdDetails.put(dsParamDetails.getValue(dsIndex, "instrumentfieldid"),
                    dsParamDetails.getValue(dsIndex, "paramid") + "~" + dsParamDetails.getValue(dsIndex, "paramtype"));
        }*/

        return dsParamDetails;
    }

    /**
     *  This method will get the paramid,paramtype for  the sample, dataset  and parameter list.
     * @param sampleID  sample IDs
     * @param paramListId  param List Id
     * @param paramListVersionId  param List VersionId
     * @param variantId  param List variant Id
     * @param dataset
     * @return	dataset.  paramid, paramtype
     */
    public DataSet getParamIdAndTypeFromSDIDATAITEM( String sampleID, String dataset, String paramListId, String paramListVersionId, String variantId ) {
        String sql = "select distinct di.paramid, di.paramtype, di.instrumentfieldid" +
                " from  sdidataitem di " +
                " where di.sdcid = 'Sample'" +
                " and di.keyid1 = '" + sampleID + "' " +
                " and di.paramlistid = '" + paramListId + "' " +
                " and di.paramlistversionid = '" + paramListVersionId + "' " +
                " and di.variantid = '" + variantId + "' " +
                " and di.dataset = '" + dataset + "' ";
        logger.debug("getParamIdAndTypeFromSDIDATAITEM : \n" + sql);
        return getQueryProcessor().getSqlDataSet( sql );
    }

    /**
     *  This method will check if all the dataset are filled  for  the sample,  instrument id and instrument fields.
     * @param sampleID  sample IDs
     * @param instrumentID  instrument ID
     * @param instrFieldID  instrument field ids ,pass as null if not using.
     * @return	boolean.
     */
    public boolean isSampleDataSetProcessed(String sampleID, String instrumentID, String instrFieldID) {

        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();
        sql.append(" select nvl(min(di.dataset), 0) mindataset");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        sql.append(" and di.instrumentfieldid  = '").append(instrFieldID).append("' ");
        sql.append(" and di.enteredtext is not null");
        logger.debug("SQL for getting Max Dataset number for Sample & User Seq : " + sql.toString());
        DataSet mindata = getQueryProcessor().getSqlDataSet(sql.toString());
        logger.debug("Dataset size is "+ mindata.size());

        dataItemCnt = Integer.parseInt(mindata.getValue(0, "mindataset"));
        return dataItemCnt == 0 ? false : true;
    }

    public String getPropertyTreeValue(String propertyTreeId){
        DataSet dsFilePath = getQueryProcessor().getSqlDataSet("select valuetree from propertytree where propertytreeid = '" + propertyTreeId + "'", true);
        String valueTree = dsFilePath.getValue(0, "valuetree");

        return valueTree;
    }

    /**
     * This method is introduced to get the SDMS tab setting from instrument database table
     *
     * @param instrumentId      Instrument Id for which SDMS tab setting to be returned
     *
     * @return          returned value is a XML structured property tree
     */
    public String getSDMSSettingFromDB(String instrumentId){
        DataSet dsFilePath = getQueryProcessor().getSqlDataSet("select collectorvaluetree valuetree from instrument where instrumentid = '" + instrumentId + "'", true);
        String valueTree = dsFilePath.getValue(0, "valuetree");

        return valueTree;
    }

    /**
     * This method is introduced to get the SDMS tab setting from instrument model database table
     *
     * @param instrumentId      Instrument Id for which SDMS tab setting to be returned
     *
     * @return          returned value is a XML structured property tree
     */
    public String getSDMSSettingFromModel(String instrumentId){
        StringBuffer sql = new StringBuffer();
        sql.append("select im.collectorvaluetree valuetree from instrument i ");
        sql.append(" join instrumentmodel im  on i.instrumentmodelid = im.instrumentmodelid");
        sql.append(" and i.instrumenttype = im.instrumenttypeid ");
        sql.append(" where i.instrumentid = '").append(instrumentId).append("' ");

        DataSet dsFilePath = getQueryProcessor().getSqlDataSet(sql.toString(), true);
        String valueTree = dsFilePath.getValue(0, "valuetree");
        return valueTree;
    }

    /**
     *  This method will get the instrument field mapped to the parameter in the sample ,wiStepNum,instrumentID and variantID
     * @param sampleID  sample ID
     * @param paramID  mandatory instrument field present in pl.
     * @param wiStepNum step number
     * @param variantID parameter list variant
     * @param instrumentID  instrument ID
     * @return	interger  mindataset number .
     */
    public String getInstrFieldIDForParamID(String sampleID, String instrumentID, String paramID, String wiStepNum, String variantID) {

        String sInstrumentfieldid = "";
        StringBuffer sql = new StringBuffer();
        sql.append(" select di.instrumentfieldid instrumentfieldid");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" and di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '").append(sampleID).append("' ");
        sql.append(" and i.instrumentid = '").append(instrumentID).append("' ");
        sql.append(" and di.paramid  = '").append(paramID).append("' ");
        sql.append(" and di.variantid = '").append(variantID).append("' ");
//        sql.append(" and wi.u_stepno  = '").append(wiStepNum).append("' ");
        sql.append(" and di.enteredtext is null");

        DataSet ds = getQueryProcessor().getSqlDataSet(sql.toString());
        if(ds.getRowCount()>0){
            sInstrumentfieldid = ds.getValue(0, "instrumentfieldid");
        }
        return sInstrumentfieldid;
    }

//    /**
//     *  This method will get the max replicate of a specific dataset of a sample using the sampleId, stepNo and instrument
//     * @param sampleId  sample ID
//     * @param stepNo  step number
//     * @param instrumentField  mandatory instrument field present in pl.
//     * @param useJoinSDIDATA  adds join to sdidata if true
//     * @param useJoinSDIWorkItem  adds join to sdiworkitem if true
//     * @return	datasetAndMaxReplicate  hashmap containing the dataset number and max replicate of the provided dataset number .
//     */
//    public HashMap<Integer, Integer> getDataSetAndMaxReplicateId(String sampleId, String stepNo, String instrumentField, boolean useJoinSDIDATA, boolean useJoinSDIWorkItem){
//        HashMap<Integer, Integer> dataSetAndMaxReplicate = new HashMap<Integer, Integer>();
//        String queryStr = "select sdi.dataset, max(sdi.replicateid) maxrep from sdidataitem sdi \n";
//
//        if(useJoinSDIDATA){
//            queryStr += "join sdidata sd on sd.keyid1 = sdi.keyid1 and sd.dataset = sdi.dataset and sd.PARAMLISTID = sdi.PARAMLISTID \n" +
//                    "and sd.paramlistversionid = sdi.paramlistversionid and sd.variantid = sdi.variantid \n";
//        }
//        if(useJoinSDIWorkItem){
//            queryStr += "join sdiworkitem swi on swi.keyid1 = sd.keyid1 and swi.workitemid = sd.sourceworkitemid and swi.workiteminstance = sd.sourceworkiteminstance \n";
//        }
//
//        queryStr += "where sdi.sdcid = 'Sample' ";
//
////        if(useJoinSDIWorkItem){
////            queryStr += "and swi.u_stepno = '" + stepNo + "' ";
////        }
//
//        queryStr += "and sdi.keyid1 = '" + sampleId + "' and sdi.INSTRUMENTFIELDID = '" + instrumentField + "' \n";
//
//        if(useJoinSDIDATA){
//            queryStr += "group by sd.dataset order by sd.dataset";
//        } else {
//            queryStr += "group by sdi.dataset order by sdi.dataset";
//        }
//
//        DataSet dsQuery = getQueryProcessor().getSqlDataSet(queryStr);
//
//        for(int i = 0; i < dsQuery.getRowCount(); i++){
//            dataSetAndMaxReplicate.put(Integer.parseInt(dsQuery.getValue(i, "dataset")), Integer.parseInt(dsQuery.getValue(i, "maxrep")));
//        }
//
//        return dataSetAndMaxReplicate;
//    }

    /**
     *  This method will get the minimum replicate number available for a sample using the sampleId, instrument, instrument field and dataset
     * @param sampleID  sample ID
     * @param instrumentID  instrument ID
     * @param instrFieldID  mandatory instrument field present in pl.
     * @param wiUserSequence  step No
     * @param dataset  dataset
     * @return dataItemCnt  integer containing the minimum replicate number available for a sample.
     */
    public int getAvailableReplicate(String sampleID, String instrumentID, String instrFieldID, String wiUserSequence, String dataset) {

        int dataItemCnt = 0;
        StringBuffer sql = new StringBuffer();

        sql.append(" select nvl(min(di.replicateid), 0) minreplicate");
        sql.append(" from  sdidataitem di  join sdidata sd");
        sql.append(" on sd.sdcid = di.sdcid and sd.keyid1 = di.keyid1 and sd.keyid2 = di.keyid2 and  sd.keyid3 = di.keyid3");
        sql.append(" and sd.paramlistid = di.paramlistid");
        sql.append(" and sd.paramlistversionid = di.paramlistversionid");
        sql.append(" and sd.variantid = di.variantid and sd.dataset = di.dataset");
        sql.append(" join sdiworkitem wi on wi.workitemid = sd.sourceworkitemid and wi.workiteminstance = sd.sourceworkiteminstance");
        sql.append(" and wi.keyid1 = sd.keyid1 and wi.keyid2 = sd.keyid2  and wi.keyid3 = sd.keyid3 and wi.sdcid = sd.sdcid");
        sql.append(" and (wi.workitemid != wi.groupid or wi.groupid is null)");
        sql.append(" join paramlist pl on sd.paramlistid = pl.paramlistid and sd.paramlistversionid = pl.paramlistversionid");
        sql.append(" and sd.variantid = pl.variantid");
        sql.append(" join instrument i on i.instrumenttype = pl.s_instrumenttype");
        sql.append(" and (i.instrumentmodelid = pl.s_instrumentmodel or  pl.s_instrumentmodel is null)");
        sql.append(" where di.sdcid = 'Sample'");
        sql.append(" and di.keyid1 = '"+sampleID+"' ");
        sql.append(" and i.instrumentid = '"+instrumentID+"' ");
        sql.append(" and di.instrumentfieldid  = '"+instrFieldID+"' ");
//        if(!wiUserSequence.isEmpty() && wiUserSequence != null)
//            sql.append(" and wi.u_stepno  = '"+wiUserSequence+"' ");
        sql.append(" and di.dataset = '"+ dataset +"' ");
        sql.append(" and di.enteredtext is null");

        DataSet minreplicate =  getQueryProcessor().getSqlDataSet(sql.toString());
        dataItemCnt = Integer.parseInt(minreplicate.getValue(0, "minreplicate"));

        return dataItemCnt;
    }

    /**
     *  This method will get the reference display value using the reference value ID.
     * @param id  Reference Value Id
     * @return fields.getValue(0, "REFDISPLAYVALUE")  reference display value associated with reference value ID.
     */
    public String getRefIdValue(String id){
        String queryToGetRefValue = "select refdisplayvalue from refvalue where reftypeid='AttachmentClass' and refvalueid='"+id+"' ";
        DataSet fields = getQueryProcessor().getSqlDataSet(queryToGetRefValue);
        return fields.getValue(0, "REFDISPLAYVALUE");
    }

    /**
     *  This method will get the details of an attachment using keyid1, attachmentClass, and sourcefilename
     * @param keyid  keyid1
     * @param attachmentClass  Attachment Class
     * @param sourcefilename  filename of Attachment
     * @return dataset  dataset containing sdcid, keyid1, attachmentclass, attachmentnum, and sourcefilename.
     */
    public DataSet getAttachmentDetailsOfSample(String keyid, String attachmentClass, String sourcefilename){
        String fileSeparator = "/";
        if(sourcefilename.contains("/")){
            fileSeparator = "/";
        } else {
            fileSeparator = "//";
        }
        String actualFileName = sourcefilename.split(fileSeparator)[sourcefilename.split(fileSeparator).length -1];
        String SQL = "select sdcid,keyid1,attachmentclass,attachmentnum, sourcefilename from sdiattachment where sdcid = 'Sample' and nvl(attachmentclass,'') = '"+ attachmentClass +"' and keyid1 ='"+keyid+"' and sourcefilename like '%"+actualFileName+"%' ";
        DataSet dataset = getQueryProcessor().getSqlDataSet(SQL);
        return dataset;

    }

    /**
     *  This method will get the filename and description of an attachment using dataCaptureId and sourcefilename
     * @param dataCaptureId  data capture ID
     * @param sourcefileName  filename of Attachment
     * @return attachmentDtls  PropertyList containing filename and attachment description.
     */
    public PropertyList getAttachmentDetails(String dataCaptureId, String sourcefileName) {
        PropertyList attachmentDtls = new PropertyList();
        StringBuffer sqlStr = new StringBuffer();
        sqlStr.append("select filename, attachmentdesc ");
        sqlStr.append("from sdiattachment where sdcid = 'LV_DataCapture' ");
        sqlStr.append("and keyid1 = '" + dataCaptureId + "' and sourcefilename = '" + sourcefileName + "'");

        DataSet dsAttachmentDetails = getQueryProcessor().getSqlDataSet(sqlStr.toString());
        if(dsAttachmentDetails.getRowCount() > 0){
            attachmentDtls.setProperty("filename", dsAttachmentDetails.getValue(0,"filename"));
            attachmentDtls.setProperty("attachmentdesc", dsAttachmentDetails.getValue(0,"attachmentdesc"));
        } else {
            attachmentDtls.setProperty("filename", "NA");
            attachmentDtls.setProperty("attachmentdesc", "NA");
        }
        return attachmentDtls;
    }

    /**
     * This method will create a property list containing successful data captures of a sample.
     * @param sampleId sample ID
     * @param instrumentId Instrument ID
     * @return props propertylist of successful data captures
     */
    public PropertyList getSucessfulDataCaptures(String sampleId, String instrumentId) {
        PropertyList props = new PropertyList();
        StringBuilder sql = new StringBuilder();

        sql.append( "SELECT COUNT(DISTINCT sdi.datacaptureid) AS sdatacapture FROM SDIATTACHMENTOPERATIONEXEC aoe " );
        sql.append( "JOIN datacapture dc ON aoe.keyid1 = dc.datacaptureid JOIN a_sdidataitem sdi " );
        sql.append( "on dc.datacaptureid = sdi.datacaptureid and " );
        sql.append( "sdi.keyid1 = '" ).append( sampleId ).append( "' " );
        sql.append( "WHERE aoe.executionstatus = 'success' and " );
        sql.append( "dc.instrumentid = '" ).append( instrumentId ).append( "'" );
        DataSet dsParamlist = getQueryProcessor().getSqlDataSet( sql.toString() );
        if ( dsParamlist.getRowCount() > 0 ) {
            props.setProperty( "sucessfuldatacaptures", dsParamlist.getValue( 0, "sdatacapture" ) );
        }
        return props;
    }



    /**
     * This method will create a HashMap containing the parameter id, alias id and instrument field id of the parameter list of a given sample.
     * @param paramlistID parameter list ID
     * @param paramListVersionId parameter list version ID
     * @param variantId variant ID of parameter list
     * @param sampleID sample ID
     * @param isAliasNull boolean to check if alias id is null
     * @return paramIds HashMap containing the parameter id, alias id and instrument field id of the parameter list of given sample
     */
    public Map<String, String> getParameters(String paramlistID, String paramListVersionId, String variantId, String sampleID, boolean isAliasNull) {

        Map<String, String> paramIds = new HashMap<String, String>();

        StringBuffer sql = new StringBuffer();
        sql.append(" select distinct paramid, paramtype, aliasid, instrumentfieldid");
        sql.append(" from  sdidataitem ");
        sql.append(" where paramlistid = '").append(paramlistID).append("' ");
        sql.append(" and variantid = '").append(variantId).append("' ");
        sql.append(" and paramlistversionid = '").append(paramListVersionId).append("' ");
        if(!isAliasNull){
            sql.append(" and aliasid is not null");
        }
        sql.append(" and keyid1 = '").append(sampleID).append("' ");
        sql.append(" and sdcid = 'Sample' ");
        DataSet dsParamids = getQueryProcessor().getSqlDataSet(sql.toString());
        //logMessage("No. of rows: "+ dsParamlist.getRowCount());
        for (int i = 0; i < dsParamids.getRowCount(); i++) {
            paramIds.put(dsParamids.getValue(i, "aliasid") == null ? dsParamids.getValue(i, "paramid") : dsParamids.getValue(i, "aliasid"), dsParamids.getValue(i, "paramid") + "@@" + dsParamids.getValue(i, "paramtype") + "@@" + (dsParamids.getValue(i,"instrumentfieldid") == null ? "" : dsParamids.getValue(i,"instrumentfieldid")));
        }

        //logMessage("ParamID: " + sparamlistid +" ParamlistVersionID: "+ sparamlistversionid +" Variant ID: "+ svariantid);

        return paramIds;
    }
}