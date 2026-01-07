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
package com.labvantage.training.sdms.utl;

import org.apache.commons.io.FilenameUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Deflater;

/**
 * Enter description here
 *
 * @author rofikul
 * @version $Author: Mark.DelaCruz $
 *          $Source: /extra/CVS/profserv/ProcterAndGamble_07735/pg_sdms_refactor/com/labvantage/pso/pg/sdms/util/FileTransferOptions.java,v $
 *          $Revision: 8947 $
 *          $Date: 2025-04-08 15:19:11 +0800 (Tue, 08 Apr 2025) $
 *          $State: Exp $
 *          $Id: FileTransferOptions.java 8947 2025-04-08 07:19:11Z Mark.DelaCruz $
 */
//@todo Update JavaDoc
public class FileTransferOptions {

    private boolean replaceTarget = false;
    private boolean deleteSourceOnSuccessfullTransfer = false;
    private boolean flattenFileStructure = false;
    private boolean validateHashValue = false;
    private boolean retrunHashValue = false;
    private boolean validateFileSize = false;
    private boolean includeSubFolder = true;
    private boolean closeoutputstream = false;
    private boolean closeinputstream = false;
    private boolean encryptdata = false;
    private boolean decryptdata = false;

    private int forceDeleteTargetRetryCount = 1;
    private int generateZipRetryCount = 1;
    private int transferRetryCount = 1;
    private int timeoutSecond = 0;
    private int compressionlevel = Deflater.BEST_SPEED;

    private long fileSizeFrom = 0;
    private long fileSizeTo = 0;
    private long sourceHashValue = 0;
    private long targetHashValue = 0;

    private List<String> fileIncldueList = new ArrayList<String>();
    private List<String> fileExcludeList = new ArrayList<String>();
    private CopyOptions copyoption = CopyOptions.NIO_Channel_transferTO;
    private HashingAlgorithm defaultHashingAlgorithm = HashingAlgorithm.MD5;
    private HashingAlgorithm hashingAlgorithm = HashingAlgorithm.None;
    private int DEFAULT_BUFFER_SIZE = 8192;
    private int BUFFER_SIZE = DEFAULT_BUFFER_SIZE;

    private String encryptDecryptPassword = "$apphire@pa$$w0rd";
    private String encryptDecryptAlgorithm = "";
    //  private String defaultEncryptDecryptAlgorithm = "PBEWithMD5AndTripleDES";
    private String defaultEncryptDecryptAlgorithm = "PBEWITHMD5ANDDES";

    private Map<String, Integer> hm = new HashMap<String, Integer>();

    public enum CopyOptions {
        NIO_Channel_transferTO,
        NIO_Channel_transferFrom,
        ApacheCommonsIO_FileUtils_CopyFile,
        NIO_Files_copy,
        FileInputStream,
        NIO_Files_Move,
        IO_File_renameTO,
        FileUtils_moveFile;
    }

    public enum HashingAlgorithm {
        CRC32, MD5, SHA1, SHA256, SHA384, SHA512, None;
    }

    public void setBufferSize( int buffersize ) {
        this.BUFFER_SIZE = buffersize;
    }

    public int getBufferSize() {
        return this.BUFFER_SIZE;
    }

    public void setHashingAlgorithm( HashingAlgorithm hashingAlgorithm ) {
        this.hashingAlgorithm = hashingAlgorithm;
    }

    public HashingAlgorithm getHashingAlgorithm() {
        return this.hashingAlgorithm;
    }

    public HashingAlgorithm getDefaultHashingAlgorithm() {
        return this.defaultHashingAlgorithm;
    }

    public void setCompressionLevel( int compressionlevel ) {
        this.compressionlevel = compressionlevel;
    }

    public int getCompressionLevel() {
        return compressionlevel;
    }

    public void setCopyOption( CopyOptions option ) {
        this.copyoption = option;
    }

    public CopyOptions getCopyOption() {
        return this.copyoption;
    }

    public String getLatestFileName( String filename ) {
        int version;
        if ( hm.containsKey( filename ) ) {
            version = hm.get( filename ) + 1;
        }
        else {
            version = 0;
        }
        hm.put( filename, version );
        String finalname = filename;
        if ( version > 0 ) {
            finalname = FilenameUtils.removeExtension( filename ) + "_" + version + "." + FilenameUtils.getExtension( filename );
        }
        return finalname;
    }


    public void setReplaceTarget( boolean replaceTarget ) {
        this.replaceTarget = replaceTarget;
    }

    public boolean replaceTarget() {
        return replaceTarget;
    }

    public void setDeleteSourceOnSuccessfullTransfer( boolean deleteSourceOnSuccessfullTransfer ) {
        this.deleteSourceOnSuccessfullTransfer = deleteSourceOnSuccessfullTransfer;
    }

    public boolean deleteSourceOnSuccessfullTransfer() {
        return deleteSourceOnSuccessfullTransfer;
    }

    public void setFlattenFileStructure( boolean flattenFileStructure ) {
        this.flattenFileStructure = flattenFileStructure;
    }

    public boolean isFlattenFileStructure() {
        return flattenFileStructure;
    }

    public void setValidateHashValue( boolean validateHashValue ) {
        this.validateHashValue = validateHashValue;
    }

    public boolean validateHashValue() {
        return validateHashValue;
    }

    public void setReturnHashValue( boolean retrunHashValue ) {
        this.retrunHashValue = retrunHashValue;
    }

    public boolean isReturnHashValue() {
        return retrunHashValue;
    }

    public void setValidateFileSize( boolean validateFileSize ) {
        this.validateFileSize = validateFileSize;
    }

    public boolean validateFileSize() {
        return validateFileSize;
    }

    public void setForceDeleteTargetRetryCount( int forceDeleteTargetRetryCount ) {
        this.forceDeleteTargetRetryCount = forceDeleteTargetRetryCount;
    }

    public int getForceDeleteTargetRetryCount() {
        return forceDeleteTargetRetryCount;
    }

    public void setGenerateZipRetryCount( int generateZipRetryCount ) {
        this.generateZipRetryCount = generateZipRetryCount;
    }

    public int getGenerateZipRetryCount() {
        return generateZipRetryCount;
    }

    public void setTransferRetryCount( int transferRetryCount ) {
        this.transferRetryCount = transferRetryCount;
    }

    public int getTransferRetryCount() {
        return transferRetryCount;
    }

    public void setTimeoutSecond( int timeoutSecond ) {
        this.timeoutSecond = timeoutSecond;
    }

    public int getTimeoutSecond() {
        return timeoutSecond;
    }

    public void setSourceFileSize( long fileSizeFrom ) {
        this.fileSizeFrom = fileSizeFrom;
    }

    public long getSourceFileSize() {
        return fileSizeFrom;
    }

    public void setTransferredFileSize( long fileSizeTo ) {
        this.fileSizeTo = fileSizeTo;
    }

    public long getTransferredFileSize() {
        return fileSizeTo;
    }

    public void setSourceHashValue( long sourceHashValue ) {
        this.sourceHashValue = sourceHashValue;
    }

    public long getSourceHashValue() {
        return sourceHashValue;
    }

    public void setTargetHashValue( long targetHashValue ) {
        this.targetHashValue = targetHashValue;
    }

    public long getTargetHashValue() {
        return targetHashValue;
    }

    public void addFileInclude( String fileInclude ) {
        this.fileIncldueList.add( fileInclude );
    }

    public List getFileIncludeList() {
        return fileIncldueList;
    }

    public void addFileExclude( String fileExclude ) {
        this.fileExcludeList.add( fileExclude );
    }

    public List getFileExcludeList() {
        return fileExcludeList;
    }

    public void setIncludeSubFolder( boolean includeSubFolder ) {
        this.includeSubFolder = includeSubFolder;
    }

    public boolean includeSubFolder() {
        return includeSubFolder;
    }

    public void setCloseOutputStream( boolean closeoutputstream ) {
        this.closeoutputstream = closeoutputstream;
    }

    public boolean closeOutputStream() {
        return closeoutputstream;
    }

    public void setCloseInputStream( boolean closeinputstream ) {
        this.closeinputstream = closeinputstream;
    }

    public boolean closeInputStream() {
        return closeinputstream;
    }

    public void setEncryptData( boolean encryptdata ) {
        this.encryptdata = encryptdata;
    }

    public boolean encryptData() {
        return encryptdata;
    }

    public void setDecryptData( boolean decryptdata ) {
        this.decryptdata = decryptdata;
    }

    public boolean decryptData() {
        return decryptdata;
    }

    public void setEncryptDecryptPassword( String password ) {
        this.encryptDecryptPassword = password;
    }

    public String getEncryptDecryptPassword() {
        return encryptDecryptPassword;
    }

    public void setEncryptDecryptAlgorithm( String algorithm ) {
        this.encryptDecryptAlgorithm = algorithm;
    }

    public String getEncryptDecryptAlgorithm() {
        return encryptDecryptAlgorithm;
    }

    public String getDefaultEncryptDecryptAlgorithm() {
        return defaultEncryptDecryptAlgorithm;
    }
}
