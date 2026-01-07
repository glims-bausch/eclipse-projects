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

import com.labvantage.sapphire.Trace;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import sapphire.SapphireException;
import sapphire.util.Logger;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class FileTransfer {

    public static long getHashValue(File source,FileTransferOptions options) throws Exception{
        if ( source == null ) {
            throw new IOException( "Source is not provided" );
        }
        if ( source.exists() && source.canRead() ) {
            boolean isSourceFile = source.isFile();

            if ( isSourceFile){
                if ( options.validateHashValue() && options.getSourceHashValue() == 0 ) {
                   return generateHashValue( source.toString(), getHashAlgorithm( options ) );
                }

            }
        }
        return 0;
    }

    public static void safeFileTransfer( File source, File target, FileTransferOptions options ) throws Exception {
        if ( source == null ) {
            throw new IOException( "Source is not provided" );
        }
        if ( target == null ) {
            throw new IOException( "Destination is not provided" );
        }
        if ( options == null ) {
            options = new FileTransferOptions();
        }

        if ( source.exists() && source.canRead() ) {

            boolean isSourceFile = source.isFile();
            boolean isSourceDirectory = source.isDirectory();
            boolean isSourceZip = "zip".equalsIgnoreCase( FilenameUtils.getExtension( source.getName() ) );
            boolean isTargerFile = target.isFile() || FilenameUtils.getExtension( target.getName() ).length() > 0;
            boolean isTargetDirectory = target.isDirectory()
                    || FilenameUtils.getExtension( target.getName() ).length() == 0;
            boolean isTargetZip = "zip".equalsIgnoreCase( FilenameUtils.getExtension( target.getName() ) );
            boolean generateZip = isTargetZip && !isSourceZip;
            boolean isAppendToZip = false;

            if ( target.exists() && isTargerFile ) {
                if ( options.replaceTarget() ) {
                    deleteTarget( target, options );
                }
                else if ( !isTargetZip ) {
                    throw new IOException( "Destination already exists." );
                }
            }

            if ( isSourceFile && isTargerFile ) {

                if ( generateZip ) {
                    if ( target.exists() ) {
                        isAppendToZip = true;
                        appendToZipFile( source, target );
                    }
                    else {
                        generateZip( source.toString(), target.toString(), options );
                    }
                }
                else {
                    if ( options.validateHashValue() && options.getSourceHashValue() == 0 ) {
                        options.setSourceHashValue( generateHashValue( source.toString(), getHashAlgorithm( options ) ) );
                    }
                    if ( options.validateFileSize() ) {
                        options.setSourceFileSize( FileUtils.sizeOf( source ) );
                    }

                    doCopy( source, target, options.getCopyOption(), options );

                    if ( options.validateFileSize() ) {
                        long tofilesize = FileUtils.sizeOf( target );
                        options.setTransferredFileSize( tofilesize );
                        if ( !( options.getSourceFileSize() == tofilesize ) ) {
                            throw new IOException( "File corrupted, To and From File Sizes are different." );
                        }
                    }

                    if ( options.validateHashValue() ) {
                        long tohashValue = generateHashValue( target.toString(), getHashAlgorithm( options ) );
                        options.setTargetHashValue( tohashValue );
                        if ( !( options.getSourceHashValue() == tohashValue ) ) {
                            throw new IOException( "File corrupted, To and From HashValues are different." );
                        }
                    }
                }
            }
            else if ( isSourceDirectory && isTargetDirectory ) {
                options.setSourceFileSize( FileUtils.sizeOf( source ) );

                //copyDirectory( source, target, options );

                options.setTransferredFileSize( FileUtils.sizeOf( target ) );
                if ( options.validateFileSize() && !( options.getSourceFileSize() == options.getTransferredFileSize() ) ) {
                    throw new IOException( "Folder corrupted, To and From Folder Sizes are different." );
                }
            }
            else if ( isSourceDirectory && isTargetZip ) {
                generateZip( source.toString(), target.toString(), options );
            }
            else if ( isSourceZip && isTargetDirectory ) {
                extractZipFile( source, target );
            }
            else if ( isSourceFile && isTargetDirectory ) {
                Path targetPath = target.toPath().resolve( source.getName() );
                safeFileTransfer( source, targetPath.toFile(), options );
            }
            else {
                throw new IOException( "Unable to transfer source." );
            }

            if ( options.deleteSourceOnSuccessfullTransfer() ) {
                if ( isAppendToZip || isTransferredFileSafe( options, source, target ) ) {
                    FileUtils.forceDelete( source );
                }
                else {
                    throw new IOException( "Transferred File corrupted, To and From File Sizes are different." );
                }
            }
        }
        else {
            if ( !source.exists() ) {
                throw new IOException( "Source does not exist." );
            }
            else if ( !source.canRead() ) {
                throw new IOException( "Source is locked" );
            }
        }
    }

   /* public static void safeDataTransfer( InputStream source, File target, FileTransferOptions options ) throws Exception {
        if ( source == null ) {
            throw new IOException( "Source is not provided" );
        }
        if ( target == null ) {
            throw new IOException( "Destination is not provided" );
        }
        if ( options == null ) {
            options = new FileTransferOptions();
        }

        if ( target.exists() ) {
            if ( options.replaceTarget() ) {
                deleteTarget( target, options );
            }
            else {
                throw new IOException( "Destination already exists." );
            }
        }

        if ( options.encryptData() ) {
            encryptDecryptDataAndTransfer( source, target, Cipher.ENCRYPT_MODE, options );
        }
        else if ( options.decryptData() ) {
            encryptDecryptDataAndTransfer( source, target, Cipher.DECRYPT_MODE, options );
        }
        else {
            Files.copy( source, target.toPath() );
        }

        if ( target.exists() && options.getSourceHashValue() > 0 && !( options.encryptData() || options.decryptData() ) ) {
            long targetHashValue = generateHashValue( target.toString(), getHashAlgorithm( options ) );
            if ( targetHashValue != options.getSourceHashValue() ) {
                throw new IOException( "Target file corrupted" );
            }
        }
        if ( options.closeInputStream() ) {
            source.close();
        }
    }

    public static void safeFileTransfer( File source, OutputStream target, FileTransferOptions options )
            throws Exception {
        if ( source == null ) {
            throw new IOException( "Source is not provided" );
        }
        if ( target == null ) {
            throw new IOException( "Destination is not provided" );
        }
        if ( options == null ) {
            options = new FileTransferOptions();
        }
        if ( source.exists() && source.canRead() ) {
            if ( options.encryptData() ) {
                encryptDecryptDataAndTransfer( source, target, Cipher.ENCRYPT_MODE, options );
            }
            else if ( options.decryptData() ) {
                encryptDecryptDataAndTransfer( source, target, Cipher.DECRYPT_MODE, options );
            }
            else {
                Files.copy( source.toPath(), target );
            }

            if ( options.validateHashValue() && options.getSourceHashValue() == 0 ) {
                options.setSourceHashValue( generateHashValue( source.toString(), getHashAlgorithm( options ) ) );
            }

            if ( options.deleteSourceOnSuccessfullTransfer() ) {
                try {
                    FileUtils.forceDelete( source );
                }
                catch ( IOException e ) {
                    throw new IOException( "Unable to delete source:" + e.getMessage() );
                }
            }
        }
        else {
            if ( !source.exists() ) {
                throw new IOException( "Source does not exist." );
            }
            else if ( !source.canRead() ) {
                throw new IOException( "Source is locked" );
            }
        }

        if ( options.closeOutputStream() ) {
            target.close();
        }
    }
*/
    private static boolean isTransferredFileSafe( FileTransferOptions options, File source, File target ) {
        boolean flag = true;
        String sourceFileType = FilenameUtils.getExtension( source.getName() );
        String targetFileType = FilenameUtils.getExtension( target.getName() );
        if ( !options.validateFileSize() && !options.validateHashValue()
                && sourceFileType.equalsIgnoreCase( targetFileType ) ) {
            if ( !( FileUtils.sizeOf( source ) == FileUtils.sizeOf( target ) ) ) {
                flag = false;
            }
        }
        return flag;
    }

    private static void doCopy( File source, File target, FileTransferOptions.CopyOptions option,
                                FileTransferOptions options ) throws Exception {
        if ( options.isReturnHashValue() ) {
            copyFileAndHash( source, target, options );
        }
        else if ( options.encryptData() ) {
            encryptDecryptDataAndTransfer( source, target, Cipher.ENCRYPT_MODE, options );
        }
        else if ( options.decryptData() ) {
            encryptDecryptDataAndTransfer( source, target, Cipher.DECRYPT_MODE, options );
        }
        else {
            if ( option == FileTransferOptions.CopyOptions.NIO_Channel_transferTO ) {
                copyFileUsingNIO_Channel_transferTO( source, target );
            }
            else if ( option == FileTransferOptions.CopyOptions.NIO_Channel_transferFrom ) {
                copyFileUsingNIO_Channel_transferFrom( source, target );
            }
            else if ( option == FileTransferOptions.CopyOptions.ApacheCommonsIO_FileUtils_CopyFile ) {
                copyFileUsingApacheCommonsIO_FileUtils_CopyFile( source, target );
            }
            else if ( option == FileTransferOptions.CopyOptions.NIO_Files_copy ) {
                copyFileUsingNIO_Files_copy( source, target );
            }
            else if ( option == FileTransferOptions.CopyOptions.FileInputStream ) {
                copyFileUsingFileInputStream( source, target );
            }
            else if ( option == FileTransferOptions.CopyOptions.NIO_Files_Move ) {
                moveUsingNIO_Files_Move( source, target );
            }
            else if ( option == FileTransferOptions.CopyOptions.IO_File_renameTO ) {
                moveUsingIO_File_renameTO( source, target );
            }
            else if ( option == FileTransferOptions.CopyOptions.FileUtils_moveFile ) {
                moveUsingFileUtils_moveFile( source, target );
            }
            else {
                copyFileUsingNIO_Channel_transferTO( source, target );
            }
        }
    }

    // https://javapapers.com/java/java-file-encryption-decryption-using-password-based-encryption-pbe/
    // 5 Common Encryption Algorithms and the Unbreakables of the
    // Future-->https://www.cleverism.com/5-common-encryption-algorithms-and-the-unbreakables-of-the-future/
    // java.security.Security.getAlgorithms("Cipher") returns all the available
    public static void encryptDecryptDataAndTransfer( File inFile, File outFile, int mode, FileTransferOptions options )
            throws Exception {
        InputStream inStream = new FileInputStream( inFile );
        FileOutputStream outStrem = new FileOutputStream( outFile );
        try {
            encryptDecryptDataAndTransfer( inStream, outStrem, mode, options );
        }
        finally {
            inStream.close();
            outStrem.close();
        }
    }

  /*  public static void encryptDecryptDataAndTransfer( File inFile, OutputStream outStrem, int mode,
                                                      FileTransferOptions options ) throws Exception {
        InputStream inStream = new FileInputStream( inFile );
        try {
            encryptDecryptDataAndTransfer( inStream, outStrem, mode, options );
        }
        finally {
            inStream.close();
        }
    }

    public static void encryptDecryptDataAndTransfer( InputStream inStream, File outFile, int mode,
                                                      FileTransferOptions options ) throws Exception {
        FileOutputStream outStrem = new FileOutputStream( outFile );
        try {
            encryptDecryptDataAndTransfer( inStream, outStrem, mode, options );
        }
        finally {
            outStrem.close();
        }
    }
*/
    public static void copyFileUsingNIO_Channel_transferTO( File source, File target ) {
        try {
            FileChannel inChannel = new FileInputStream( source ).getChannel();
            FileChannel outChannel = new FileOutputStream( target ).getChannel();
            try {
                int maxCount = ( 64 * 1024 * 1024 );// chunk of 64mb
                long size = inChannel.size();
                long position = 0;
                while ( position < size ) {
                    position += inChannel.transferTo( position, maxCount, outChannel );

                }
            }
            finally {
                if ( inChannel != null ) {
                    inChannel.close();
                }
                outChannel.close();

            }
        }
        catch ( IOException e ) {
            e.printStackTrace();
        }
    }

    public static void copyFileUsingNIO_Channel_transferFrom( File source, File dest ) throws IOException {
        FileChannel sourceChannel = null;
        FileChannel destChannel = null;
        try {
            sourceChannel = new FileInputStream( source ).getChannel();
            destChannel = new FileOutputStream( dest ).getChannel();
            destChannel.transferFrom( sourceChannel, 0, sourceChannel.size() );
        }
        catch ( IOException ioe ) {
            Trace.logError( "Failed to copyFile Using NIO_Channel_transferFrom", ioe.getMessage() );
        }
        finally {
            if ( sourceChannel != null ) {
                sourceChannel.close();
            }
            if ( destChannel != null ) {
                destChannel.close();
            }
        }
    }

    public static void copyFileUsingApacheCommonsIO_FileUtils_CopyFile( File source, File dest ) throws IOException {
        try {
            FileUtils.copyFile( source, dest );
        }
        catch ( IOException ioe ) {
            throw new IOException( "Failed to copy:" + ioe.getMessage() );
        }
    }

    public static void copyFileUsingNIO_Files_copy( File source, File dest ) throws IOException {
        try {
            Files.copy( Paths.get( source.toString() ), Paths.get( dest.toString() ) );
        }
        catch ( IOException ioe ) {
            throw new IOException( "Failed to copy:" + ioe.getMessage() );
        }
    }

    public static void copyFileUsingFileInputStream( File source, File dest ) throws IOException {
        InputStream is = null;
        OutputStream os = null;
        try {
            is = new FileInputStream( source );
            os = new FileOutputStream( dest );
            byte[] buffer = new byte[8192];
            int length;
            while ( ( length = is.read( buffer ) ) > 0 ) {
                os.write( buffer, 0, length );
            }
        }
        catch ( IOException ioe ) {
            throw new IOException( "Failed to copy:" + ioe.getMessage() );
        }
        finally {
            if ( is != null ) {
                is.close();
            }
            if ( os != null ) {
                os.close();
            }
        }
    }

    public static void copyFileAndHash( File source, File dest, FileTransferOptions options ) throws Exception {
        InputStream is = null;
        OutputStream os = null;
        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest
                    .getInstance( FileTransfer.getHashAlgorithmName( FileTransferOptions.HashingAlgorithm.MD5           ) );
            is = new FileInputStream( source );
            os = new FileOutputStream( dest );
            byte[] buffer = new byte[options.getBufferSize()];
            int length;
            while ( ( length = is.read( buffer ) ) > 0 ) {
                os.write( buffer, 0, length );
                messageDigest.update( buffer, 0, length );
            }
        }
        catch ( Exception ioe ) {
            throw new Exception( "Failed to copy:" + ioe.getMessage() );
        }
        finally {
            if ( is != null ) {
                is.close();
            }
            if ( os != null ) {
                os.close();
            }
        }

        if ( messageDigest != null ) {
            byte[] checksum = messageDigest.digest();
            BigInteger bigInt = new BigInteger( 1, checksum );
            options.setSourceHashValue( bigInt.longValue() );
        }
    }

    public static void moveUsingNIO_Files_Move( File source, File target ) throws IOException {
        Files.move( source.toPath(), target.toPath() );
    }

    public static void moveUsingIO_File_renameTO( File source, File target ) throws IOException {
        if ( !source.renameTo( target ) ) {
            throw new IOException( "Failed to Move" );
        }
    }

    public static void moveUsingFileUtils_moveFile( File source, File target ) throws IOException {
        FileUtils.moveFile( source, target );
    }

  /*  public static void zipFileAndMove( String source, String target ) throws IOException {
        File zipFle = new File( "temp.zip" );
        File sourceFile = new File( source );
        try (ZipOutputStream zos = new ZipOutputStream( new FileOutputStream( zipFle ) )) {
            zos.putNextEntry( new ZipEntry( sourceFile.getName() ) );
            Files.copy( sourceFile.toPath(), zos );
        }
        Files.move( zipFle.toPath(), new File( target ).toPath() );
    }
*/
    private static void deleteTarget( File target, FileTransferOptions options ) throws Exception {
        int deleteAttemp = options.getForceDeleteTargetRetryCount();
        boolean deleteSuccessfull = false;
        while ( !deleteSuccessfull && deleteAttemp > 0 ) {
            try {
                deleteSuccessfull = target.delete();
                deleteAttemp--;
                if ( !deleteSuccessfull && deleteAttemp > 0 ) {
                    Thread.sleep( 100 );
                }
            }
            catch ( Exception e ) {
                throw new IOException( "Unable to delete source:" + e.getMessage() );
            }

        }
        if ( !deleteSuccessfull ) {
            throw new IOException( "Destination already exists and cannot be deleted in "
                    + options.getForceDeleteTargetRetryCount() + "  attemp(s)." );
        }
    }

    public static void appendToZipFile( File source, File target ) throws IOException {
        Map<String, String> env = new HashMap<>();
        env.put( "create", "false" );
        URI uri = URI.create( "jar:" + Paths.get( target.toString() ).toUri() );
        try (java.nio.file.FileSystem zipfs = FileSystems.newFileSystem( uri, env )) {
            Path externalTxtFile = Paths.get( source.toString() );
            Path pathInZipfile = zipfs.getPath( source.getName() );
            Files.copy( externalTxtFile, pathInZipfile );
        }
    }

    public static long generateCheckSumUsingCRC32( InputStream inputStream ) throws Exception {
        CRC32 crc = new CRC32();
        byte[] buffer = new byte[8192];
        int read = 0;
        while ( ( read = inputStream.read( buffer ) ) > 0 ) {
            crc.update( buffer, 0, read );
        }
        Logger.logDebug( "CRC32: " + crc.getValue() );
        return crc.getValue();
    }

   /* public static long generateCheckSumUsingCRC32( String sourceFile ) throws Exception {
        InputStream inputStream = new FileInputStream( new File( sourceFile ) );
        long value = generateCheckSumUsingCRC32( inputStream );
        inputStream.close();
        return value;
    }

    public static long checksumMappedFile( String filepath ) throws IOException {
        FileInputStream inputStream = new FileInputStream( filepath );
        FileChannel fileChannel = inputStream.getChannel();
        int len = (int)fileChannel.size();// this is not working for long size file found not working for 11GB data...
        // Returning negetive size in this line
        MappedByteBuffer buffer = fileChannel.map( FileChannel.MapMode.READ_ONLY, 0, len );
        CRC32 crc = new CRC32();
        for ( int cnt = 0; cnt < len; cnt++ ) {
            int i = buffer.get( cnt );
            crc.update( i );
        }
        return crc.getValue();
    }

    public static long checksumBufferedInputStream( String filepath ) throws IOException {
        InputStream inputStream = new BufferedInputStream( new FileInputStream( filepath ) );
        CRC32 crc = new CRC32();
        int cnt;
        while ( ( cnt = inputStream.read() ) != -1 ) {
            crc.update( cnt );
        }
        return crc.getValue();
    }
*/
    public static String getHashAlgorithmName( FileTransferOptions.HashingAlgorithm option ) {
        String name = "MD5";
        if ( option == FileTransferOptions.HashingAlgorithm.SHA1 ) {
            name = "SHA-1";
        }
        else if ( option == FileTransferOptions.HashingAlgorithm.SHA256 ) {
            name = "SHA-256";
        }
        else if ( option == FileTransferOptions.HashingAlgorithm.SHA384 ) {
            name = "SHA-384";
        }
        else if ( option == FileTransferOptions.HashingAlgorithm.SHA512 ) {
            name = "SHA-512";
        }
        else if ( option == FileTransferOptions.HashingAlgorithm.CRC32 ) {
            name = "CRC32";
        }
        else if ( option == FileTransferOptions.HashingAlgorithm.MD5 ) {
            name = "MD5";
        }
        return name;
    }

    public static long generateCheckSum( InputStream inputStream ) throws Exception {
        return generateCheckSum( inputStream, FileTransferOptions.HashingAlgorithm.MD5 );
    }

    public static long generateCheckSum( InputStream inputStream, FileTransferOptions.HashingAlgorithm option )
            throws Exception {
        MessageDigest digest = MessageDigest.getInstance( getHashAlgorithmName( option ) );

        byte[] buffer = new byte[8192];
        int read = 0;
        try {
            while ( ( read = inputStream.read( buffer ) ) > 0 ) {
                digest.update( buffer, 0, read );
            }
            byte[] checksum = digest.digest();
            BigInteger bigInt = new BigInteger( 1, checksum );
            Logger.logDebug( getHashAlgorithmName( option ) + ": " + bigInt.longValue() );
            return bigInt.longValue();
        }
        catch ( IOException e ) {
            throw new RuntimeException( "Unable to process file for " + getHashAlgorithmName( option ), e );
        }
    }

    public static void extractZipStream( InputStream inputStream, File destination ) throws IOException {
        ZipInputStream in = null;
        OutputStream out = null;
        if ( !destination.isAbsolute() ) {
            destination = destination.toPath().toAbsolutePath().toFile();
        }
        in = new ZipInputStream( inputStream );
        try {
            ZipEntry entry = null;
            while ( ( entry = in.getNextEntry() ) != null ) {
                String outFilename = entry.getName();
                if ( outFilename.endsWith( "\\" ) || outFilename.endsWith( "/" ) ) {    // Could be replaced with .isDirectory() in the future
                    File folder = new File( destination, outFilename );
                    if ( !folder.mkdirs() ) {
                        throw new IOException( "Failed to create directory." );
                    }
                }
                else {
                    out = new FileOutputStream( new File( destination, outFilename ) );
                    try {
                        // Transfer bytes from the ZIP file to the output file
                        byte[] buf = new byte[1024];
                        int len;
                        while ( ( len = in.read( buf ) ) > 0 ) {
                            out.write( buf, 0, len );
                        }
                    }
                    finally {
                        if ( out != null ) {
                            out.close();
                        }
                    }
                }
            }
        }
        finally {
            if ( in != null ) {
                in.close();
            }
        }
    }

    public static void extractZipFile( File file, File destination ) throws IOException {
        if ( !file.isAbsolute() ) {
            file = file.toPath().toAbsolutePath().toFile();
        }
        FileInputStream in = new FileInputStream( file );
        try {
            extractZipStream( in, destination );
        }
        finally {
            in.close();
        }
    }
/*
    // method returns hash checking stream
    public static DigestInputStream getHashedInputSteam( InputStream in, FileTransferOptions.HashingAlgorithm option )
            throws SapphireException {
        try {
            MessageDigest digest = MessageDigest.getInstance( getHashAlgorithmName( option ) );
            return new DigestInputStream( in, digest );
        }
        catch ( Exception e ) {
            throw new SapphireException( e );
        }
    }

    // method returns hash checking stream
    public static HashedAttachmentInputStream getHashedInputSteam( Attachment attachment,
                                                                   FileTransferOptions.HashingAlgorithm option ) throws SapphireException {
        try {
            MessageDigest digest = MessageDigest.getInstance( getHashAlgorithmName( option ) );
            return new HashedAttachmentInputStream( attachment, digest );
        }
        catch ( Exception e ) {
            throw new SapphireException( e );
        }
    }

 /*   // method pipes a normal input stream into a gzipped one
    public static InputStream getCompressedInputStream( InputStream in ) throws SapphireException {
        final PipedInputStream zipped = new PipedInputStream();
        try {
            final PipedOutputStream pipe = new PipedOutputStream( zipped );
            Thread t = new Thread( new Runnable() {
                @Override
                public void run() {
                    FileTransferOptions fto = new FileTransferOptions();
                    fto.setCloseInputStream( false );
                    fto.setCloseOutputStream( false );
                    try (OutputStream zipper = new GZIPOutputStream( pipe )) {
                        safeDataTransfer( in, zipper, fto );
                    }
                    catch ( Exception e ) {
                        e.printStackTrace();
                    }
                }
            } );
            t.start();
            int count = 0;
            while ( zipped.available() == 0 && count < 1000 ) {
                Thread.sleep( 100 );
                count++;
            }
            return zipped;
        }
        catch ( Exception e ) {
            throw new SapphireException( e );
        }
    }

    // method pipes a normal input stream into a gzipped one
    public static InputStream getUncompressedInputStream( InputStream in ) throws SapphireException {

        try {
            GZIPInputStream zippedInput = new GZIPInputStream( in );
            return zippedInput;
        }
        catch ( IOException e ) {
            throw new SapphireException( e );
        }

//        final PipedInputStream unzipped = new PipedInputStream();
//        try {
//            GZIPInputStream zippedInput = new GZIPInputStream( in );
//            PipedOutputStream pipe = new PipedOutputStream( unzipped );
//            Thread t = new Thread( new Runnable() {
//                @Override
//                public void run() {
//                    FileTransferOptions fto = new FileTransferOptions();
//                    fto.setCloseInputStream( false );
//                    fto.setCloseOutputStream( false );
//                    try {
//                        safeDataTransfer( zippedInput, pipe, fto );
//                    }
//                    catch ( Exception e ) {
//                        e.printStackTrace();
//                    }
//                }
//            });
//            t.start();
//            int count = 0;
//            while ( unzipped.available() == 0 && count < 1000 ) {
//                Thread.sleep( 100 );
//                count++;
//            }
//            return unzipped;
//        }
//        catch ( Exception e ){
//            throw new SapphireException( e );
//        }
    }
   */
    private static FileTransferOptions.HashingAlgorithm getHashAlgorithm( FileTransferOptions options ) {
        FileTransferOptions.HashingAlgorithm algo = options.getHashingAlgorithm();
        if ( !( algo == FileTransferOptions.HashingAlgorithm.None ) ) {
            return algo;
        }

        String encryptionAlgorithm = options.getEncryptDecryptAlgorithm();
        if ( encryptionAlgorithm.length() > 0 ) {
            for ( FileTransferOptions.HashingAlgorithm encryptedAlgo : FileTransferOptions.HashingAlgorithm.values() ) {
                if ( encryptionAlgorithm.contains( getHashAlgorithmName( encryptedAlgo ) ) ) {
                    return encryptedAlgo;
                }
            }
        }
        return options.getDefaultHashingAlgorithm();
    }

    private static String getEncryptDecryptAlgorithm( FileTransferOptions options ) {
        String encryptionAlgorithm = options.getEncryptDecryptAlgorithm();
        if ( encryptionAlgorithm.length() > 0 ) {
            return encryptionAlgorithm;
        }
        return options.getDefaultEncryptDecryptAlgorithm();
    }

    public static CipherInputStream getCipherInputStream( InputStream inputStream, boolean decrypt )
            throws SapphireException {
        return getCipherInputStream( inputStream, null, null, decrypt );
    }

    public static CipherInputStream getCipherInputStream( final InputStream inputStream, String password,
                                                          String algorithm, boolean decrypt ) throws SapphireException {
        FileTransferOptions fto = new FileTransferOptions();
        if ( algorithm == null || algorithm.length() == 0 ) {
            algorithm = fto.getDefaultEncryptDecryptAlgorithm();
        }
        if ( password == null || password.length() == 0 ) {
            password = fto.getEncryptDecryptPassword();
        }
        byte[] salt = new byte[8];
        PBEParameterSpec defParams = new PBEParameterSpec( salt, 99 );
        PBEKeySpec pbeKeySpec = new PBEKeySpec( password.toCharArray() );
        try {
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance( algorithm );
            SecretKey secretKey = secretKeyFactory.generateSecret( pbeKeySpec );

//            final KeyGenerator kg = KeyGenerator.getInstance( algorithm );
//            kg.init( new SecureRandom( new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8 } ));
//            final SecretKey key = kg.generateKey();

//            byte[] salt = new byte[8];
            final Cipher cipher = Cipher.getInstance( algorithm );
            if ( decrypt ) {
                cipher.init( Cipher.DECRYPT_MODE, secretKey, defParams );
            }
            else {
                cipher.init( Cipher.ENCRYPT_MODE, secretKey, defParams );
            }

            return new CipherInputStream( inputStream, cipher ) {
                @Override
                public int available() throws IOException {
                    return inputStream.available();
                }

            };
        }
        catch ( Exception e ) {
            throw new SapphireException( e );
        }

    }

    public static void encryptDecryptDataAndTransfer( InputStream inputStream, OutputStream outputStream, int mode,
                                                      FileTransferOptions options ) throws Exception {

        if ( !( mode == Cipher.ENCRYPT_MODE || mode == Cipher.DECRYPT_MODE ) ) {
            throw new Exception( "Mode should be either Cipher.ENCRYPT_MODE or Cipher.DECRYPT_MODE" );
        }

        String password = options.getEncryptDecryptPassword();
        String algorithm = getEncryptDecryptAlgorithm( options );

        byte[] salt = new byte[8];
        if ( mode == Cipher.ENCRYPT_MODE ) {
            byte[] byteArray = IOUtils.toByteArray( inputStream );
            InputStream inputStream1 = new ByteArrayInputStream( byteArray );
            inputStream = new ByteArrayInputStream( byteArray );
            setHashValue( inputStream1, options );
            inputStream1.close();
            Random random = new Random();
            random.nextBytes( salt );
            outputStream.write( salt );
        }
        else {
            inputStream.read( salt );
        }

        PBEKeySpec pbeKeySpec = new PBEKeySpec( password.toCharArray() );
        SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance( algorithm );
        SecretKey secretKey = secretKeyFactory.generateSecret( pbeKeySpec );

        PBEParameterSpec pbeParameterSpec = new PBEParameterSpec( salt, 100 );
        Cipher cipher = Cipher.getInstance( algorithm );
        cipher.init( mode, secretKey, pbeParameterSpec );

        byte[] input = new byte[1024];
        int bytesRead;
        while ( ( bytesRead = inputStream.read( input ) ) != -1 ) {
            byte[] output = cipher.update( input, 0, bytesRead );
            if ( output != null ) {
                outputStream.write( output );
            }
        }

        byte[] output = cipher.doFinal();
        if ( output != null ) {
            outputStream.write( output );
        }
    }

  /*  public static void safeDataTransfer( InputStream source, OutputStream target, FileTransferOptions options )
            throws Exception {
        if ( source == null ) {
            throw new IOException( "Source is not provided" );
        }
        if ( target == null ) {
            throw new IOException( "Destination is not provided" );
        }
        if ( options == null ) {
            options = new FileTransferOptions();
        }

        if ( options.encryptData() ) {
            encryptDecryptDataAndTransfer( source, target, Cipher.ENCRYPT_MODE, options );
        }
        else if ( options.decryptData() ) {
            encryptDecryptDataAndTransfer( source, target, Cipher.DECRYPT_MODE, options );
        }
        else {
            IOUtils.copy( source, target );
        }

        if ( options.closeInputStream() ) {
            source.close();
        }
        if ( options.closeOutputStream() ) {
            target.close();
        }
    }
*/
    public static long generateHashValue( InputStream inputStream, FileTransferOptions.HashingAlgorithm option )
            throws Exception {
        long value = 0;
        if ( option == FileTransferOptions.HashingAlgorithm.MD5 || option == FileTransferOptions.HashingAlgorithm.SHA1
                || option == FileTransferOptions.HashingAlgorithm.SHA256
                || option == FileTransferOptions.HashingAlgorithm.SHA384
                || option == FileTransferOptions.HashingAlgorithm.SHA512 ) {
            value = generateCheckSum( inputStream, option );
        }
        else if ( option == FileTransferOptions.HashingAlgorithm.CRC32 ) {
            value = generateCheckSumUsingCRC32( inputStream );
        }
        else {
            value = generateCheckSum( inputStream, FileTransferOptions.HashingAlgorithm.MD5 );
        }

        return value;
    }

    public static long generateHashValue( String file, FileTransferOptions.HashingAlgorithm option ) throws Exception {
        InputStream inputStream = new FileInputStream( file );
        long value = generateHashValue( inputStream, option );
        inputStream.close();
        return value;
    }

    private static void setHashValue( InputStream inputStream, FileTransferOptions options ) throws Exception {
        if ( options.getSourceHashValue() == 0 ) {
            FileTransferOptions.HashingAlgorithm algorithm = getHashAlgorithm( options );
            options.setSourceHashValue( generateHashValue( inputStream, algorithm ) );
        }
    }

   /* public static void copyDirectory( File source, File target, FileTransferOptions options ) throws IOException {
        FileUtils.copyDirectory( source, target, new FileFilter() {
            public boolean accept( File pathname ) {
                return isAcceptableFile( pathname.getName(), options );
            }
        } );
    }*/

    public static void generateZip( String inputFolder, String targetZipp, FileTransferOptions options )
            throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream( targetZipp );
        ZipOutputStream zipOutputStream = new ZipOutputStream( fileOutputStream );
        File inputFile = new File( inputFolder );
        if ( inputFile.isFile() ) {
            zipFile( inputFile, "", zipOutputStream, options );
        }
        else if ( inputFile.isDirectory() ) {
            zipFolder( zipOutputStream, inputFile, "", options, true );
        }
        zipOutputStream.close();
    }

    private static void zipFolder( ZipOutputStream zipOutputStream, File inputFolder, String parentName,
                                   FileTransferOptions options, boolean ignoreRootFolder ) throws IOException {
        String myname = ignoreRootFolder ? "" : parentName + inputFolder.getName() + "/";
        if ( !options.isFlattenFileStructure() && myname.length() > 0 ) {
            ZipEntry folderZipEntry = new ZipEntry( myname );
            zipOutputStream.putNextEntry( folderZipEntry );
        }
        File[] contents = inputFolder.listFiles();
        if ( contents != null ) {
            for ( File f : contents ) {
                if ( f.isFile() ) {
                    zipFile( f, myname, zipOutputStream, options );
                }
                else if ( f.isDirectory() && options.includeSubFolder() ) {
                    zipFolder( zipOutputStream, f, myname, options, false );
                }
            }
        }
        zipOutputStream.closeEntry();
    }

    private static void zipFile( File inputFile, String parentName, ZipOutputStream zipOutputStream,
                                 FileTransferOptions options ) throws IOException {
        // A ZipEntry represents a file entry in the zip archive
        // We name the ZipEntry after the original file's name
        String inputFileName = inputFile.getName();
        String zipItem = parentName + inputFileName;
        if ( isAcceptableFile( inputFileName, options ) ) {
            if ( options.isFlattenFileStructure() ) {
                zipItem = options.getLatestFileName( inputFileName );
            }
            ZipEntry zipEntry = new ZipEntry( zipItem );
            zipOutputStream.putNextEntry( zipEntry );
            zipOutputStream.setLevel( options.getCompressionLevel() );

            try {
                Files.copy( inputFile.toPath(), zipOutputStream );
            }
            catch ( IOException e ) {
                throw new IOException( "Failed to zip:" + e.getMessage() );
            }
            finally {
                zipOutputStream.closeEntry();
            }
        }
    }

    public static void generateTarGZip( File originFileOrFolder, File destinationFile,
                                        FileTransferOptions zipCopyOptions ) throws FileNotFoundException, IOException {
        try {
            FileOutputStream fos = new FileOutputStream( destinationFile );
            try {
                BufferedOutputStream bos = new BufferedOutputStream( fos );
                try {
                    TarArchiveOutputStream taos = (TarArchiveOutputStream)new ArchiveStreamFactory()
                            .createArchiveOutputStream( ArchiveStreamFactory.TAR, bos );
                    try {
                        taos.setLongFileMode( TarArchiveOutputStream.LONGFILE_GNU );
                        addFileOrFolderToTarGz( taos, originFileOrFolder.getCanonicalPath(), "" );
                    }
                    finally {
                        taos.close();
                    }
                }
                finally {
                    bos.close();
                }
            }
            finally {
                fos.close();
            }
        }
        catch ( Exception e ) {
            // TODO: handle exception
        }
    }

    private static void addFileOrFolderToTarGz( TarArchiveOutputStream taos, String path, String base )
            throws IOException {
        File f = new File( path );
        String entryName = base + f.getName();
        TarArchiveEntry tarEntry = new TarArchiveEntry( f, entryName );
        taos.putArchiveEntry( tarEntry );

        if ( f.isFile() ) {
            FileInputStream fin = new FileInputStream( f );
            IOUtils.copy( fin, taos );
            fin.close();
            taos.closeArchiveEntry();
        }
        else {
            taos.closeArchiveEntry();
            File[] children = f.listFiles();
            if ( children != null ) {
                for ( File child : children ) {
                    addFileOrFolderToTarGz( taos, child.getAbsolutePath(), entryName + "/" );
                }
            }
        }
    }

    public static boolean isAcceptableFile( String filename, FileTransferOptions options ) {
        boolean valid = true;
        if ( filename != null && filename.contains( "." ) ) {
            List includeTypeList = options.getFileIncludeList();
            if ( includeTypeList.size() > 0 ) {
                valid = matchFileName( includeTypeList, filename );
            }
            List excludeTypeList = options.getFileExcludeList();
            if ( excludeTypeList.size() > 0 && matchFileName( excludeTypeList, filename ) ) {
                valid = false;
            }
        }
        return valid;
    }

    private static boolean matchFileName( List list, String name ) {
        boolean valid = false;
        for ( Object item : list ) {
            String pattern = (String)item;
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher( "glob:" + pattern );
            if ( matcher.matches( Paths.get( name ) ) ) {
                valid = true;
                break;
            }
        }
        return valid;
    }

    public static boolean isLocked( List<Path> paths ) {
        boolean locked = false;
        try {
            for ( int i = 0; i < paths.size(); i++ ) {
                locked = isLocked( paths.get( i ).toFile() );
                if ( locked ) {
                    break; // if one file/folder is locked return true;
                }
            }
        }
        catch ( Exception e ) {

        }
        return locked;
    }

    public static boolean isLocked( File lockFile ) throws Exception {
        boolean locked = false;
        if ( lockFile.isDirectory() ) {
            File dirFiles[] = lockFile.listFiles();
            for ( int d = 0; d < dirFiles.length; d++ ) {
                locked = isLocked( dirFiles[d] );
                if ( locked ) {
                    break; // if one file/folder is locked return true;
                }
            }
        }
        else {
            FileChannel fileChannel = null; // The channel to the file
            FileLock lock = null; // The lock object we hold
            RandomAccessFile file = null;
            try {
                file = new RandomAccessFile( lockFile, "rw" );
                fileChannel = file.getChannel();
                // Try to get an exclusive lock on the file.
                // This method will return a lock or null, but will not block.
                // See also FileChannel.lock() for a blocking variant.
                lock = fileChannel.tryLock();
                if ( lock == null || !lock.isValid() ) {
                    locked = true;
                }
            }
            catch ( Exception e ) {
                locked = true;
            }
            finally {
                // Always release the lock and close the file
                // Closing the RandomAccessFile also closes its FileChannel.
                if ( lock != null && lock.isValid() ) {
                    lock.release();
                }
                if ( file != null ) {
                    file.close();
                }
            }
        }
        return locked;
    }
}
