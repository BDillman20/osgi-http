package com.pavlovmedia.oss.osgi.minio.client;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Consumer;

/**
 * Interface for file zone file i/o
 * @author Corbin Sumner {@literal <csumner@pavlovmedia.com>}
 *
 */
public interface IFileZoneIo {
    /**
     * Gets a list of file names associated with a file zone
     * @param fileZoneId - id of the requested file zone
     * @return list of file names corresponding to the 
     */
    List<String> getAllFileNamesInFileZone(String fileZoneId, Consumer<Exception> onError);
    
    /**
     * Uploads a file by the given file name to the file zone designated by the file zone id
     * @param fileZoneId - id of the file zone that the file will be uploaded to
     * @param fileName - name of the file that is being uploaded
     */
    boolean uploadFileToFileZone(String fileZoneId, String fileName, InputStream fileStream, Consumer<Exception> onError);
    
    /**
     * Deletes a file by the given file name from the file zone designated by the file zone id
     * @param fileZoneId - id of the file zone that the file will be uploaded to
     * @param fileName - name of the file that is being uploaded
     */
    void deleteFileFromFileZone(String fileZoneId, String fileName, Consumer<Exception> onError);
    
    /**
     * Gets a requested file from the designated file zone
     * @param fileZoneId - id of the file zone that the file will be uploaded to
     * @param fileName - name of the file that is being uploaded
     * @return the requested file
     */
    boolean getFileFromFileZone(String fileZoneId, String fileName, OutputStream outputStream, Consumer<Exception> onError);
}
