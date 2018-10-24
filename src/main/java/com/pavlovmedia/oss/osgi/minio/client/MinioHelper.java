package com.pavlovmedia.oss.osgi.minio.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.osgi.framework.ServiceException;
import org.osgi.service.log.LogService;

import com.pavlovmedia.oss.osgi.utils.api.exception.ExceptionUtils;
import com.pavlovmedia.oss.osgi.utils.api.osgi.OsgiServiceValueHelper;

/**
 * Minio-based implementation of the TFTP file operations
 * @author Corbin Sumner {@literal <csumner@pavlovmedia.com>}
 *
 */
@Component(metatype=true, label="Component dhc.osgi.tftp.server.miniohelper.MinioHelper", policy=ConfigurationPolicy.REQUIRE)
@Service
@Properties({
    @Property(name=MinioHelper.PROP_MINIO_URL, label="Base URL", description="https://myapp.com/minio"),
    @Property(name=MinioHelper.PROP_MINIO_ACCESS_KEY, label="Minio Access Key"),
    @Property(name=MinioHelper.PROP_MINIO_SECRET_KEY, label="Minio Secret Key"),
    @Property(name="jaxskip", boolValue=true, propertyPrivate=true)
})
public class MinioHelper implements IFileZoneIo {
    public static final String PROP_MINIO_URL = "baseDirectoryPath";
    public static final String PROP_MINIO_ACCESS_KEY = "minioAccessKey";
    public static final String PROP_MINIO_SECRET_KEY = "minioSecretKey";
    public static final int BUFFER_SIZE = 9000;


    private String minioUrlValue;
    private String minioAccessKey;
    private String minioSecretKey;
    private URL minioUrl;
    private S3Client s3Client;

    @Reference
    LogService logger;

    private Consumer<Exception> onMinIoError = e -> this.logger.log(LogService.LOG_INFO, "MinIo Exception " + e);

/**
 * Create a S3Client with the necessary keys and get the bucket list for the service
 * @param properties
 */
    @Activate
    public void activate(final Map<String, Object> properties) {
        OsgiServiceValueHelper helper = new OsgiServiceValueHelper(properties);

        this.minioUrlValue = helper.getString(PROP_MINIO_URL).orElseThrow(() -> new ServiceException("No minio url set"));
        this.minioUrlValue = this.minioUrlValue.endsWith("/")
                ? this.minioUrlValue.substring(0, this.minioUrlValue.length() - 1)
                : this.minioUrlValue;

        this.minioAccessKey = helper.getString(PROP_MINIO_ACCESS_KEY)
                .orElseThrow(() -> new ServiceException("No minio access key is set"));
        this.minioSecretKey = helper.getString(PROP_MINIO_SECRET_KEY)
                .orElseThrow(() -> new ServiceException("No minio secret key is set"));

        this.minioUrl = ExceptionUtils
                .exceptionSupplier(() -> new URL(this.minioUrlValue),
                        e -> this.logger.log(LogService.LOG_ERROR, e.getMessage(), e))
                .orElseThrow(() -> new ServiceException("Minio URL is not correct: " + this.minioUrlValue));
        this.s3Client = ExceptionUtils
                .exceptionSupplier(() -> new S3Client(this.minioUrl, this.minioAccessKey, this.minioSecretKey, this.onMinIoError),
                        e -> this.logger.log(LogService.LOG_ERROR, e.getMessage(), e))
                .orElseThrow(() -> new ServiceException("S3 Client could not be created with: URL= " + this.minioUrlValue
                        + " Access Key=" + this.minioAccessKey + " Secret Key=" + this.minioSecretKey));
        Consumer<Exception> onActivationError = e -> {
            throw new ServiceException("MinioHelper activation failed ", e);
        };
        Optional<String> response = this.s3Client.getAllBuckets(onActivationError);
        this.logger.log(LogService.LOG_INFO, "Get all buckets worked MinioHelper is activated " + response);
    }

    /**
     * Get the names of all the files in a filezone
     * @param fileZoneId - id of the filezone
     * @param onError - handles exceptions
     * @return a list of the file names associated with the filezone or an empty array if the list is not found
     */
    @Override
    public List<String> getAllFileNamesInFileZone(final String fileZoneId, final Consumer<Exception> onError) {
        return this.s3Client.getObjectList(fileZoneId, this.onMinIoError);
    }

    /**
     * Uploads a file to the filezone
     * @param fileZoneId - id of the filezone
     * @param fileName - name of the file being uploaded
     * @param fileStream - file stream containing the file data
     * @param onError - handles exceptions
     */
    @Override
    public boolean uploadFileToFileZone(final String fileZoneId, final String fileName, final InputStream fileStream,
            final Consumer<Exception> onError) {
            if (!this.s3Client.headBucket(fileZoneId, onError)) {
                this.s3Client.putBucket(fileZoneId, onError);
            }
            return this.s3Client.putObject(fileZoneId, fileName, fileStream, "application/octet-stream", onError)
                    .orElse(false);
    }

    /**
     * Deletes a file from the filezone
     * @param fileZoneId - id of the filezone
     * @param fileName - name of the file being uploaded
     * @param onError - handles exceptions
     */
    @Override
    public void deleteFileFromFileZone(final String fileZoneId, final String fileName, final Consumer<Exception> onError) {
        this.s3Client.deleteObject(fileZoneId, fileName, onError);
    }

    /**
     * Gets the requested file from a filezone
     * @param fileZoneId - id of the filezone
     * @param fileName - name of the file being uploaded
     * @param onError - handles exceptions
     */
    @Override
    public boolean getFileFromFileZone(final String fileZoneId, final String fileName,
            final OutputStream outputStream, final Consumer<Exception> onError) {
        Optional<InputStream> inputStream = this.s3Client.getObject(fileZoneId, fileName, this.onMinIoError);
        if (inputStream.isPresent()) {
            writeToOutput(inputStream.get(), outputStream, onError);
            return true;
        }
        return false;
    }

    /**
     * handles writing the contents of an input stream into an output stream
     * @param inputStream - file stream being read
     * @param outputStream - file stream being written
     * @param onError - handles exceptions
     */
    private void writeToOutput(final InputStream inputStream, final OutputStream outputStream,
            final Consumer<Exception> onError) {
        byte[] buffer = new byte[BUFFER_SIZE];
        int length;

        try {
            while ((length = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, length);
            }
        } catch (IOException e) {
            onError.accept(e);
        }
    }
}
