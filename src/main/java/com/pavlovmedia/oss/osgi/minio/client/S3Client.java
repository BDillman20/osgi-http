package com.pavlovmedia.oss.osgi.minio.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

import org.simpleframework.xml.core.Persister;

import com.pavlovmedia.oss.osgi.http.HttpResponse;
import com.pavlovmedia.oss.osgi.http.HttpVerbs;
import com.pavlovmedia.oss.osgi.http.PavlovHttpClient;
import com.pavlovmedia.oss.osgi.http.PavlovHttpClientImpl;
import com.pavlovmedia.oss.osgi.utils.api.exception.ExceptionUtils;

import dhc.osgi.tftp.server.miniohelper.serializedxmlhelpers.ListBucketResult;


/**
 * Lightweight S3-compatible HTTP Client
 *
 * @author Bryce Dillman {@literal <bdillman@pavlovmedia.com>}
 *
 */
public class S3Client {
    public static final String ALGO_HMAC_SHA1 = "HmacSHA1";

    public static final String CHARSET_UTF8 = "UTF-8";

    public static final String MEDIATYPE_NONE = "";
    public static final String MEDIATYPE_TEXT = "text/plain";
    public static final String MEDIATYPE_XML = "application/xml";
    public static final String MEDIATYPE_OCTETSTREAM = "application/octet-stream";

    public static final String METHOD_HEAD = "HEAD";
    public static final String METHOD_GET = "GET";
    public static final String METHOD_PUT = "PUT";
    public static final String METHOD_DELETE = "DELETE";

    public static final String HEADER_AUTHORIZATION = "Authorization";
    public static final String HEADER_DATE = "Date";

    public static final String HEADERVALUE_ACCEPT_ANY = "*/*";

    public static final int RESPONSE_OK = 200;
    public static final int RESPONSE_NOCONTENT = 204;

    public static final DateFormat DF_S3_HTTP_HEADER = new SimpleDateFormat("EEE', 'dd' 'MMM' 'yyyy' 'HH:mm:ss' 'Z", Locale.US);

    public static final int UPLOAD_BUFFER_SIZE = 4096;

    public static final String FORMAT_RESOURCE_PATH = "/%s/%s"; // (bucket, objectName)
    public static final String FORMAT_BUCKET_PATH = "/%s"; // (bucket)

    // Note(ykako): FORMAT_RESOURCE_SIGNATURE does not handle MD5 checksums or Amazon-specific headers
    public static final String FORMAT_RESOURCE_SIGNATURE = "%s\n\n%s\n%s\n%s"; // (method, contentType, formattedDate, resourcePath)
    public static final String FORMAT_AUTHORIZATION_STRING = "AWS %s:%s"; // (accessKey, resourceSignature)

    public static final String ENCODE_UTF8 = "UTF-8";

    protected final String accessKey;
    protected final String secretKey;

    protected PavlovHttpClient client;
    private final Consumer<Exception> minIoOnError;

    private static SSLSocketFactory SOCKET_FACTORY;
    static {
        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, new TrustManager[] { new MinioTrustManager() }, new java.security.SecureRandom());
            SOCKET_FACTORY = sc.getSocketFactory();
        } catch (GeneralSecurityException e) {
            // This happens before we are running in osgi
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * sets the SSLSocketFactory to be used when this instance creates sockets for secure https URL connections
     * @param conn
     */
    private static void setSslFactory(final HttpURLConnection conn) {
        if (conn instanceof HttpsURLConnection) {
            ((HttpsURLConnection) conn).setSSLSocketFactory(SOCKET_FACTORY);
        }
    }

    public S3Client(final URL url, final String accessKey, final String secretKey, final Consumer<Exception> minIoOnError) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.minIoOnError = minIoOnError;

        this.client = new PavlovHttpClientImpl().againstUrl(url).beforeConnectRaw(S3Client::setSslFactory);
    }

    /**
     * Determines if a requested bucket exists
     * @param bucketId - id of the bucket being tested
     * @param onError - handles exceptions
     * @return whether a bucket exists
     */
    public boolean headBucket(final String bucketId, final Consumer<Exception> onError) {
        String s3HttpHeaderFormattedNow = DF_S3_HTTP_HEADER.format(new Date());

        Optional<String> formattedBucketId =
                ExceptionUtils.exceptionSupplier(() -> URLEncoder.encode(bucketId, ENCODE_UTF8), this.minIoOnError);

        if (!formattedBucketId.isPresent()) {
            return false;
        }

        String resourcePath = String.format(FORMAT_BUCKET_PATH, formattedBucketId.get());

        String authorizationString =
                calculateAuthorizationString(METHOD_HEAD, MEDIATYPE_NONE, s3HttpHeaderFormattedNow, resourcePath, onError);

        Optional<HttpResponse> response = this.client.clone()
            .withUrlPath(resourcePath)
            .withVerb(HttpVerbs.HEAD)
            .addHeader(HEADER_DATE, s3HttpHeaderFormattedNow)
            .addHeader(HEADER_AUTHORIZATION, authorizationString)
            .execute(this.minIoOnError);

        return response.map(r -> r.isValidResponse(e -> { }))
                .orElse(false);
    }

    /**
     * Returns the bucket list for the service
     * @param onError - handles exceptions
     * @return a list of buckets
     */
    public Optional<String> getAllBuckets(final Consumer<Exception> onError) {
        String s3HttpHeaderFormattedNow = DF_S3_HTTP_HEADER.format(new Date());
        String resourcePath = "/";
        String authorizationString =
                calculateAuthorizationString(METHOD_GET, "", s3HttpHeaderFormattedNow, resourcePath, onError);

        Optional<HttpResponse> response = this.client.clone()
            .withUrlPath(resourcePath)
            .withVerb(HttpVerbs.GET)
            .addHeader(HEADER_DATE, s3HttpHeaderFormattedNow)
            .addHeader(HEADER_AUTHORIZATION, authorizationString)
            .beforeConnectRaw(S3Client::setSslFactory)
            .execute(onError);
       if (response.isPresent()) {
           return Optional.of(response.get().getResponseText());
       }
      return Optional.empty();
    }

    /**
     * Creates a bucket
     * @param bucketId - id of the bucket being created
     * @param onError - handles exceptions
     * @return whether PUT operation is successful
     */
    public boolean putBucket(final String bucketId, final Consumer<Exception> onError) {
        String s3HttpHeaderFormattedNow = DF_S3_HTTP_HEADER.format(new Date());

        Optional<String> formattedBucketId =
                ExceptionUtils.exceptionSupplier(() -> URLEncoder.encode(bucketId, ENCODE_UTF8), onError);

        if (!formattedBucketId.isPresent()) {
            return false;
        }

        String resourcePath = String.format(FORMAT_BUCKET_PATH, formattedBucketId.get());

        String authorizationString =
                calculateAuthorizationString(METHOD_PUT, MEDIATYPE_NONE, s3HttpHeaderFormattedNow, resourcePath, onError);

        Optional<HttpResponse> response = this.client.clone()
                .withUrlPath(resourcePath)
                .withVerb(HttpVerbs.PUT)
                .addHeader(HEADER_DATE, s3HttpHeaderFormattedNow)
                .addHeader(HEADER_AUTHORIZATION, authorizationString)
                .beforeConnectRaw(S3Client::setSslFactory)
                .execute(onError);

        return response.isPresent()
                && response.get().isValidResponse(onError);
    }

    /**
     * Gets a list of object names contained within a bucket
     * @param bucketId - id of the bucket that will have its contents listed
     * @param onError - handles exception
     * @return list of object names in a bucket
     */
    public List<String> getObjectList(final String bucketId, final Consumer<Exception> onError) {
        String s3HttpHeaderFormattedNow = DF_S3_HTTP_HEADER.format(new Date());

        Optional<String> formattedBucketId =
                ExceptionUtils.exceptionSupplier(() -> URLEncoder.encode(bucketId, ENCODE_UTF8), onError);

        if (!formattedBucketId.isPresent()) {
            return Collections.emptyList();
        }

        String resourcePath = String.format(FORMAT_BUCKET_PATH, formattedBucketId.get());

        String authorizationString =
                calculateAuthorizationString(METHOD_GET, MEDIATYPE_NONE, s3HttpHeaderFormattedNow, resourcePath, onError);

        Optional<HttpResponse> response = this.client.clone()
                .withUrlPath(resourcePath)
                .withQueryParameter("list-type", "2")
                .withVerb(HttpVerbs.GET)
                .addHeader(HEADER_DATE, s3HttpHeaderFormattedNow)
                .addHeader(HEADER_AUTHORIZATION, authorizationString)
                .beforeConnectRaw(S3Client::setSslFactory)
                .execute(onError);

        // empty list if invalid or no response
        if (!response.isPresent() || !response.get().isValidResponse(onError)) {
            return Collections.emptyList();
        }

        Persister myPersister = new Persister();

        Optional<ListBucketResult> myResults =
                ExceptionUtils.exceptionSupplier(() ->
                    myPersister.read(ListBucketResult.class, response.get().getResponseText(), false), onError);

        // empty list if no keys or response
        if (!myResults.isPresent() || myResults.get().keyCount <= 0) {
            return Collections.emptyList();
        }

        return myResults.map(r -> r.contents.stream()
                .map(c -> c.key)
                .collect(Collectors.toList()))
              .orElse(Collections.emptyList());
    }

    /**
     * Get an object by its  object name
     * @param bucketId - id of the bucket that contains the object
     * @param objectName - name of the requested  object
     * @param onError - handles exceptions
     * @return an optional input stream containing the object
     */
    public Optional<InputStream> getObject(final String bucketId, final String objectName, final Consumer<Exception> onError) {
        String s3HttpHeaderFormattedNow = DF_S3_HTTP_HEADER.format(new Date());

        Optional<String> formattedBucketId =
                ExceptionUtils.exceptionSupplier(() -> URLEncoder.encode(bucketId, ENCODE_UTF8), onError);

        Optional<String> formattedObjectName =
                ExceptionUtils.exceptionSupplier(() -> URLEncoder.encode(objectName, ENCODE_UTF8), onError);

        if (!formattedBucketId.isPresent() || !formattedObjectName.isPresent()) {
            return Optional.empty();
        }

        String resourcePath = String.format(FORMAT_RESOURCE_PATH, formattedBucketId.get(), formattedObjectName.get());

        String authorizationString =
                calculateAuthorizationString(METHOD_GET, MEDIATYPE_NONE, s3HttpHeaderFormattedNow, resourcePath, onError);

        AtomicReference<InputStream> ret = new AtomicReference<>();
             this.client.clone()
                   .withUrlPath(resourcePath)
                   .withVerb(HttpVerbs.GET)
                   .addHeader(HEADER_DATE, s3HttpHeaderFormattedNow)
                   .addHeader(HEADER_AUTHORIZATION, authorizationString)
                   .asStreaming(ret::set)
                   .execute(onError);

        return Optional.ofNullable(ret.get());
    }

    /**
     * Puts an object into a bucket
     * @param bucketId - id of the bucket the object is being placed in
     * @param objectName - name of the object being placed
     * @param inputStream - inputstream containing the object data
     * @param contentType - type of the data being put in the bucket
     * @param onError - handles exceptions
     * @return Optional of type Boolean
     */
    public Optional<Boolean> putObject(final String bucketId, final String objectName, final InputStream inputStream,
            final String contentType, final Consumer<Exception> onError) {
        String s3HttpHeaderFormattedNow = DF_S3_HTTP_HEADER.format(new Date());

        Optional<String> formattedBucketId =
                ExceptionUtils.exceptionSupplier(() -> URLEncoder.encode(bucketId, ENCODE_UTF8), onError);

        Optional<String> formattedObjectName =
                ExceptionUtils.exceptionSupplier(() -> URLEncoder.encode(objectName, ENCODE_UTF8), onError);

        if (!formattedBucketId.isPresent() || !formattedObjectName.isPresent()) {
            return Optional.of(false);
        }

        String resourcePath = String.format(FORMAT_RESOURCE_PATH, formattedBucketId.get(), formattedObjectName.get());

        String authorizationString =
                calculateAuthorizationString(METHOD_PUT, contentType, s3HttpHeaderFormattedNow, resourcePath, onError);

        Optional<HttpResponse> response =
            this.client.clone()
                .withUrlPath(resourcePath)
                .withData(outputStream -> {
                    handleUpload(inputStream, outputStream, onError);
                })
                .withVerb(HttpVerbs.PUT)
                .withAcceptTypes(HEADERVALUE_ACCEPT_ANY)
                .withContentType(contentType)
                .addHeader(HEADER_DATE, s3HttpHeaderFormattedNow)
                .addHeader(HEADER_AUTHORIZATION, authorizationString)
                .beforeConnectRaw(S3Client::setSslFactory)
                .execute(onError);

        if (!response.isPresent() || !response.get().isValidResponse(onError)) {
            return Optional.of(false);
        }
        return Optional.of(true);
    }

    /**
     * Deletes an object from a bucket
     * @param bucketId - id of the bucket that the object will be deleted from
     * @param objectName - name of the object being deleted
     * @param onError - handles exceptions
     */
    public void deleteObject(final String bucketId, final String objectName, final Consumer<Exception> onError) {

        String s3HttpHeaderFormattedNow = DF_S3_HTTP_HEADER.format(new Date());
        Optional<String> formattedBucketId =
                ExceptionUtils.exceptionSupplier(() -> URLEncoder.encode(bucketId, ENCODE_UTF8), onError);

        Optional<String> formattedObjectName =
                ExceptionUtils.exceptionSupplier(() -> URLEncoder.encode(objectName, ENCODE_UTF8), onError);

        if (formattedBucketId.isPresent() && formattedObjectName.isPresent()) {
            String resourcePath = String.format(FORMAT_RESOURCE_PATH, formattedBucketId.get(), formattedObjectName.get());

            String authorizationString =
                    calculateAuthorizationString(METHOD_DELETE, MEDIATYPE_NONE, s3HttpHeaderFormattedNow, resourcePath, onError);

            this.client.clone()
                .withUrlPath(resourcePath)
                .withVerb(HttpVerbs.DELETE)
                .addHeader(HEADER_DATE, s3HttpHeaderFormattedNow)
                .addHeader(HEADER_AUTHORIZATION, authorizationString)
                .execute(onError);
        }
    }

    /**
     * Reads from an input stream and that is written to an output stream to allow the creating of a file using the output stream
     * @param inputStream - stream of data you want in the file
     * @param outputStream - stream of data that you can then place in the file
     * @param onError - exception consumer
     */
    protected void handleUpload(final InputStream inputStream, final OutputStream outputStream, final Consumer<Exception> onError) {
        byte[] buffer = new byte[UPLOAD_BUFFER_SIZE];
        int bytesRead = -1;
        try {
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            onError.accept(e);
            return;
        }
    }

    // https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html
    protected String calculateResourceSignature(final String method, final String contentType, final String formattedDate,
            final String resourceUrlFragment, final Consumer<Exception> onError) {
        String signee = String.format(FORMAT_RESOURCE_SIGNATURE, method, contentType, formattedDate, resourceUrlFragment);

        try {
            Mac hmac = Mac.getInstance(ALGO_HMAC_SHA1);
            hmac.init(new SecretKeySpec(this.secretKey.getBytes(CHARSET_UTF8), ALGO_HMAC_SHA1));

            return Base64.getEncoder().encodeToString(hmac.doFinal(signee.getBytes(CHARSET_UTF8)));
        } catch (NoSuchAlgorithmException | InvalidKeyException | UnsupportedEncodingException e) {
            onError.accept(e);
            return "";
        }
    }

    /**
     * formats and caclulates the String needed to authorize connection to S3
     * @param method
     * @param contentType
     * @param formattedDate
     * @param resourcePath
     * @param onError
     * @return String that has been formated and the authorization has been calculated
     */
    protected String calculateAuthorizationString(final String method, final String contentType, final String formattedDate,
            final String resourcePath, final Consumer<Exception> onError) {
        return String.format(FORMAT_AUTHORIZATION_STRING, this.accessKey, calculateResourceSignature(
                method, contentType, formattedDate, resourcePath, onError));
    }

}
