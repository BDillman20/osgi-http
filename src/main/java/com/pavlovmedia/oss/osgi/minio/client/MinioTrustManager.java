package com.pavlovmedia.oss.osgi.minio.client;


import java.security.cert.X509Certificate;

import javax.net.ssl.X509TrustManager;

/**
 * This is a TEMPORARY HACK to get around cert issues.
 * @author Ira Greenstein {@literal <igreenstein@pavlovmedia.com>}
 *
 */
public class MinioTrustManager implements X509TrustManager {
    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }

    /**
     * Given the peer certification chain, build a certificate path to a trusted root and return if it can be
     *  validated and is trusted for client SSL authentication based on the authentication type
     *  @param certs - the peer certification chain
     *  @param authType - the authentication type based on the client certifications
     */
    @Override
    public void checkClientTrusted(final X509Certificate[] certs, final String authType) {
    }

    /**
     * Given the peer certification chain, build a certificate path to a trusted root and return if it can be
     *  validated and is trusted for client SSL authentication based on the authentication type
     *  @param certs - the peer certification chain
     *  @param authType - the authentication type based on the client certifications
     */
    @Override
    public void checkServerTrusted(final X509Certificate[] certs, final String authType) {
    }
}
