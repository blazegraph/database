/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Mar 8, 2011
 */

package com.bigdata.util.httpd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase2;

import com.bigdata.rawstore.Bytes;
import com.bigdata.util.config.NicUtil;

/**
 * Test suite for {@link NanoHTTPD}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNanoHTTPD extends TestCase2 {

    /**
     * 
     */
    public TestNanoHTTPD() {
    }

    /**
     * @param name
     */
    public TestNanoHTTPD(String name) {
        super(name);
    }

    public void test_startShutdownNow() throws IOException {

        final NanoHTTPD fixture = new NanoHTTPD(0/* port */);

        try {

            assertNotSame("port", 0, fixture.getPort());

            assertTrue("open", fixture.isOpen());

        } finally {

            fixture.shutdownNow();

        }

        assertFalse("open", fixture.isOpen());

    }

    public void test_startShutdown() throws IOException, InterruptedException,
            ExecutionException, TimeoutException {

        final NanoHTTPD fixture = new NanoHTTPD(0/* port */);

        try {

            final ExecutorService executor = Executors
                    .newSingleThreadScheduledExecutor();

            try {

                assertNotSame("port", 0, fixture.getPort());

                assertTrue("open", fixture.isOpen());

                final FutureTask<Void> ft = new FutureTask<Void>(
                        new Runnable() {
                            public void run() {
                                fixture.shutdown();
                            }
                        }, null/* Void */);

                executor.submit(ft);

                // wait up to a timeout for the service to terminate.
                ft.get(1000L, TimeUnit.MILLISECONDS);

                assertFalse("open", fixture.isOpen());

            } finally {

                executor.shutdownNow();

            }

        } finally {

            fixture.shutdownNow();

        }

        assertFalse("open", fixture.isOpen());

    }

    /*
     * GET
     */
    
    public void test_get() throws IOException {

        final NanoHTTPD fixture = new NanoHTTPD(0/* port */) {

            @Override
            public Response serve(final Request req) {

                if (GET.equalsIgnoreCase(req.method)) {

                    // One signal for GET .
                    return new Response(HTTP_OK, MIME_TEXT_PLAIN, "");

                }

                // Another for any other method or resource.
                return new Response(HTTP_INTERNALERROR, MIME_TEXT_PLAIN, 
                        req.method);

            }

        };

        try {

            // Note: Use ZERO (0) for an infinite timeout to debug.
            // final int timeout = 0;// timeout (ms).
            final int timeout = 1000;// timeout (ms).

            final int port = fixture.getPort();

            final String hostAddr = NicUtil.getIpAddress("default.nic",
                    "default", true/* loopbackOk */);

            final URL url = new URL("http", hostAddr, port, ""/* file */);

            if (log.isInfoEnabled())
                log.info("url: " + url);

            HttpURLConnection conn = null;
            try {

                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.setDoOutput(true);
                conn.setUseCaches(false);
                conn.setReadTimeout(timeout);
                // conn.setRequestProperty("Accept", MIME_RDF_XML);

                // connect.
                conn.connect();

                final int rc = conn.getResponseCode();
                if (rc < 200 || rc >= 300) {
                    throw new IOException(rc + " : "
                            + conn.getResponseMessage() + " : " + url);
                }

                // if (log.isInfoEnabled())
                // log.info("Status Line: " + conn.getResponseMessage());

                /*
                 * Write out the response body
                 */
                {

                    final LineNumberReader r = new LineNumberReader(
                            new InputStreamReader(
                                    conn.getInputStream(),
                                    conn.getContentEncoding() == null ? "ISO-8859-1"
                                            : conn.getContentEncoding()));
                    try {
                        String s;
                        while ((s = r.readLine()) != null) {
                            if (log.isInfoEnabled())
                                log.info(s);
                        }
                    } finally {
                        r.close();
                    }

                }

                // final long elapsed = System.nanoTime() - begin;

            } finally {

                // clean up the connection resources
                if (conn != null)
                    conn.disconnect();

            }

        } finally {

            fixture.shutdownNow();

        }

    }

    /**
     * Unit test for URL query parameter decode for GET.
     */
    public void test_getDecodeParams() throws IOException {

        final LinkedHashMap<String, Vector<String>> expected = new LinkedHashMap<String, Vector<String>>();
        {
            {
                final Vector<String> v = new Vector<String>();
                v.add("1");
                v.add("2");
                v.add("3");
                expected.put("a", v);
            }
            {
                final Vector<String> v = new Vector<String>();
                v.add("a b c");
                v.add("d e");
                v.add("f");
                expected.put("blue", v);
            }
        }
        
        final NanoHTTPD fixture = new NanoHTTPD(0/* port */) {
            
            @Override
            public Response serve(final Request req) {

                if (GET.equalsIgnoreCase(req.method)) {

                    assertEquals("size", expected.size(), req.params.size());

                    for (Map.Entry<String, Vector<String>> e : expected
                            .entrySet()) {
                    
                        assertEquals(e.getKey(), expected.get(e.getKey()),
                                req.params.get(e.getKey()));
                        
                    }
                    
                    // One signal for GET .
                    return new Response(HTTP_OK, MIME_TEXT_PLAIN, "");

                }

                // Another for any other method or resource.
                return new Response(HTTP_INTERNALERROR, MIME_TEXT_PLAIN, 
                        req.method);

            }

        };

        try {

            // Note: Use ZERO (0) for an infinite timeout to debug.
            // final int timeout = 0;// timeout (ms).
            final int timeout = 1000;// timeout (ms).

            final int port = fixture.getPort();

            final String hostAddr = NicUtil.getIpAddress("default.nic",
                    "default", true/* loopbackOk */);

            final StringBuilder queryParms = NanoHTTPD.encodeParams(expected);

            final URL url = new URL("http", hostAddr, port, "/?" + queryParms);

            if (log.isInfoEnabled())
                log.info("url: " + url);

            HttpURLConnection conn = null;
            try {

                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.setDoOutput(true);
                conn.setUseCaches(false);
                conn.setReadTimeout(timeout);
                // conn.setRequestProperty("Accept", MIME_RDF_XML);

                // connect.
                conn.connect();

                final int rc = conn.getResponseCode();
                if (rc < 200 || rc >= 300) {
                    throw new IOException(rc + " : "
                            + conn.getResponseMessage() + " : " + url);
                }

                // if (log.isInfoEnabled())
                // log.info("Status Line: " + conn.getResponseMessage());

                /*
                 * Write out the response body
                 */
                {

                    final LineNumberReader r = new LineNumberReader(
                            new InputStreamReader(
                                    conn.getInputStream(),
                                    conn.getContentEncoding() == null ? "ISO-8859-1"
                                            : conn.getContentEncoding()));
                    try {
                        String s;
                        while ((s = r.readLine()) != null) {
                            if (log.isInfoEnabled())
                                log.info(s);
                        }
                    } finally {
                        r.close();
                    }

                }

                // final long elapsed = System.nanoTime() - begin;

            } finally {

                // clean up the connection resources
                if (conn != null)
                    conn.disconnect();

            }

        } finally {

            fixture.shutdownNow();

        }

    }

    public void test_getStream() throws IOException {

        final byte[] data = new byte[Bytes.kilobyte32 * 8];
        {
        
            final Random r = new Random();

            r.nextBytes(data);
            
        }

        final NanoHTTPD fixture = new NanoHTTPD(0/* port */) {
            
            @Override
            public Response serve(final Request req) {

                if (GET.equalsIgnoreCase(req.method)) {

                    // One signal for GET .
                    return new Response(HTTP_OK, MIME_DEFAULT_BINARY,
                            new ByteArrayInputStream(data));

                }

                // Another for any other method or resource.
                return new Response(HTTP_INTERNALERROR, MIME_TEXT_PLAIN, 
                        req.method);

            }

        };

        try {

            // Note: Use ZERO (0) for an infinite timeout to debug.
            // final int timeout = 0;// timeout (ms).
            final int timeout = 1000;// timeout (ms).

            final int port = fixture.getPort();

            final String hostAddr = NicUtil.getIpAddress("default.nic",
                    "default", true/* loopbackOk */);

            final URL url = new URL("http", hostAddr, port, ""/* file */);

            if (log.isInfoEnabled())
                log.info("url: " + url);

            HttpURLConnection conn = null;
            try {

                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.setDoOutput(true);
                conn.setUseCaches(false);
                conn.setReadTimeout(timeout);
                // conn.setRequestProperty("Accept", MIME_RDF_XML);

                // connect.
                conn.connect();

                final int rc = conn.getResponseCode();
                if (rc < 200 || rc >= 300) {
                    throw new IOException(rc + " : "
                            + conn.getResponseMessage() + " : " + url);
                }

                // if (log.isInfoEnabled())
                // log.info("Status Line: " + conn.getResponseMessage());

                /*
                 * Read the response body and compare it with the expected data.
                 */
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                {

                    final InputStream is = conn.getInputStream();

                    try {
                        int b;
                        while ((b = is.read()) != -1) {
                            baos.write(b);
                        }
                    } finally {
                        is.close();
                    }

                    baos.flush();
                }

                assertEquals(data.length, baos.size());
                assertEquals(data, baos.toByteArray());

            } finally {

                // clean up the connection resources
                if (conn != null)
                    conn.disconnect();

            }

        } finally {

            fixture.shutdownNow();

        }

    }

    /*
     * POST
     */
    
    /**
     * Post with an empty request body.
     */
    public void test_post_emptyBody() throws IOException {

        final NanoHTTPD fixture = new NanoHTTPD(0/* port */) {

            @Override
            public Response serve(final Request req) {

                if (POST.equalsIgnoreCase(req.method)) {

                    // One signal for POST.
                    return new Response(HTTP_OK, MIME_TEXT_PLAIN, "");

                }

                // Another for any other method or resource.
                return new Response(HTTP_INTERNALERROR, MIME_TEXT_PLAIN, 
                        req.method);

            }

        };

        try {

            // Note: Use ZERO (0) for an infinite timeout to debug.
             final int timeout = 0;// timeout (ms).
//            final int timeout = 1000;// timeout (ms).

            final int port = fixture.getPort();

            final String hostAddr = NicUtil.getIpAddress("default.nic",
                    "default", true/* loopbackOk */);

            final URL url = new URL("http", hostAddr, port, ""/* file */);

            if (log.isInfoEnabled())
                log.info("url: " + url);

            HttpURLConnection conn = null;
            try {

                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setDoOutput(true);
                conn.setUseCaches(false);
                conn.setReadTimeout(timeout);
                // conn.setRequestProperty("Accept", MIME_RDF_XML);

                // connect.
                conn.connect();
                
                // send an empty POST body.
                conn.getOutputStream().close();
                
                final int rc = conn.getResponseCode();
                if (rc < 200 || rc >= 300) {
                    throw new IOException(rc + " : "
                            + conn.getResponseMessage() + " : " + url);
                }

                // if (log.isInfoEnabled())
                // log.info("Status Line: " + conn.getResponseMessage());

                /*
                 * Read the response body
                 */
                {

                    final LineNumberReader r = new LineNumberReader(
                            new InputStreamReader(
                                    conn.getInputStream(),
                                    conn.getContentEncoding() == null ? "ISO-8859-1"
                                            : conn.getContentEncoding()));
                    try {
                        String s;
                        while ((s = r.readLine()) != null) {
                            if (log.isInfoEnabled())
                                log.info(s);
                        }
                    } finally {
                        r.close();
                    }

                }

                // final long elapsed = System.nanoTime() - begin;

            } finally {

                // clean up the connection resources
                if (conn != null)
                    conn.disconnect();

            }

        } finally {

            fixture.shutdownNow();

        }

    }

    /**
     * Post with <code>application/x-www-form-urlencoded</code> parameters in a
     * request body.
     */
    public void test_post_application_x_www_form_urlencoded() throws IOException {

        final LinkedHashMap<String, Vector<String>> expected = new LinkedHashMap<String, Vector<String>>();
        {
            {
                final Vector<String> v = new Vector<String>();
                v.add("1");
                v.add("2");
                v.add("3");
                expected.put("a", v);
            }
            {
                final Vector<String> v = new Vector<String>();
                v.add("a b c");
                v.add("d e");
                v.add("f");
                expected.put("blue", v);
            }
        }

        final NanoHTTPD fixture = new NanoHTTPD(0/* port */) {

            @Override
            public Response serve(final Request req) {

                if (POST.equalsIgnoreCase(req.method)) {

                    assertEquals("size", expected.size(), req.params.size());

                    for (Map.Entry<String, Vector<String>> e : expected
                            .entrySet()) {
                    
                        assertEquals(e.getKey(), expected.get(e.getKey()),
                                req.params.get(e.getKey()));
                        
                    }
                    
                    // One signal for POST.
                    return new Response(HTTP_OK, MIME_TEXT_PLAIN, "");

                }

                // Another for any other method or resource.
                return new Response(HTTP_INTERNALERROR, MIME_TEXT_PLAIN, 
                        req.method);

            }

        };

        try {

            // Note: Use ZERO (0) for an infinite timeout to debug.
             final int timeout = 0;// timeout (ms).
//            final int timeout = 1000;// timeout (ms).

            final int port = fixture.getPort();

            final String hostAddr = NicUtil.getIpAddress("default.nic",
                    "default", true/* loopbackOk */);

            final StringBuilder queryParms = NanoHTTPD.encodeParams(expected);

            final URL url = new URL("http", hostAddr, port, "/?" + queryParms);

            if (log.isInfoEnabled())
                log.info("url: " + url);

            HttpURLConnection conn = null;
            try {

                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setDoOutput(false);
                conn.setUseCaches(false);
                conn.setReadTimeout(timeout);
                // conn.setRequestProperty("Accept", MIME_RDF_XML);

                // connect.
                conn.connect();
                
                final int rc = conn.getResponseCode();
                if (rc < 200 || rc >= 300) {
                    throw new IOException(rc + " : "
                            + conn.getResponseMessage() + " : " + url);
                }

                // if (log.isInfoEnabled())
                // log.info("Status Line: " + conn.getResponseMessage());

                /*
                 * Read the response body
                 */
                {

                    final LineNumberReader r = new LineNumberReader(
                            new InputStreamReader(
                                    conn.getInputStream(),
                                    conn.getContentEncoding() == null ? "ISO-8859-1"
                                            : conn.getContentEncoding()));
                    try {
                        String s;
                        while ((s = r.readLine()) != null) {
                            if (log.isInfoEnabled())
                                log.info(s);
                        }
                    } finally {
                        r.close();
                    }

                }

                // final long elapsed = System.nanoTime() - begin;

            } finally {

                // clean up the connection resources
                if (conn != null)
                    conn.disconnect();

            }

        } finally {

            fixture.shutdownNow();

        }

    }

    /**
     * Post with a non-empty request body using
     * <code>application/octet-stream</code>. The request body is written using
     * the raw {@link OutputStream}.
     */
    public void test_post_application_octet_stream_usingOutputStream()
            throws IOException {

        final byte[] expected = "Hello World!".getBytes();
        
        final NanoHTTPD fixture = new NanoHTTPD(0/* port */) {

            @Override
            public Response serve(final Request req) {

                if (POST.equalsIgnoreCase(req.method)) {

                    try {

                        final byte[] actual = req.getBinaryContent();
                        
                        assertEquals(expected, actual);
                        
                        // One signal for POST.
                        return new Response(HTTP_OK, MIME_TEXT_PLAIN, "");
                        
                    } catch (IOException e) {
                        
                        throw new RuntimeException(e);

                    }
                    
                }

                // Another for any other method or resource.
                return new Response(HTTP_INTERNALERROR, MIME_TEXT_PLAIN, 
                        req.method);

            }

        };

        try {

            // Note: Use ZERO (0) for an infinite timeout to debug.
//             final int timeout = 0;// timeout (ms).
            final int timeout = 1000;// timeout (ms).

            final int port = fixture.getPort();

            final String hostAddr = NicUtil.getIpAddress("default.nic",
                    "default", true/* loopbackOk */);

            final URL url = new URL("http", hostAddr, port, ""/* file */);

            if (log.isInfoEnabled())
                log.info("url: " + url);

            HttpURLConnection conn = null;
            try {

                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setDoOutput(true);
                conn.setUseCaches(false);
                conn.setReadTimeout(timeout);

                // send an POST body.
                {
                    conn.addRequestProperty("Content-Type",
                            NanoHTTPD.MIME_DEFAULT_BINARY);
                    final OutputStream os = conn.getOutputStream();
                    os.write(expected);
                    os.flush();
                    os.close();
                }
                // connect.
                conn.connect();

                final int rc = conn.getResponseCode();
                if (rc < 200 || rc >= 300) {
                    throw new IOException(rc + " : "
                            + conn.getResponseMessage() + " : " + url);
                }

                /*
                 * Read the response body
                 */
                {

                    final LineNumberReader r = new LineNumberReader(
                            new InputStreamReader(
                                    conn.getInputStream(),
                                    conn.getContentEncoding() == null ? "ISO-8859-1"
                                            : conn.getContentEncoding()));
                    try {
                        String s;
                        while ((s = r.readLine()) != null) {
                            if (log.isInfoEnabled())
                                log.info(s);
                        }
                    } finally {
                        r.close();
                    }

                }

                // final long elapsed = System.nanoTime() - begin;

            } finally {

                // clean up the connection resources
                if (conn != null)
                    conn.disconnect();

            }

        } finally {

            fixture.shutdownNow();

        }

    }

    /**
     * Post with a non-empty request body using
     * <code>application/octet-stream</code>. The request body is written using
     * a {@link Writer}.
     * 
     * @throws IOException
     */
    public void test_post_application_octet_stream_usingWriter() throws IOException {

        final String expected = "Hello World!";
        
        final NanoHTTPD fixture = new NanoHTTPD(0/* port */) {

            @Override
            public Response serve(final Request req) {

                if (POST.equalsIgnoreCase(req.method)) {

                    try {

                        final String actual = req.getStringContent();
                        
                        assertEquals(expected, actual);
                        
                        // One signal for POST.
                        return new Response(HTTP_OK, MIME_TEXT_PLAIN, "");
                        
                    } catch (IOException e) {
                        
                        throw new RuntimeException(e);

                    }
                    
                }

                // Another for any other method or resource.
                return new Response(HTTP_INTERNALERROR, MIME_TEXT_PLAIN, 
                        req.method);

            }

        };

        try {

            // Note: Use ZERO (0) for an infinite timeout to debug.
             final int timeout = 0;// timeout (ms).
//            final int timeout = 1000;// timeout (ms).

            final int port = fixture.getPort();

            final String hostAddr = NicUtil.getIpAddress("default.nic",
                    "default", true/* loopbackOk */);

            final URL url = new URL("http", hostAddr, port, ""/* file */);

            if (log.isInfoEnabled())
                log.info("url: " + url);

            HttpURLConnection conn = null;
            try {

                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setDoOutput(true);
                conn.setUseCaches(false);
                conn.setReadTimeout(timeout);
                // conn.setRequestProperty("Accept", MIME_RDF_XML);

                // send an POST body.
                {
                    conn.addRequestProperty("Content-Type",
                            NanoHTTPD.MIME_TEXT_PLAIN);
                    final OutputStream os = conn.getOutputStream();
                    final Writer w = new OutputStreamWriter(os);
                    w.write(expected);
                    w.flush();
                    w.close();
                    os.flush();
                    os.close();
                }
                // connect.
                conn.connect();

                final int rc = conn.getResponseCode();
                if (rc < 200 || rc >= 300) {
                    throw new IOException(rc + " : "
                            + conn.getResponseMessage() + " : " + url);
                }

                // if (log.isInfoEnabled())
                // log.info("Status Line: " + conn.getResponseMessage());

                /*
                 * Read the response body
                 */
                {

                    final LineNumberReader r = new LineNumberReader(
                            new InputStreamReader(
                                    conn.getInputStream(),
                                    conn.getContentEncoding() == null ? "ISO-8859-1"
                                            : conn.getContentEncoding()));
                    try {
                        String s;
                        while ((s = r.readLine()) != null) {
                            if (log.isInfoEnabled())
                                log.info(s);
                        }
                    } finally {
                        r.close();
                    }

                }

                // final long elapsed = System.nanoTime() - begin;

            } finally {

                // clean up the connection resources
                if (conn != null)
                    conn.disconnect();

            }

        } finally {

            fixture.shutdownNow();

        }

    }

}
