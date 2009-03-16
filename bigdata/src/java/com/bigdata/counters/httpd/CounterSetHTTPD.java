package com.bigdata.counters.httpd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Vector;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.httpd.XHTMLRenderer.Model;
import com.bigdata.rawstore.Bytes;
import com.bigdata.service.IService;
import com.bigdata.util.httpd.AbstractHTTPD;

/**
 * Exposes a {@link CounterSet} via HTTPD.
 * 
 * @todo Write XSL and stylesheet for interactive browsing of the CounterSet XML
 *       and support conneg for XML result.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CounterSetHTTPD extends AbstractHTTPD {
    
    protected final CounterSet root;
    
    /**
     * The service reference iff one one specified to the ctor (may be null).
     */
    protected final IService service;

    /**
     * An immutable collection of pre-declared classpath resources which can be
     * downloaded via httpd.
     */
    private final Collection<String> allowedClassPathResources;
    
    public CounterSetHTTPD(final int port, final CounterSet root) throws IOException {

        this(port, root, null/*fed*/);
        
    }
    
    /**
     * 
     * @param port
     * @param root
     * @param service
     *            Optional reference to the service within which this httpd is
     *            hosted.
     * @throws IOException
     */
    public CounterSetHTTPD(final int port, final CounterSet root,
            final IService service) throws IOException {

        super(port);

        this.root = root;
        
        // Note: MAY be null.
        this.service = service;
    
        this.allowedClassPathResources = Collections.unmodifiableCollection(Collections.synchronizedCollection(Arrays.asList(new String[]{
                "jquery.js",
                "jquery.flot.js",
                "excanvas.pack.js"
        })));
        
    }
    
    public Response doGet(final String uri, final String method,
            final Properties header,
            final LinkedHashMap<String, Vector<String>> parms) throws Exception {
        
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(
                2 * Bytes.kilobyte32);

        final String charset = "UTF-8";
        
        final String mimeType;
        
        final InputStream is;

        /*
         * Note: This hacks specific downloadable resources which are accessed
         * using the classpath.
         */
        if (uri.contains("jquery.js")) {

            return sendClasspathResource(MIME_TEXT_HTML, "jquery.js");

        } else if (uri.contains("jquery.flot.js")) {

            return sendClasspathResource(MIME_TEXT_HTML, "jquery.flot.js");

        } else if (uri.contains("excanvas.pack.js")) {

            return sendClasspathResource(MIME_TEXT_HTML, "excanvas.pack.js");

        } else if (true) {

            // conneg HMTL
            
            mimeType = MIME_TEXT_HTML;
            
            final OutputStreamWriter w = new OutputStreamWriter(baos);

            // build model of the controller state.
            final Model model = new Model(service, root, uri, parms, header);
            
            // @todo if controller state error then send HTTP_BAD_REQUEST
            
            // render the view.
            new XHTMLRenderer(model).write(w);

            w.flush();

            is = new ByteArrayInputStream(baos.toByteArray());
            
        } else {

            // conneg XML

            mimeType = MIME_APPLICATION_XML;

            // send everything - client can apply XSLT as desired.            
            root.asXML(baos, charset, null/*filter*/);

            is = new ByteArrayInputStream(baos.toByteArray());
            
        }

        final Response r = new Response(HTTP_OK, mimeType + "; charset='"
                + charset + "'", is);

        /*
         * Sets the cache behavior -- the data should be good for up to 60
         * seconds unless you change the query parameters. These cache control
         * parameters SHOULD indicate that the response is valid for 60 seconds,
         * that the client must revalidate, and that the response is cachable
         * even if the client was authenticated.
         */
        r.addHeader("Cache-Control", "max-age=60, must-revalidate, public");
        // to disable caching.
        // r.addHeader("Cache-Control", "no-cache");

        return r;
        
    }

    /**
     * Sewnd a resource from the classpath.
     * 
     * @param mimeType
     *            The mime type for the response.
     * @param resource
     *            The resource.
     * @return The {@link Response} which will send that resource.
     * @throws RuntimeException
     *             if the resource is not found on the classpath.
     * @throws RuntimeException
     *             if the resource was not explicitly declared as a resource
     *             which may be sent in response to an httpd request.
     */
    protected Response sendClasspathResource(final String mimeType,
            final String resource) {
        
        if (mimeType == null)
            throw new IllegalArgumentException();

        if (resource == null)
            throw new IllegalArgumentException();
        
        if (allowedClassPathResources.contains(resource)) {

            throw new RuntimeException("Resource not pre-declared: " + resource);
            
        }
        
        final InputStream is = getClass().getResourceAsStream(resource);

        if (is == null) {

            throw new RuntimeException("Resource not on classpath: " + resource);

        }

        if (INFO)
            log.info("Serving: " + resource + " as " + mimeType);

        /*
         * Note: This response may be cached.
         * 
         * Note: The Response will consume and close the InputStream.
         */

        final String charset = "UTF-8";

        final Response r = new Response(HTTP_OK, mimeType + "; charset='"
                + charset + "'", is);

        return r;

    }

}
