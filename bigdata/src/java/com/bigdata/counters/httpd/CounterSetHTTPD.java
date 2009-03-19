package com.bigdata.counters.httpd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
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
     * Class used to pre-declare classpath resources that are available for
     * download via httpd.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class DeclaredResource {
        
        final String localResource;
        final String mimeType;

        /**
         * 
         * @param localResource
         *            The name of a resource to be located using the classpath.
         * @param mimeType
         *            The mime type of that resource.
         */
        public DeclaredResource(final String localResource,
                final String mimeType) throws IOException {
            
            this.localResource = localResource;

            this.mimeType = mimeType;

            if (localResource == null)
                throw new IllegalArgumentException();

            if (mimeType == null)
                throw new IllegalArgumentException();

            // verify that the resource is on the classpath.
            InputStream is = getInputStream();

            is.close();

        }

        public InputStream getInputStream() throws IOException {

            final InputStream is = getClass()
                    .getResourceAsStream(localResource);

            if (is == null) {

                throw new IOException("Resource not on classpath: "
                        + localResource);

            }

            return is;
            
        }

    }
    
    /**
     * An immutable collection of pre-declared classpath resources which can be
     * downloaded via httpd.
     */
    private final Map<String/*uri*/,DeclaredResource> allowedClassPathResources;
    
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
    
        final HashMap<String/* uri */, DeclaredResource> map = new HashMap<String, DeclaredResource>();

        /*
         * Pre-declare resources that will be served from the CLASSPATH.
         */

        map.put("/jquery.js", new DeclaredResource("jquery.js",
                MIME_TEXT_JAVASCRIPT + "; charset='UTF-8'"));

        map.put("/jquery.flot.js", new DeclaredResource("jquery.flot.js",
                MIME_TEXT_JAVASCRIPT + "; charset='UTF-8'"));

        map.put("/excanvas.pack.js", new DeclaredResource("excanvas.pack.js",
                MIME_TEXT_JAVASCRIPT + "; charset='UTF-8'"));

        this.allowedClassPathResources = Collections
                .unmodifiableMap(Collections.synchronizedMap(map));
        
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
         * If the request uri is one of the pre-declared resources then we send
         * that resource.
         */
        final DeclaredResource decl = allowedClassPathResources.get(uri);

        if (decl != null) {

            // send that resource.
            return sendClasspathResource(decl);

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
     * Send a resource from the classpath.
     * 
     * @param decl
     *            A pre-declared resource.
     * 
     * @return The {@link Response} which will send that resource.
     * 
     * @throws IOException
     *             if the resource is not found on the classpath.
     */
    final private Response sendClasspathResource(final DeclaredResource decl)
            throws IOException {

        if (decl == null)
            throw new IllegalArgumentException();

        if (INFO)
            log.info("Serving: " + decl.localResource + " as " + decl.mimeType);

        /*
         * Note: This response may be cached.
         * 
         * Note: The Response will consume and close the InputStream.
         */

        final Response r = new Response(HTTP_OK, decl.mimeType, decl
                .getInputStream());

        return r;

    }

}
