package com.bigdata.counters.httpd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Vector;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.httpd.XHTMLRenderer.Model;
import com.bigdata.rawstore.Bytes;
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
    
    public CounterSetHTTPD(int port, CounterSet root) throws IOException {

        super(port);
        
        this.root = root;
        
    }
    
    public Response doGet(String uri, String method, Properties header,
            LinkedHashMap<String, Vector<String>> parms) throws Exception {
        
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(
                2 * Bytes.kilobyte32);

        final String charset = "UTF-8";
        
        final String mimeType;
        
        if (true) {

            // conneg HMTL
            
            mimeType = MIME_TEXT_HTML;
            
            final OutputStreamWriter w = new OutputStreamWriter(baos);

            // build model of the controller state.
            final Model model = new Model(root, uri, parms);
            
            // @todo if controller state error then send HTTP_BAD_REQUEST
            
            // render the view.
            new XHTMLRenderer(model).write(w);

            w.flush();

        } else {

            // conneg XML

            mimeType = MIME_APPLICATION_XML;

            // send everything - client can apply XSLT as desired.            
            root.asXML(baos, charset, null/*filter*/);

        }

        final InputStream is = new ByteArrayInputStream(baos.toByteArray());

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
    
}
