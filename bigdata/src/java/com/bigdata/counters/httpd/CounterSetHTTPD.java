package com.bigdata.counters.httpd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Properties;

import com.bigdata.counters.CounterSet;
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
    
    public Response doGet( String uri, String method, Properties header, Properties parms ) throws Exception {
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream(2 * Bytes.kilobyte32);

        final String charset = "UTF-8";
        
        final String mimeType;
        
        if (true) {

            // conneg HMTL
            
            mimeType = MIME_TEXT_HTML;
            
            OutputStreamWriter w = new OutputStreamWriter(baos);

            new XHTMLRenderer(root, uri, parms).write(w);

            w.flush();

        } else {

            // conneg XML

            mimeType = MIME_APPLICATION_XML;

            // send everything - client can apply XSLT as desired.            
            root.asXML(baos, charset, null/*filter*/);

        }

        InputStream is = new ByteArrayInputStream(baos.toByteArray());

        Response r = new Response(HTTP_OK, mimeType + "; charset='" + charset
                + "'", is);

        /*
         * @todo set cache headers -- the data should be good for 60 seconds
         * unless you change the query parameters.
         */
        
        r.addHeader("Cache-Control", "no-cache");
        
        return r;
        
    }
    
}
