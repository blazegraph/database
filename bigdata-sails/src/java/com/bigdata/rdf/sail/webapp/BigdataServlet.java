package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.journal.IIndexManager;

/**
 * Useful glue for implementing service actions, but does not directly implement
 * any service action/
 */
abstract public class BigdataServlet extends HttpServlet {
	
	/**
     * 
     */
    private static final long serialVersionUID = 1L;

    private static final transient Logger log = Logger.getLogger(BigdataServlet.class); 

    /**
     * The name of the {@link ServletContext} attribute whose value is the
     * {@link IIndexManager}.
     */
    /*package*/ static final transient String ATTRIBUTE_INDEX_MANAGER = 
        IIndexManager.class.getName();
    
	/**
	 * The character set used for the response (not negotiated).
	 */
    static protected final String charset = "UTF-8";

    protected static final transient String GET = "GET";
    protected static final transient String POST = "POST";
    protected static final transient String PUT = "PUT";
    protected static final transient String DELETE = "DELETE";
    
	/**
	 * Some HTTP response status codes
	 */
	public static final transient int
		HTTP_OK = HttpServletResponse.SC_ACCEPTED,
		HTTP_REDIRECT = HttpServletResponse.SC_TEMPORARY_REDIRECT,
		HTTP_FORBIDDEN = HttpServletResponse.SC_FORBIDDEN,
		HTTP_NOTFOUND = HttpServletResponse.SC_NOT_FOUND,
        HTTP_BADREQUEST = HttpServletResponse.SC_BAD_REQUEST,
        HTTP_METHOD_NOT_ALLOWED = HttpServletResponse.SC_METHOD_NOT_ALLOWED,
		HTTP_INTERNALERROR = HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
		HTTP_NOTIMPLEMENTED = HttpServletResponse.SC_NOT_IMPLEMENTED;

	/**
	 * Common mime types for dynamic content
	 */
	public static final transient String
		MIME_TEXT_PLAIN = "text/plain",
		MIME_TEXT_HTML = "text/html",
		MIME_TEXT_XML = "text/xml",
		MIME_DEFAULT_BINARY = "application/octet-stream",
        MIME_APPLICATION_XML = "application/xml",
        MIME_TEXT_JAVASCRIPT = "text/javascript",
        /**
         * The traditional encoding of URL query parameters within a POST
         * message body.
         */
        MIME_APPLICATION_URL_ENCODED = "application/x-www-form-urlencoded";

    protected <T> T getRequiredServletContextAttribute(final String name) {

        final T v = (T) getServletContext().getAttribute(name);

        if (v == null)
            throw new RuntimeException("Not set: " + name);

        return v;

    }

	protected IIndexManager getIndexManager() {
	
	    return getRequiredServletContextAttribute(ATTRIBUTE_INDEX_MANAGER);
	    
	}
	
    static protected void buildResponse(HttpServletResponse resp, int status,
            String mimeType) throws IOException {

        resp.setStatus(status);

        resp.setContentType(mimeType);

    }

    static protected void buildResponse(HttpServletResponse resp, int status,
            String mimeType, String content) throws IOException {

        buildResponse(resp, status, mimeType);

        resp.getWriter().print(content);

    }

    static protected void buildResponse(HttpServletResponse resp, int status,
            String mimeType, InputStream content) throws IOException {

        buildResponse(resp, status, mimeType);

        copyStream(content, resp.getOutputStream());

    }

    /**
     * Copy the input stream to the output stream.
     * 
     * @param content
     *            The input stream.
     * @param outstr
     *            The output stream.
     *            
     * @throws IOException
     */
    static protected void copyStream(final InputStream content,
            final OutputStream outstr) throws IOException {

        final byte[] buf = new byte[1024];

        while (true) {
        
            final int rdlen = content.read(buf);
            
            if (rdlen <= 0) {
            
                break;
                
            }
            
            outstr.write(buf, 0, rdlen);
            
        }

    }
    
}
