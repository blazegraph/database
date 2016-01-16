/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2014.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

package com.bigdata.rdf.sail.webapp.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;

/**
 * Class handles the jetty {@link Response} input stream.
 */
public class JettyResponseListener extends InputStreamResponseListener {
	
    private static final transient Logger log = Logger
            .getLogger(JettyResponseListener.class);
    
    private final long queryTimeoutMillis;
    private volatile Request m_request;
    private volatile Response m_response;
	private volatile InputStream m_cachedStream = null;

	/**
	 * Note: This is the default content encoding for <code>text/*</code> per
	 * Section 3.7.1 Canonicalization and Text Defaults of the HTTP 1.1
	 * specification.
	 */
	private static final String ISO_8859_1 = "ISO-8859-1";
	
	/**
	 * 
	 * @param request
	 * @param queryTimeoutMillis
	 *            the timeout in milliseconds (if non-positive, then an infinite
	 *            timeout is used).
	 */
	public JettyResponseListener(final Request request,
			long queryTimeoutMillis) {
		if (request == null)
			throw new IllegalArgumentException();
		m_request = request;
		this.queryTimeoutMillis = queryTimeoutMillis <= 0L ? Long.MAX_VALUE
				: queryTimeoutMillis;
	}

	/**
	 * Blocks (up to a timeout) for the response to arrive (http status code,
	 * etc.)
	 * 
	 * @throws IOException if there is a problem, if there is a timeout, etc.
	 */
	private void ensureResponse() throws IOException {
		if (m_response == null) {
			try {
				final boolean traceEnabled = log.isTraceEnabled();

				final long start = traceEnabled ? System.currentTimeMillis()
						: 0;

				m_response = get(queryTimeoutMillis, TimeUnit.MILLISECONDS);

				if (traceEnabled)
					log.trace("Response in "
							+ (System.currentTimeMillis() - start) + "ms");

			} catch (InterruptedException | TimeoutException
					| ExecutionException e) {
				throw new IOException(e);
			}
		}		
	}

	/**
	 * Return the value of the <code>Content-Type</code> header.
	 * @return
	 * @throws IOException
	 */
	public String getContentType() throws IOException {
		ensureResponse();
		
		final HttpFields headers = m_response.getHeaders();
		
		return headers.get(HttpHeader.CONTENT_TYPE);
	}

	/**
	 * Return the content encoding specified by the <code>charset</code> MIME
	 * parameter for the <code>Content-Type</code> header and <code>null</code>
	 * if that MIME type parameter was not specified.
	 * <p>
	 * Note: Per Section 3.7.1 Canonicalization and Text Defaults of the HTTP
	 * 1.1 specification:
	 * <ul>
	 * <li>
	 * The "charset" parameter is used with some media types to define the
	 * character set (section 3.4) of the data. When no explicit charset
	 * parameter is provided by the sender, media subtypes of the "text" type
	 * are defined to have a default charset value of "ISO-8859-1" when received
	 * via HTTP. Data in character sets other than "ISO-8859-1" or its subsets
	 * MUST be labeled with an appropriate charset value. See section 3.4.1 for
	 * compatibility problems.</li>
	 * </ul>
	 * 
	 * @return The content encoding if the <code>charset</code> parameter was
	 *         specified and otherwise <code>null</code>.
	 * 
	 * @throws IOException
	 */
	public String getContentEncoding() throws IOException {
		ensureResponse();
				
		final HttpFields headers = m_response.getHeaders();
		
      final String contentType = headers.get(HttpHeader.CONTENT_TYPE);

      if (contentType == null)
         return null;

      final MiniMime mimeType = new MiniMime(contentType);
		
		return mimeType.getContentEncoding();
	}

	/**
	 * The http status code.
	 */
	public int getStatus() throws IOException {
		ensureResponse();
		
		return m_response.getStatus();
	}

	/**
	 * The http reason line.
	 */
	public String getReason() throws IOException {
		ensureResponse();
		
		return m_response.getReason();
	}

	/** The http headers. */
	public HttpFields getHeaders() throws IOException {
		ensureResponse();
		
		return m_response.getHeaders();
	}

	/**
	 * Return the response body as a string.
	 */
	public String getResponseBody() throws IOException {

		final Reader r;
		{
			final String contentEncoding = getContentEncoding();
			if (contentEncoding != null ) {
				/*
				 * Explicit content encoding. 
				 */
				r = new InputStreamReader(getInputStream(), contentEncoding);
			} else if (getContentType()!=null && getContentType().startsWith("text/")) {
				/**
				 * Note: Per Section 3.7.1 Canonicalization and Text Defaults of
				 * the HTTP 1.1 specification:
				 * <p>
				 * The "charset" parameter is used with some media types to
				 * define the character set (section 3.4) of the data. When no
				 * explicit charset parameter is provided by the sender, media
				 * subtypes of the "text" type are defined to have a default
				 * charset value of "ISO-8859-1" when received via HTTP. Data in
				 * character sets other than "ISO-8859-1" or its subsets MUST be
				 * labeled with an appropriate charset value. See section 3.4.1
				 * for compatibility problems.
				 */
				r = new InputStreamReader(getInputStream(),ISO_8859_1);
			} else {
				/*
				 * Also per that section, no default otherwise.
				 */
				r = new InputStreamReader(getInputStream());
			}		
		}

        try {

            final StringWriter w = new StringWriter();

    		final char[] buf = new char[1024];

    		int rdlen = 0;
            
    		while ((rdlen = r.read(buf)) != -1) {
    		
    			w.write(buf, 0, rdlen);
    			
            }

            return w.toString();

        } finally {

            r.close();

        }
        
	}

	@Override
	public InputStream getInputStream() {
		if (m_cachedStream != null) {
			/*
			 * This preserves the semantics of the method on the base class
			 * since Jetty will return a closed input stream if you invoke this
			 * method more than once.
			 */
			return super.getInputStream(); 
		}

		// await a response up to a timeout.
		try {
			ensureResponse();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		// request the input stream.
		m_cachedStream = super.getInputStream();
		
		return m_cachedStream;
	}

	/**
	 * Abort the request/response. The request is associated with the http
	 * request/response is aborted. If we already have the response, then it's
	 * {@link InputStream} is closed.
	 */
	public void abort() {

		// Note: jetty requires a cause for request.abort(Throwable).
		abort(new IOException());
		
	}

	/**
	 * Abort the request/response. The request is associated with the http
	 * request/response is aborted. If we already have the response, then it's
	 * {@link InputStream} is closed.
	 * 
	 * @param cause
	 *            The cause (required).
	 */
	public void abort(final Throwable cause) {

		final InputStream is = m_cachedStream;
		if (is != null) {
			m_cachedStream = null;
			try {
				is.close();
			} catch (IOException ex) {
				log.warn(ex);
			}
		}

		final Request r = m_request;
		if (r != null) {
			m_request = null;
			r.abort(cause);
		}

	}

}
