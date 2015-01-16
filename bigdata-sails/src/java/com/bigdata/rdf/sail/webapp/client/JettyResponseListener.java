/**
Copyright (C) SYSTAP, LLC 2014.  All rights reserved.

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
    
    private volatile Request m_request;
    private volatile Response m_response;
	private volatile InputStream m_cachedStream = null;

	// FIXME Configuration parameter for maxBufferSize:long
	public JettyResponseListener(final Request request) {
		if (request == null)
			throw new IllegalArgumentException();
		m_request = request;
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
				
				final long start = traceEnabled ? System.currentTimeMillis() : 0;
				// FIXME Configuration parameter.
				m_response = get(300, TimeUnit.SECONDS); // wait up to 5 minutes, for queued requests!
				// m_response = await(300, TimeUnit.SECONDS).getResponse(); // wait up to 5 minutes!
				
				if (traceEnabled)
					log.trace("Response in " + (System.currentTimeMillis()-start) + "ms");
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
	 * parameter for the <code>Content-Type</code> header.
	 * 
	 * @return The content encoding and <code>ISO-8851-1</code> if the
	 *         <code>charset</code> parameter was not specified.
	 * @throws IOException
	 * 
	 *             TODO Really, <code>ISO-8851-1</code> is only the default if
	 *             the Content-Type is <code>text/*</code> and otherwise there
	 *             is no default and the response must specify the content
	 *             encoding.
	 */
	public String getContentEncoding() throws IOException {
		ensureResponse();
				
		final HttpFields headers = m_response.getHeaders();
		
		final MiniMime mimeType = new MiniMime(headers.get(HttpHeader.CONTENT_TYPE));
		
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
				r = new InputStreamReader(getInputStream(),contentEncoding);
			} else {
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
