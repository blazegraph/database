package com.bigdata.util.httpd;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.CognitiveWeb.util.CaseInsensitiveStringComparator;
import org.apache.log4j.Logger;

import com.bigdata.rawstore.Bytes;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * A simple, tiny, nicely embeddable HTTP 1.0 server in Java
 * 
 * <p>
 * NanoHTTPD version 1.1, Copyright &copy; 2001,2005-2007 Jarno Elonen
 * (elonen@iki.fi, http://iki.fi/elonen/)
 * <p>
 * Various modifications since supporting integration within bigdata services
 * &copy; 2008, SYSTAP, LLC.
 * <p>
 * <p>
 * <b>Features + limitations: </b>
 * <ul>
 * 
 * <li>Only one Java file</li>
 * <li>Java 1.1 compatible</li>
 * <li>Released as open source, Modified BSD license</li>
 * <li>No fixed config files, logging, authorization etc. (Implement yourself if
 * you need them.)</li>
 * <li>Supports parameter parsing of GET and POST methods</li>
 * <li>Supports both dynamic content and file serving</li>
 * <li>Never caches anything</li>
 * <li>Doesn't limit bandwidth, request time or simultaneous connections</li>
 * <li>Default code serves files and shows all HTTP parameters and headers</li>
 * <li>File server supports directory listing, index.html and index.htm</li>
 * <li>File server does the 301 redirection trick for directories without '/'</li>
 * <li>File server supports simple skipping for files (continue download)</li>
 * <li>File server uses current directory as a web root</li>
 * <li>File server serves also very long files without memory overhead</li>
 * <li>Contains a built-in list of most common mime types</li>
 * <li>All header names are converted lowercase so they don't vary between
 * browsers/clients</li>
 * 
 * </ul>
 * 
 * <p>
 * <b>Ways to use: </b>
 * <ul>
 * 
 * <li>Run as a standalone app, serves files from current directory and shows
 * requests</li>
 * <li>Subclass serve() and embed to your own program</li>
 * <li>Call serveFile() from serve() with your own base directory</li>
 * 
 * </ul>
 * 
 * <h3>License (Modified BSD license)</h3>
 * 
 * <pre>
 * Copyright (C) 2001,2005 by Jarno Elonen <elonen@iki.fi>
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer. Redistributions in
 * binary form must reproduce the above copyright notice, this list of
 * conditions and the following disclaimer in the documentation and/or other
 * materials provided with the distribution. The name of the author may not
 * be used to endorse or promote products derived from this software without
 * specific prior written permission. 
 *  
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * </pre>
 * 
 * @version $Id$
 */
public class NanoHTTPD implements IServiceShutdown
{

    /**
     * Log levels:
     * <ul>
     * <li>INFO is request/response and life cycle events.</li>
     * <li>DEBUG is headers and such</li>
     * <li>TRACE is gruesome detail</li>
     * </ul>
     */
    final static private Logger log = Logger.getLogger(NanoHTTPD.class);

    /** The server socket. */
    private final ServerSocket ss;
    
    /** True once opened and until closed. */
    private volatile boolean open = false;

    /**
     * EOL as used in an HTTPD header. 
     */
    private static final String EOL = "\r\n";

    /**
     * <code>UTF-8</code>
     */
    public static final String UTF8 = "UTF-8";
    
    /*
     * Various method names.
     */
    public static final String GET = "GET";

    public static final String PUT = "PUT";

    public static final String POST = "POST";

    public static final String DELETE = "DELETE";
    
    /*
     * Various error messages.
     */
    
    protected static final String ERR_BAD_REQUEST = "BAD REQUEST: Syntax error. Usage: GET /example/file.html";
    
    /*
     * Various well known headers.
     */
    
    public static final String CONTENT_LENGTH = "Content-Length";

    public static final String CONTENT_TYPE = "Content-Type";

    public static final String DATE = "Date";

    /**
     * The name of the default character set encoding for HTTP which is
     * <code>ISO-8859-1</code>. The character set of an HTTP entity is indicated
     * by the <code>charset</code> parameter on the HTTP
     * <code>Content-Type</code> header. This default MUST be applied when the
     * <code>charset</code> parameter is not specified.
     */
    static public final String httpDefaultCharacterEncoding = "ISO-8859-1";

	// ==================================================
	// API parts
	// ==================================================

    /**
     * Override this to customize the server. (By default, this delegates to
     * serveFile() and allows directory listing.)
     * 
     * @return HTTP response, see class Response for details
     */
    protected Response serve(final Request req) {

        if (log.isDebugEnabled())
            log.debug(req.method + " '" + req.uri + "' ");

        if (log.isDebugEnabled()) {
            {
                for (Map.Entry<String, String> e : req.headers.entrySet()) {
                    log.debug("  HDR: '" + e.getKey() + "' = '" + e.getValue()
                            + "'");
                }
            }
            {
                final Iterator<Map.Entry<String, Vector<String>>> itr = req.params
                        .entrySet().iterator();
                while (itr.hasNext()) {
                    final Map.Entry<String, Vector<String>> e = itr.next();
                    log.debug("  PRM: '" + e.getKey() + "' = '" + e.getValue()
                            + "'");
                }
            }
        }

        return serveFile(req.uri, req.headers, new File("."), true);
        
    }

	/**
	 * HTTP response.
	 * Return one of these from serve().
	 */
	static public class Response
	{
		/**
		 * Default constructor: response = HTTP_OK, data = mime = 'null'
		 */
		public Response()
		{
            this.status = HTTP_OK;
            this.mimeType = null;
            this.data = null;
		}

		/**
		 * Basic constructor.
		 */
        public Response(final String status, final String mimeType,
                final InputStream data)
        {
        	this.status = status;
			this.mimeType = mimeType;
			this.data = data;
		}

		/**
		 * Convenience method that makes an InputStream out of
		 * given text.
		 */
        public Response(final String status, final String mimeType,
                final String txt)
		{
			this.status = status;
			this.mimeType = mimeType;
			this.data = new ByteArrayInputStream(txt == null ? new byte[] {}
                    : txt.getBytes());
        }

        /**
         * Adds given line to the header.
         * 
         * TODO This does not let you specify multiple values for a header.
         */
        public void addHeader(final String name, final String value) {
            
            if (header == null) {
                
                header = new TreeMap<String, String>(
                        new CaseInsensitiveStringComparator());
                
            }
            
            header.put(name, value);
            
        }

		/**
		 * HTTP status code after processing, e.g. "200 OK", HTTP_OK
		 */
		final String status;

		/**
		 * MIME type of content, e.g. "text/html"
		 */
		final String mimeType;

		/**
		 * Data of the response, may be null.
		 */
		final InputStream data;

        /**
         * Headers for the HTTP response. Use addHeader() to add lines. Lazily
         * allocated.
         */
        Map<String,String> header = null;
		
	}

	/**
	 * Some HTTP response status codes
	 */
	public static final String
		HTTP_OK = "200 OK",
		HTTP_REDIRECT = "301 Moved Permanently",
		HTTP_FORBIDDEN = "403 Forbidden",
		HTTP_NOTFOUND = "404 Not Found",
        HTTP_BADREQUEST = "400 Bad Request",
        HTTP_METHOD_NOT_ALLOWED = "405 Method Not Allowed",
		HTTP_INTERNALERROR = "500 Internal Server Error",
		HTTP_NOTIMPLEMENTED = "501 Not Implemented";

	/**
	 * Common mime types for dynamic content
	 */
	public static final String
		MIME_TEXT_PLAIN = "text/plain",
		MIME_TEXT_HTML = "text/html",
		MIME_DEFAULT_BINARY = "application/octet-stream",
        MIME_APPLICATION_XML = "application/xml",
        MIME_TEXT_JAVASCRIPT = "text/javascript",
        /**
         * The traditional encoding of URL query parameters within a POST
         * message body.
         */
        MIME_APPLICATION_URL_ENCODED = "application/x-www-form-urlencoded";

	// ==================================================
	// Socket & server code
	// ==================================================

	/**
     * Starts a HTTP server to given port.
     * <p>
     * Throws an IOException if the socket is already in use
     * 
     * @param port
     *            The port. If <code>0</code> the the server will start on a
     *            random port. The actual port is available from #getPort().
     */
    public NanoHTTPD(int port) throws IOException {

        if (port != 0) {

            /*
             * Use the specified port.
             */
            myTcpPort = port;

            ss = new ServerSocket(myTcpPort);

        } else {

            /*
             * Use any open port.
             */
            ss = new ServerSocket(0);

            myTcpPort = ss.getLocalPort();
            
        }
        
        if (log.isInfoEnabled())
            log.info("Running on port=" + myTcpPort);

        /*
         * FIXME parameter and configuration of same and per-instance buffers.
         */
        final int requestServicePoolSize = 0;

        if (requestServicePoolSize == 0) {

            requestService = (ThreadPoolExecutor) Executors
                    .newCachedThreadPool(new DaemonThreadFactory
                            (getClass().getName()+".requestService"));

        } else {

            requestService = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                    requestServicePoolSize, new DaemonThreadFactory
                    (getClass().getName()+".requestService"));

        }
        
        /*
         * Begin accepting connections.
         */

        open = true;

        acceptService.submit(new Runnable() {
            
            public void run() {
            
                try {
                
                    while (open) {
            
                        /*
                         * Hand off request to a pool of worker threads.
                         */
                        
                        requestService.submit(new HTTPSession(ss.accept()));
                        
                    }

                } catch (IOException ioe) {
                    
                    if (!open) {

                        if(log.isInfoEnabled())
                            log.info("closed.");

                        return;

                    }

                    log.error(ioe, ioe);
                    
                }
                
            }
            
        });

    }

    /**
     * Runs a single thread which accepts connections.
     */
    private final ExecutorService acceptService = Executors
            .newSingleThreadExecutor(new DaemonThreadFactory(getClass()
                    .getName()
                    + ".acceptService"));

    /**
     * Runs a pool of threads for handling requests.
     */
    private final ExecutorService requestService;

    public boolean isOpen() {
        return open;
    }

    synchronized public void shutdown() {
    
        if(!open)
            return;
        
        if (log.isInfoEnabled())
            log.info("");
        
        // time when shutdown begins.
        final long begin = System.currentTimeMillis();

//        /*
//         * Note: when the timeout is zero we approximate "forever" using
//         * Long.MAX_VALUE.
//         */

        final long shutdownTimeout = 1000;
        
//        final long shutdownTimeout = this.shutdownTimeout == 0L ? Long.MAX_VALUE
//                : this.shutdownTimeout;

        final TimeUnit unit = TimeUnit.MILLISECONDS;

        /*
         * Note: Use abrupt shutdown for the acceptService. It will be blocked
         * waiting on a socket and therefore will not notice if we set [open :=
         * false].
         */
        open = false; // Note: Runnable will terminate when open == false.
        acceptService.shutdownNow(); // Abrupt shutdown of acceptService.
        
        requestService.shutdown();

        try {

            if (log.isInfoEnabled())
                log.info("Awaiting request service termination");
            
            final long elapsed = System.currentTimeMillis() - begin;
            
            if (!requestService.awaitTermination(shutdownTimeout - elapsed, unit)) {
                
                log.warn("Request service termination: timeout");
                
            }

        } catch(InterruptedException ex) {
            
            log.warn("Interrupted awaiting request service termination.", ex);
            
        }
        
        try {
            
            ss.close();
            
        } catch (IOException e) {
            
            log.warn(e, e);
            
        }
        
    }

    synchronized public void shutdownNow() {

        if(!open)
            return;

        if (log.isInfoEnabled())
            log.info("");

        // Note: Runnable will terminate when open == false.
        open = false;

        acceptService.shutdownNow();

        requestService.shutdownNow();
        
        try {
            
            ss.close();
            
        } catch (IOException e) {
            
            log.warn(e, e);
            
        }
        
    }

    /**
	 * Starts as a standalone file server and waits for Enter.
	 */
	public static void main( final String[] args )
	{
		System.out.println( "NanoHTTPD 1.1 (C) 2001,2005-2007 Jarno Elonen\n" +
							"(Command line options: [port])\n" );

		// Show licence if requested
//		final int lopt = -1;
//		for ( int i=0; i<args.length; ++i )
//		if ( args[i].toLowerCase().endsWith( "licence" ))
//		{
//			lopt = i;
//			System.out.println( LICENCE + "\n" );
//		}

		// Change port if requested
		int port = 80;
		if ( args.length > 0 ) //&& lopt != 0 )
			port = Integer.parseInt( args[0] );

//		if ( args.length > 1 &&
//			 args[1].toLowerCase().endsWith( "licence" ))
//				System.out.println( LICENCE + "\n" );

//		NanoHTTPD nh = null;
		try
		{
            /* nh = */new NanoHTTPD(port);
        }
		catch( IOException ioe )
		{
			System.err.println( "Couldn't start server:\n" + ioe );
			System.exit( -1 );
		}
//		nh.myFileDir = new File("");

		System.out.println( "Now serving files in port " + port + " from \"" +
							new File("").getAbsolutePath() + "\"" );

		System.out.println( "Hit Enter to stop.\n" );

		try { System.in.read(); } catch( Throwable t ) {};
	}

    /**
     * Handles one session, i.e. parses the HTTP request and returns the
     * response.
     * 
     * @see http://www.w3.org/Protocols/rfc2616/rfc2616.html
     */
	private class HTTPSession implements Runnable
	{

        final private Socket mySocket;
        
		public HTTPSession(final Socket s) {

            mySocket = s;

        }

        public void run() {

            if (log.isInfoEnabled())
                log.info("Handling request: localPort="
                        + mySocket.getLocalPort());

            InputStream is = null;
            try {
                is = mySocket.getInputStream();
                if (is == null)
                    return; // Should never happen...

                /*
                 * Parse the request.
                 * 
                 * FIXME Buffer reuse and sizing.
                 */
                final int bufSize = Bytes.kilobyte32 * 1;
                final Request req = parseRequest(new BufferedInputStream(is,
                        bufSize));

                // Ok, now do the serve()
                try {
                    final Response r = serve(req);
                    if (r == null)
                        sendError(HTTP_INTERNALERROR,
                                "SERVER INTERNAL ERROR: Serve() returned a null response.");
                    else
                        sendResponse(r.status, r.mimeType, r.header, r.data);
                } catch (Exception ex) {
                    log.warn(ex.getMessage(), ex);
                    sendError(HTTP_INTERNALERROR, ex.getMessage());
                    return;
                }

            } catch (InterruptedException ie) {
                // Thrown by sendError, ignore and exit the thread.
            } catch (Throwable t) {
                try {
                    log.error(t.getMessage(), t);
                    sendError(HTTP_INTERNALERROR, "SERVER INTERNAL ERROR: "
                            + t.getMessage());
                } catch (Throwable t2) {
                    // ignore.
                }
            } finally {
                if (is != null) {
                    try {
                        is.close();
                    } catch (IOException ex) {/* ignore */
                    }
                }
            }

        }

        /**
         * Parse the request, delegate the formulation of the response, and
         * finally send the response to the client.
         * 
         * @param bis
         *            The input stream from which the request may be read.
         * 
         * @return The request.
         * 
         * @throws InterruptedException
         * @throws IOException
         */
        private Request parseRequest(final BufferedInputStream bis)
                throws InterruptedException, IOException {

            final String method;
            final String uri;
            
            // Note: The Map comparator folds case for header names!
            final Map<String, String> headers = new TreeMap<String, String>(
                    new CaseInsensitiveStringComparator());

            final LinkedHashMap<String, Vector<String>> params = new LinkedHashMap<String, Vector<String>>();

            /*
             * Tokenize the request line.
             * 
             * Request-Line = Method SP Request-URI SP HTTP-Version CRLF
             * 
             * HTTP-Version   = "HTTP" "/" 1*DIGIT "." 1*DIGIT
             */
            {

                // Read the request line
                final String requestLine = readLine(bis);

                if (requestLine == null)
                    sendError(HTTP_BADREQUEST, ERR_BAD_REQUEST);

                final StringTokenizer st = new StringTokenizer(requestLine);

                if (!st.hasMoreTokens())
                    sendError(HTTP_BADREQUEST, ERR_BAD_REQUEST);

                method = st.nextToken();

                if (!st.hasMoreTokens()) {
                    // Missing URI
                    sendError(HTTP_BADREQUEST, ERR_BAD_REQUEST);
                }

                /*
                 * TODO Save off the full requestURI for the Request object vs
                 * provide for reconstruction?
                 */
                
                // uri = decodePercent( st.nextToken());

                final String requestURI = st.nextToken();
                
                String uriString = requestURI;

                /*
                 * Decode parameters from the URI (LinkedHashMap preserves their
                 * ordering). This gives us just the "file" as a side-effect.
                 */
                final int qmi = uriString.indexOf('?');

                if (qmi != -1) {

                    decodeParams(uriString.substring(qmi + 1), params);

                    uriString = decodePercent(uriString.substring(0, qmi));

                } else {

                    uriString = decodePercent(uriString);

                }

                uri = uriString;

                /*
                 * The protocol version. 
                 */
                
                final String version = st.nextToken();

                if (log.isDebugEnabled()) {
                    log.debug("method=" + method + ", requestURI=["
                            + requestURI + "], version=" + version);
                }

                /*
                 * The headers will follow starting with the next line.
                 */
                
            }

            /*
             * Parse headers.
             * 
             * Note: The headers terminate with a line consisting of a bare
             * (CRLF). Since readLine() strips the trailing CRLF, this loop will
             * terminate when it observes that bare CRLF sequence.
             */
            {
                String line = readLine(bis);
                while (line != null && line.length() > 0) {
                    final int p = line.indexOf(':');
                    final String name = line.substring(0, p).trim();
                    final String value = line.substring(p + 1).trim();
                    headers.put(name, value);
                    if (log.isDebugEnabled())
                        log.debug("name=[" + name + "], value=[" + value + "]");
                    line = readLine(bis);
                }
            }

            final String contentType = headers.get(CONTENT_TYPE);

            if (MIME_APPLICATION_URL_ENCODED.equals(contentType)) {
                /*
                 * Decode url encoded parameters in the request body.
                 */
                long size = 0x7FFFFFFFFFFFFFFFl;
                final String contentLength = headers.get(CONTENT_LENGTH);
                if (contentLength != null) {
                    try {
                        size = Integer.parseInt(contentLength);
                    } catch (NumberFormatException ex) {
                    }
                }
                if (size > 0) {
                    // Read until the end of the input stream.
                    String s;
                    while ((s = readLine(bis)) != null) {
                        decodeParams(s, params);
                    }
                }
            } else {
                /*
                 * Otherwise the service is responsible for reading the request
                 * body directly from the input stream and the input stream is
                 * currently positioned on the request body (if any).
                 */
            }

            return new Request(uri, method, headers, params, bis);

		}

        /**
         * Reads and returns everything up to the next CRLF sequence. The CRLF
         * sequence is consumed, but not returned. Bytes are converted to
         * characters
         * 
         * @param is
         *            The input stream.
         * 
         * @return The data read, converted to a {@link String} -or-
         *         <code>null</code> iff the end of the input was reached 
         *         while the buffer was empty.
         */
        private String readLine(final InputStream is) throws IOException {

            _baos.reset(); // reset the buffer - it is reused for each line.
            int lastChar = -1;
            int thisChar; 
            int nread = 0; // #of bytes read.
            while ((thisChar = is.read()) != -1) {

                nread++;

                _baos.write(thisChar);
                
                if (lastChar == '\r' && thisChar == '\n') {

                    nread -= 2;

                    /*
                     * Convert everything up to the CRLF into a String.
                     * 
                     * TODO This should explicitly use the appropriate encoding
                     * for HTTP headers and the HTTP request line. What is that?
                     */
                    final String s = new String(_baos.toByteArray(),
                            0/* off */, nread /* len */, UTF8);

                    if (log.isTraceEnabled())
                        log.trace("[" + s + "]");
                    
                    return s;

                }

                lastChar = thisChar;

            }

            if (thisChar == -1 && nread == 0) {
                // Nothing available.
                if (log.isTraceEnabled())
                    log.trace("EOF");
                return null;
            }

            /*
             * The end of the stream was encountered after we had already read
             * some bytes and before we encountered the CRLF sequence. This is
             * an error since the protocol should always terminate the "lines"
             * we are reading with a CRLF sequence.
             */
            throw new IOException("End of input stream?");

		}
        // buffer reused by readLine() across calls.
        private final ByteArrayOutputStream _baos = new ByteArrayOutputStream(256);

		/**
         * Returns an error message as a HTTP response and throws
         * {@link InterruptedException} to stop further request processing.
         */
        private void sendError(final String status, final String msg)
                throws InterruptedException {
			
            sendResponse(status, MIME_TEXT_PLAIN, null,
                    new ByteArrayInputStream(msg.getBytes()));
			
            throw new InterruptedException();
            
		}

		/**
		 * Sends given response to the socket.
		 */
        private void sendResponse(final String status, final String mime,
                final Map<String,String> header, final InputStream data)
        {
			try
			{
				if ( status == null )
					throw new Error( "sendResponse(): Status can't be null." );

                if (log.isInfoEnabled()) { // optionally log the status and content type.
                    log.info("status: [HTTP/1.0 " + status
                            + "]"//
                            + (mime == null ? "" : "[Content-Type: " + mime
                                    + "]")//
                    );
                }

				final OutputStream out = mySocket.getOutputStream();
				final PrintWriter pw = new PrintWriter( out );
                pw.print("HTTP/1.0 ");
                pw.print(status);
                pw.print(" ");
                pw.print(EOL);

                if (mime != null) {
                    pw.print(CONTENT_TYPE);
                    pw.print(": ");
                    pw.print(mime);
                    pw.print(EOL);
                }

                if (header == null || header.get(DATE) == null) {
                    pw.print(DATE);
                    pw.print(": ");
                    pw.print(gmtFrmt.format(new Date()));
                    pw.print(EOL);
                }

                if (header != null) {
                    for (Map.Entry<String, String> e : header.entrySet()) {
                        final String key = (String) e.getKey();
                        final String value = e.getValue();
                        pw.print(key);
                        pw.print(": ");
                        pw.print(value);
                        pw.print(EOL);
                    }
                }

				pw.print(EOL);
				pw.flush();

                if (data != null) {
//				    if(log.isDebugEnabled())
//				        log.debug("Sending data.");
				    // FIXME reuse buffer and size to fit socket output stream.
                    final byte[] buff = new byte[2048];
                    while (true) {
                        final int read = data.read(buff, 0, buff.length);
                        if (read <= 0)
                            break;
//                        if (log.isTraceEnabled())
//                            log.trace("Sending " + read + " bytes.");
                        out.write(buff, 0, read);
					}
				}
				out.flush();
				out.close();
//                if(log.isTraceEnabled())
//                    log.trace("Response done.");
			}
			catch( IOException ioe )
			{
				// Couldn't write? No can do.
				try { mySocket.close(); } catch( Throwable t ) {}
                log.error(ioe, ioe);
			} finally {
                if ( data != null ) {
                    /*
                     * Close input stream. Producer should notice and abort if
                     * running.
                     */
                    try {data.close();} catch(Throwable t) {}
                }
			}
		}
	};

	/**
	 * A http request. 
	 */
    public static class Request {

        /**
         * Percent-decoded URI without parameters, for example "/index.cgi"
         */
        final public String uri;

        /** "GET", "POST" etc. */
        final public String method;

        /**
         * Header entries, percent decoded and forced to <strong>lower
         * case</strong>.
         */
        final public Map<String, String> headers;

        /**
         * Parsed, percent decoded parameters from URI and, in case of a POST
         * request body using {@value NanoHTTPD#MIME_APPLICATION_URL_ENCODED},
         * data. The keys are the parameter names. Each value is an ordered
         * collection of {@link String}s containing the bindings for the named
         * parameter. The order of the URL parameters is preserved by the
         * {@link LinkedHashMap}. The order of the bindings for each parameter
         * is preserved by the {@link Vector}.
         */
        final public LinkedHashMap<String, Vector<String>> params;

        /**
         * The input stream. A {@value NanoHTTPD#MIME_APPLICATION_URL_ENCODED}
         * request body will have already been decoded into {@link #params}.
         * Otherwise, this argument may be used to read the request body. The
         * input stream will be closed regardless by the caller.
         */
        final private InputStream is;

        /**
         * The input stream. A {@value NanoHTTPD#MIME_APPLICATION_URL_ENCODED}
         * request body will have already been decoded into {@link #params}.
         * Otherwise, this argument may be used to read the request body. The
         * input stream will be closed regardless by the caller.
         */
        public InputStream getInputStream() {
            
            return is;
            
        }
        
        /**
         * @param uri
         *            Percent-decoded URI without parameters, for example
         *            "/index.cgi"
         * @param method
         *            "GET", "POST" etc.
         * @param params
         *            Parsed, percent decoded parameters from URI and, in case
         *            of a POST request body using
         *            {@value NanoHTTPD#MIME_APPLICATION_URL_ENCODED}, data. The
         *            keys are the parameter names. Each value is an ordered
         *            collection of {@link String}s containing the bindings for
         *            the named parameter. The order of the URL parameters is
         *            preserved by the {@link LinkedHashMap}. The order of the
         *            bindings for each parameter is preserved by the
         *            {@link Vector}.
         * @param headers
         *            Header entries, percent decoded and forced to
         *            <strong>lower case</strong>.
         * @param is
         *            The input stream. A
         *            {@value NanoHTTPD#MIME_APPLICATION_URL_ENCODED} request
         *            body will have already been decoded into <i>params</i>.
         *            Otherwise, this argument may be used to read the request
         *            body. The input stream will be closed regardless by the
         *            caller.
         */
        private Request(final String uri, final String method,
                final Map<String,String> headers,
                final LinkedHashMap<String, Vector<String>> params,
                final InputStream is) {
            this.uri = uri;
            this.method = method;
            this.headers = headers;
            this.params = params;
            this.is = is;
        }

        /**
         * Return the length of the request body if known and <code>-1</code>
         * otherwise.
         */
        public int getContentLength() {

            final String s = headers.get(CONTENT_LENGTH);
            
            if(s == null)
                return -1;
            
            return Long.valueOf(s).intValue();
            
        }

        /**
         * Return the <code>Content-Type</code> header -or- <code>null</code> if
         * that header was not present in the request.
         */
        public String getContentType() {

            return headers.get(CONTENT_TYPE);
            
        }

        /**
         * Return the request body.
         * 
         * @return
         * @throws IOException
         *             a variety of reasons, including if it has already been
         *             read.
         */
        public byte[] getBinaryContent() throws IOException {

            final int contentLength = getContentLength();

            if (log.isDebugEnabled())
                log.debug("contentLength=" + contentLength
                        + " : reading request body...");

            final ByteArrayOutputStream baos = contentLength == -1 ? new ByteArrayOutputStream()
                    : new ByteArrayOutputStream(contentLength);

            int ch;
            int len = 0;
            while ((ch = is.read()) != -1) {
                if (log.isTraceEnabled())
                    log.trace("Read ch=" + ch + ", len=" + len
                            + ", contentLength=" + contentLength);
                baos.write(ch);
                len++;
                if (contentLength != -1 && len == contentLength)
                    break;
            }

            return baos.toByteArray();

        }
        
        public String getStringContent() throws IOException {

            final String s = headers.get(CONTENT_TYPE);

            final MIMEType mt = new MIMEType(s);

            final byte[] b = getBinaryContent();

            String charset = mt.getParamValue("charset");

            if (charset == null)
                charset = httpDefaultCharacterEncoding;

            return new String(b, charset);

        }
        
	} // class Request
	
	/**
	 * URL-encodes everything between "/"-characters.
	 * Encodes spaces as '%20' instead of '+'.
	 */
	static private String encodeUri( final String uri )
	{
        final StringTokenizer st = new StringTokenizer(uri, "/ ", true);
        final StringBuilder newUri = new StringBuilder("");
		while ( st.hasMoreTokens())
		{
			final String tok = st.nextToken();
			if ( tok.equals( "/" ))
				newUri.append("/");
			else if ( tok.equals( " " ))
				newUri.append("%20");
			else
			{
//				newUri += URLEncoder.encode( tok );
				// For Java 1.4 you'll want to use this instead:
				 try {
                    newUri.append(URLEncoder.encode(tok, UTF8));
                } catch (UnsupportedEncodingException uee) {
                    throw new RuntimeException(uee);
                }
			}
		}
		return newUri.toString();
	}

//	private File myFileDir;

    /**
     * The port on which the service was started.
     */
    public int getPort() {
        
        return myTcpPort;
        
    }
    final private int myTcpPort;
    
	// ==================================================
	// File server code
	// ==================================================

	/**
	 * Serves file from homeDir and its' subdirectories (only).
	 * Uses only URI, ignores all headers and HTTP parameters.
	 * 
	 * TODO This uses += for string append which is not efficient.
	 */
    protected Response serveFile(String uri, final Map<String,String> header,
            final File homeDir, final boolean allowDirectoryListing) {
        
		// Make sure we won't die of an exception later
		if ( !homeDir.isDirectory())
			return new Response( HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
								 "INTERNAL ERRROR: serveFile(): given homeDir is not a directory." );

		// Remove URL arguments
		uri = uri.trim().replace( File.separatorChar, '/' );
		if ( uri.indexOf( '?' ) >= 0 )
			uri = uri.substring(0, uri.indexOf( '?' ));

		// Prohibit getting out of current directory
		if ( uri.startsWith( ".." ) || uri.endsWith( ".." ) || uri.indexOf( "../" ) >= 0 )
			return new Response( HTTP_FORBIDDEN, MIME_TEXT_PLAIN,
								 "FORBIDDEN: Won't serve ../ for security reasons." );

		File f = new File( homeDir, uri );
		if ( !f.exists())
			return new Response( HTTP_NOTFOUND, MIME_TEXT_PLAIN,
								 "Error 404, file not found." );

		// List the directory, if necessary
		if ( f.isDirectory())
		{
			// Browsers get confused without '/' after the
			// directory, send a redirect.
			if ( !uri.endsWith( "/" ))
			{
				uri += "/";
				Response r = new Response( HTTP_REDIRECT, MIME_TEXT_HTML,
										   "<html><body>Redirected: <a href=\"" + uri + "\">" +
										   uri + "</a></body></html>");
				r.addHeader( "Location", uri );
				return r;
			}

			// First try index.html and index.htm
			if ( new File( f, "index.html" ).exists())
				f = new File( homeDir, uri + "/index.html" );
			else if ( new File( f, "index.htm" ).exists())
				f = new File( homeDir, uri + "/index.htm" );

			// No index file, list the directory
			else if ( allowDirectoryListing )
			{
				final String[] files = f.list();
				String msg = "<html><body><h1>Directory " + uri + "</h1><br/>";

				if ( uri.length() > 1 )
				{
					final String u = uri.substring( 0, uri.length()-1 );
					final int slash = u.lastIndexOf( '/' );
					if ( slash >= 0 && slash  < u.length())
						msg += "<b><a href=\"" + uri.substring(0, slash+1) + "\">..</a></b><br/>";
				}

				for ( int i=0; i<files.length; ++i )
				{
				    final File curFile = new File( f, files[i] );
					final boolean dir = curFile.isDirectory();
					if ( dir )
					{
						msg += "<b>";
						files[i] += "/";
					}

					msg += "<a href=\"" + encodeUri( uri + files[i] ) + "\">" +
						   files[i] + "</a>";

					// Show file size
					if ( curFile.isFile())
					{
						long len = curFile.length();
						msg += " &nbsp;<font size=2>(";
						if ( len < 1024 )
							msg += curFile.length() + " bytes";
						else if ( len < 1024 * 1024 )
							msg += curFile.length()/1024 + "." + (curFile.length()%1024/10%100) + " KB";
						else
							msg += curFile.length()/(1024*1024) + "." + curFile.length()%(1024*1024)/10%100 + " MB";

						msg += ")</font>";
					}
					msg += "<br/>";
					if ( dir ) msg += "</b>";
				}
				return new Response( HTTP_OK, MIME_TEXT_HTML, msg );
			}
			else
			{
				return new Response( HTTP_FORBIDDEN, MIME_TEXT_PLAIN,
								 "FORBIDDEN: No directory listing." );
			}
		}

		try
		{
			// Get MIME type from file name extension, if possible
			String mime = null;
			final int dot = f.getCanonicalPath().lastIndexOf( '.' );
			if ( dot >= 0 )
                mime = (String) theMimeTypes.get(f.getCanonicalPath()
                        .substring(dot + 1).toLowerCase());
            if (mime == null )
				mime = MIME_DEFAULT_BINARY;

			// Support (simple) skipping:
			long startFrom = 0;
			String range = header.get( "Range" );
			if ( range != null )
			{
				if ( range.startsWith( "bytes=" ))
				{
					range = range.substring( "bytes=".length());
					int minus = range.indexOf( '-' );
					if ( minus > 0 )
						range = range.substring( 0, minus );
					try	{
						startFrom = Long.parseLong( range );
					}
					catch ( NumberFormatException nfe ) {}
				}
			}

			final FileInputStream fis = new FileInputStream( f );
			fis.skip( startFrom );
			final Response r = new Response( HTTP_OK, mime, fis );
			r.addHeader( "Content-length", "" + (f.length() - startFrom));
			r.addHeader( "Content-range", "" + startFrom + "-" +
						(f.length()-1) + "/" + f.length());
			return r;
		}
		catch( IOException ioe )
		{
			return new Response( HTTP_FORBIDDEN, MIME_TEXT_PLAIN, "FORBIDDEN: Reading file failed." );
		}
	}

    /**
     * Decodes parameters in percent-encoded URI-format ( e.g.
     * "name=Jack%20Daniels&pass=Single%20Malt" ) and adds them to a {@link Map}
     * .
     * 
     * @param parms
     *            The URL query parameters.
     * @param p
     *            A map of the parsed, percent decoded parameters (required).
     *            The keys are the parameter names. Each value is a
     *            {@link Vector} of {@link String}s containing the bindings for
     *            the named parameter. The order of the URL parameters is
     *            preserved by the insertion order of the {@link LinkedHashMap}
     *            and the elements of the {@link Vector} values.
     * 
     * @return The caller's map.
     * 
     * @throws UnsupportedEncodingException
     */
    static public LinkedHashMap<String, Vector<String>> decodeParams(
            final String parms, final LinkedHashMap<String, Vector<String>> p)
            throws UnsupportedEncodingException {

        if (parms == null)
            throw new IllegalArgumentException();
        
        if (p == null)
            throw new IllegalArgumentException();

        final StringTokenizer st = new StringTokenizer(parms, "&");
        while (st.hasMoreTokens()) {
            final String e = st.nextToken();
            final int sep = e.indexOf('=');
            final String name, val;
            if (sep != -1) {
                name = decodePercent(e.substring(0, sep)).trim();
                val = decodePercent(e.substring(sep + 1));
            } else {
                name = decodePercent(e).trim();
                val = null;
            }
            if (log.isDebugEnabled())
                log.debug(name + ": " + val);
            Vector<String> vals = p.get(name);
            if (vals == null) {
                vals = new Vector<String>();
                p.put(name, vals);
            }
            if (val != null)
                vals.add(val);
        }

        return p;
        
    }

    /**
     * Decodes the percent encoding scheme. <br/>
     * For example: "an+example%20string" -> "an example string"
     * 
     * @throws UnsupportedEncodingException
     */
    static private String decodePercent(final String str)
            throws UnsupportedEncodingException {

        return URLDecoder.decode(str, UTF8);
        
    }

    /**
     * Construct a percent encoded representation of the URL query parameters.
     * 
     * @param expected
     *            The parameters.
     * 
     * @return The encoded representation.
     * 
     * @throws UnsupportedEncodingException
     */
    static public StringBuilder encodeParams(
            final LinkedHashMap<String, Vector<String>> expected)
            throws UnsupportedEncodingException {

        final StringBuilder sb = new StringBuilder();

        boolean first = true;
        for (Map.Entry<String, Vector<String>> e : expected.entrySet()) {
            final String k = e.getKey();
            final Vector<String> vec = e.getValue();
            for (String v : vec) {
                if (first) {
                    first = false;
                } else {
                    sb.append("&");
                }
                sb.append(URLEncoder.encode(k, UTF8));
                sb.append("=");
                sb.append(URLEncoder.encode(v, UTF8));
            }
        }

        return sb;

    }

    /**
	 * Hashtable mapping (String)FILENAME_EXTENSION -> (String)MIME_TYPE
	 */
	private static Hashtable<String,String> theMimeTypes = new Hashtable<String, String>();
	static
	{
		StringTokenizer st = new StringTokenizer(
			"htm		text/html "+
			"html		text/html "+
			"txt		text/plain "+
			"asc		text/plain "+
			"gif		image/gif "+
			"jpg		image/jpeg "+
			"jpeg		image/jpeg "+
			"png		image/png "+
			"mp3		audio/mpeg "+
			"m3u		audio/mpeg-url " +
			"pdf		application/pdf "+
			"doc		application/msword "+
			"ogg		application/x-ogg "+
			"zip		application/octet-stream "+
			"exe		application/octet-stream "+
			"class		application/octet-stream " );
		while ( st.hasMoreTokens())
			theMimeTypes.put( st.nextToken(), st.nextToken());
	}

	/**
	 * GMT date formatter
	 */
    private static java.text.SimpleDateFormat gmtFrmt;
	static
	{
		gmtFrmt = new java.text.SimpleDateFormat( "E, d MMM yyyy HH:mm:ss 'GMT'", Locale.US);
		gmtFrmt.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

    /*
     * Note: This is the original distribution license text. I've taken it out
     * of the code and moved it into a comment block on the class javadoc in
     * order to reduce the size of the compiled class file.
     */
//	/**
//	 * The distribution license
//	 */
//	private static final String LICENCE =
//		"Copyright (C) 2001,2005 by Jarno Elonen <elonen@iki.fi>\n"+
//		"\n"+
//		"Redistribution and use in source and binary forms, with or without\n"+
//		"modification, are permitted provided that the following conditions\n"+
//		"are met:\n"+
//		"\n"+
//		"Redistributions of source code must retain the above copyright notice,\n"+
//		"this list of conditions and the following disclaimer. Redistributions in\n"+
//		"binary form must reproduce the above copyright notice, this list of\n"+
//		"conditions and the following disclaimer in the documentation and/or other\n"+
//		"materials provided with the distribution. The name of the author may not\n"+
//		"be used to endorse or promote products derived from this software without\n"+
//		"specific prior written permission. \n"+
//		" \n"+
//		"THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR\n"+
//		"IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES\n"+
//		"OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.\n"+
//		"IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,\n"+
//		"INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT\n"+
//		"NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"+
//		"DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n"+
//		"THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"+
//		"(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE\n"+
//		"OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.";

}
