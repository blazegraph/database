/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2013.  All rights reserved.

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
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServlet;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;

/**
 * This class supports making requests to the server with fairly low level control.
 * Each operation is set up by calls to the protected methods such as
 * {@link #setMethodisPost(String, String)}, {@link #setAllow400s()},
 * and then to call {@link #serviceRequest(String...)} to actually
 * process the request.
 * This process may be repeated multiple times.
 * After each call to {@link #serviceRequest(String...)}
 * the options are reset to the defaults.
 * @author jeremycarroll
 *
 */
public abstract class AbstractProtocolTest  extends AbstractTestNanoSparqlClient<IIndexManager> {

	protected interface RequestFactory {
		HttpUriRequest createRequest(String ... params);
	};

	protected static final String SELECT = "SELECT (1 as ?one){}";
	protected static final String ASK = "ASK WHERE {}";
	protected static final String CONSTRUCT = "CONSTRUCT { <a:b> <c:d> <e:f> } WHERE {}";
	protected static final long PAUSE_BEFORE_CLOSE_TIME = 100;
	private static int updateCounter = 0;
	private static String update() {
		return "INSERT { <http://example.org/a> <http://example.org/a> <http://example.org/" + updateCounter++ + "> } WHERE {}";
	}

	private static String askIfUpdated() {
		return "ASK { <http://example.org/a> <http://example.org/a> <http://example.org/" + updateCounter + "> }";
	}

	/**
	 * A SPARQL ASK Query that returns true iff {@Link #update} has successfully run
	 */
	private final String askIfUpdated = askIfUpdated();
	/**
	 * A SPARQL Update that adds a triple
	 */
	final String update       = update();

	HttpServlet  servlet;
	HttpClient client;
	private String responseContentType = null;
	private String accept = null;
	private boolean permit400s = false;
	private Header[] headers = null;

    private final String getSparqlURL(final String serviceURL) {
        return serviceURL + "/sparql";
    }
	
	private final RequestFactory GET = new RequestFactory(){
		@Override
		public HttpUriRequest createRequest(String... params) {
			final StringBuffer url = new StringBuffer();
			url.append(getSparqlURL(m_serviceURL));
			char sep = '?';
			for (int i=0;i<params.length;i+=2) {
				url.append(sep);
				url.append(params[i]);
				url.append('=');
				try {
					url.append(URLEncoder.encode(params[i+1], "UTF-8"));
				} catch (final UnsupportedEncodingException e) {
					// JVM must support UTF-8
					throw new Error(e);
				}
				sep='&';
			}
			return new HttpGet(url.toString());
		}
	};

	private volatile RequestFactory requestFactory = GET;
	
	protected RequestFactory getRequestFactory() {
		return requestFactory;
	}
	@Override
	public void setUp() throws Exception {
		super.setUp();
		client = new DefaultHttpClient(newInstance());
		resetDefaultOptions();
	}
	@Override
	public void tearDown() throws Exception {
		client.getConnectionManager().shutdown();
		client = null;
		servlet = null;
		super.tearDown();
	}
	/**
	 * This method is called automatically after each call to {@link #serviceRequest(String...)}
	 * so probably is unnecessary.
	 */
	protected void resetDefaultOptions() {
		accept = null;
		requestFactory = GET;
		accept = null;
		permit400s = false;
		headers = null;
	}

	/**
	 * 
	 * @return The content type of the last response, null if none (e.g. a 204?)
	 */
	protected String getResponseContentType() {
		return responseContentType;
	}

	/**
	 * Sets the accept header, default is "*"
	 * @param mimetype
	 */
	protected void setAccept(String mimetype) {
		accept = mimetype;
	}
	
	/**
	 * 
	 * @param mimetype
	 */
	protected void setHeaders(Header[] headers) {
		this.headers = headers;
	}

	static private Pattern charset = Pattern.compile("[; ]charset *= *\"?([^ ;\"]*)([ \";]|$)");

	/**
	 * Sanity check the {@link #charset} pattern
	 * @param argv
	 */
	public static void main(String argv[]) {
		for (final String t:new String[]{
				"text/html ; charset=iso-8856-1",
				"text/html ; charset=iso-8856-1; foo = bar",
				"text/html ;charset=iso-8856-1; foo = bar",
				"text/html ; charset= \"iso-8856-1\"",
				"text/html ; charset=iso-8856-1; foo = bar",
				"text/html ; charset = iso-8856-1; foo = bar",
				"text/html ; foo = bar",
				"text/html",

		}) {
			final Matcher m = charset.matcher(t);
			System.err.println(t+ " ====> "+(m.find()?m.group(1):""));
		}
	}

    protected ClientConnectionManager newInstance() {

        final ThreadSafeClientConnManager cm = new ThreadSafeClientConnManager(
                newSchemeRegistry());

        // Increase max total connection to 200
        cm.setMaxTotal(200);

        // Increase default max connection per route to 20
        cm.setDefaultMaxPerRoute(20);

        // Increase max connections for localhost to 50
        final HttpHost localhost = new HttpHost("locahost");

        cm.setMaxForRoute(new HttpRoute(localhost), 50);

        return cm;

    }


    protected SchemeRegistry newSchemeRegistry() {
        
        final SchemeRegistry schemeRegistry = new SchemeRegistry();

        schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory
                .getSocketFactory()));
        
        schemeRegistry.register(new Scheme("https", 443, SSLSocketFactory
                .getSocketFactory()));

        return schemeRegistry;

    }
	/**
	 * This is the main entry point for subclasses.
	 * This method sends a request to the server, as set up
	 * by setABC methods, and returns the string send back to the client.
	 * @param paramValues This is an even number of param [=] value pairs. Multiple values for the same param are supported.
	 *    These are passed to the server either as URL query params, or as URL encoded values in the body if the method
	 *    {@link #setMethodisPostUrlEncodedData()} has been called.
	 * @return the data returned by the server.
	 * @throws IOException
	 */
	protected String serviceRequest(final String ... paramValues) throws IOException {
		HttpUriRequest req;
		responseContentType = null;
		try {
			try {
				req = requestFactory.createRequest(paramValues);
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
			req.setHeader("Accept", accept==null?"*":accept);
			
			if(headers != null) {
				req.setHeaders(headers);
			}
			
			final HttpResponse resp = client.execute(req);
			String page="";
			final HttpEntity entity = resp.getEntity();
			if (entity != null ) {
				String encoding = "utf-8";
				assertNotNull("Entity in " + resp.getStatusLine().getStatusCode()+" response must specify content type",entity.getContentType());
				final Matcher m = charset.matcher(entity.getContentType().getValue());
				if (m.find()) {
					encoding = m.group(1);
				}
				page = QueryServlet.readFully(new InputStreamReader(entity.getContent(),encoding));
				responseContentType = entity.getContentType().getValue();
			}
			if ( resp.getStatusLine().getStatusCode()>=(permit400s?500:400) ) {
				fail(resp.getStatusLine().toString()+"\n"+ page);
			}
			return page;
		}
		finally {
			resetDefaultOptions();
		}
	}

	private Map<String,String[]> pairs2map(String... paramValues) {
		final Map<String,String[]> params = new HashMap<String,String[]>();
		for (int i=0;i<paramValues.length;i+=2) {
			final String key = paramValues[i];
			final String value = paramValues[i+1];
			final String[] val = params.get(key);
			if (val==null) {
				params.put(key, new String[]{value});
			} else {
				// horridly inefficient, never called?
				final String nval[] = new String[val.length+1];
				System.arraycopy(val, 0, nval, 0, val.length);
				nval[val.length] = value;
				params.put(key, nval);
			}
		}
		return params;
	}

	/**
	 * The method is a POST usng url-encoded form data, with the parameters being those past
       to {@link #serviceRequest(String...)} call.
	 */
	protected void setMethodisPostUrlEncodedData() {
		requestFactory = new RequestFactory(){
			@Override
			public HttpUriRequest createRequest(String... params) {
				final HttpPost rslt = new HttpPost(getSparqlURL(m_serviceURL));
				try {
					rslt.setEntity(ConnectOptions.getFormEntity(pairs2map(params)));
				} catch (final Exception e) {
					throw new RuntimeException(e);
				}
				return rslt;
			}
		};
	}

	/**
	 * The method is a POST of the given document
	 * @param mimeType The mimetype of the document
	 * @param body     The string of the document body
	 */
	protected void setMethodisPost(String mimeType, String body) {
		StringEntity toPostx = null;
		try {
			toPostx = new StringEntity(body, mimeType,"utf-8");
		} catch (final UnsupportedEncodingException e) {
			throw new Error(e);
		}
		final HttpEntity toPost = toPostx;
		requestFactory = new RequestFactory(){

			@Override
			public HttpUriRequest createRequest(String... params) {
				final StringBuffer url = new StringBuffer();
				url.append(getSparqlURL(m_serviceURL));
				char sep = '?';
				for (int i=0;i<params.length;i+=2) {
					url.append(sep);
					url.append(params[i]);
					url.append('=');
					try {
						url.append(URLEncoder.encode(params[i+1], "UTF-8"));
					} catch (final UnsupportedEncodingException e) {
						// JVM must support UTF-8
						throw new Error(e);
					}
					sep='&';
				}
				final HttpPost rslt = new HttpPost(url.toString());
				rslt.setEntity(toPost);
				return rslt;
			}
		};
	}

	/**
	 * Normally a 400 or 404 response fails the test, calling this method allows such responses.
	 */
	protected void setAllow400s() {
		this.permit400s = true;
	}

	/**
	 * Assert that the update from {@link #update} has or has not taken place.
	 * This calls {@link #resetDefaultOptions()}, and the next call to {@link #serviceRequest(String...)}
	 * will need to be setup after this call.
	 * @param expected The expected result
	 * @throws IOException
	 */
	protected void checkUpdate(boolean expected) throws IOException {
		resetDefaultOptions();
		assertTrue(serviceRequest("query",askIfUpdated).contains(Boolean.toString(expected)));
	}

	/**
	 * The next request is a GET, (this is the default)
	 */
	protected void setMethodAsGet() {
		requestFactory = GET;
	}

	public  AbstractProtocolTest(HttpServlet servlet, String name) {
		super(name);
		this.servlet = servlet;
	}

	public AbstractProtocolTest(String name)  {
		this(new QueryServlet(), name);
	}
}
