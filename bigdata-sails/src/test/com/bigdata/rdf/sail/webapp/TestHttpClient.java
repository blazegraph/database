/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.http.HttpEntity;
import org.eclipse.jetty.client.HttpContentResponse;
import org.eclipse.jetty.client.HttpResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;

import com.bigdata.rdf.sail.webapp.client.ApacheConnectOptions;
import com.bigdata.rdf.sail.webapp.client.EntityContentProvider;
import com.bigdata.rdf.sail.webapp.client.JettyResponseListener;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.util.http.ApacheHttpClient;
import com.bigdata.util.http.DefaultHttpClient;
import com.bigdata.util.http.JettyHttpClient;

import junit.framework.TestCase;

/**
 * Tests used for the HttpClient refactor to move from
 * Apache to Jetty code base.
 * 
 * @author Martyn Cutcher
 *
 */
public class TestHttpClient extends TestCase {
	
	/**
	 * Just create client to connect to google.com with simple query
	 * @throws IOException 
	 * @throws ClientProtocolException 
	 */
	public void testSimpleApacheClient() throws Exception,
			IOException {
		org.apache.http.client.HttpClient client = new org.apache.http.impl.client.DefaultHttpClient();
		org.apache.http.client.methods.HttpHead method = new org.apache.http.client.methods.HttpHead(
				"http://ctc.io/");

		// Execute the method.
		org.apache.http.HttpResponse response = client.execute(method);

		if (response.getStatusLine().getStatusCode() != org.apache.http.HttpStatus.SC_OK) {
			System.err.println("Method failed: " + response.getStatusLine());
		} else {
			System.out.println("Success: " + response.getStatusLine());

		}
	}

	public void testSimpleBigdataApacheClient() throws Exception {
		ApacheHttpClient client = new ApacheHttpClient();
		
		com.bigdata.util.http.IHttpResponse response = client.GET("http://ctc.io/");
		
		if (response.getStatusCode() == 200) {
			System.out.println("Apache Success: " + response);
		} else {
			System.err.println("Apache Failure: " + response);
		}
	}
	
	public void _testSimpleApacheRepository() {
		org.apache.http.client.HttpClient client = new org.apache.http.impl.client.DefaultHttpClient();
		
		final RemoteRepository repo = new RemoteRepository("http://ctc.io/", false, client, null);
		
	}

	/**
	 * Jetty 9 documentation:  http://www.eclipse.org/jetty/documentation/current/http-client-api.html
	 * 
	 * @throws Exception
	 */
	public void testSimpleJettyClient() throws Exception {
		org.eclipse.jetty.client.HttpClient client = new org.eclipse.jetty.client.HttpClient();
		client.start();
		
		try {
			org.eclipse.jetty.client.HttpContentResponse response = (HttpContentResponse) client.GET("http://ctc.io/");
			 
			// Waits until the exchange is terminated
			if (response.getStatus() != org.apache.http.HttpStatus.SC_OK) {
				System.err.println("Method failed: " + response.toString());
			} else {
				System.out.println("Success: " + response.toString());
	
			}
			
			final Request clientRequest = client.POST("http://ctc.io/");
			
			final Map<String, String[]> params = new LinkedHashMap<String, String[]>();
			params.put("Name", new String[] {"Martyn"});
			
			HttpEntity entity = ApacheConnectOptions.getFormEntity(params); //getFormEntity
			
			clientRequest.content(new EntityContentProvider(entity));
			
			response = (HttpContentResponse) clientRequest.send();
			
			if (response.getStatus() != org.apache.http.HttpStatus.SC_OK) {
				System.err.println("Method failed: " + response.toString());
			} else {
				System.out.println("Success: " + response.toString() + ", type: " + response.getMediaType());	
			}
			
			response = (HttpContentResponse) clientRequest.send();
			
			if (response.getStatus() != org.apache.http.HttpStatus.SC_OK) {
				System.err.println("Method failed: " + response.toString());
			} else {
				System.out.println("Second Success: " + response.toString() + ", type: " + response.getMediaType() + ", encoding: " + response.getEncoding());	
			}
			
			final JettyResponseListener listener = new JettyResponseListener();
			
			clientRequest.send(listener);
			
			final HttpResponse lresp = (HttpResponse) listener.get(5, TimeUnit.SECONDS);
			if (lresp.getStatus() != org.apache.http.HttpStatus.SC_OK) {
				System.err.println("Method failed: " + lresp.toString());
			} else {
				System.out.println("Third Listener Success: " + lresp.getReason() 
						+ "\nheaders: " + lresp.getHeaders() 
						+ "\ntype: " + listener.getContentType()
						+ "\nencoding: " + listener.getContentEncoding());	
			}
			
			final StringBuilder sb = new StringBuilder();
			final InputStream instr = listener.getInputStream();
			final byte[] buf = new byte[256];
			int rdlen = 0;
			while ((rdlen = instr.read(buf)) != -1) {
				sb.append(new String(buf, 0, rdlen));
			}
			
			System.out.println("\nStream Content:\n\n" + sb);

		} finally {
			client.destroy();
		}
	}

	public void testMultipartJettyClient() throws Exception {
		org.eclipse.jetty.client.HttpClient client = new org.eclipse.jetty.client.HttpClient();
		client.start();
		
		try {
			org.eclipse.jetty.client.HttpContentResponse response = (HttpContentResponse) client.GET("http://ctc.io/");
			 
			// Waits until the exchange is terminated
			if (response.getStatus() != org.apache.http.HttpStatus.SC_OK) {
				System.err.println("Method failed: " + response.toString());
			} else {
				System.out.println("Success: " + response.toString());
	
			}
			
			final Request clientRequest = client.POST("http://ctc.io/");
			
			final Map<String, String[]> params = new LinkedHashMap<String, String[]>();
			params.put("Name", new String[] {"Martyn"});
			
			HttpEntity entity = ApacheConnectOptions.getFormEntity(params); //getFormEntity
			
			clientRequest.content(new EntityContentProvider(entity));
			
			response = (HttpContentResponse) clientRequest.send();
			
			if (response.getStatus() != org.apache.http.HttpStatus.SC_OK) {
				System.err.println("Method failed: " + response.toString());
			} else {
				System.out.println("Success: " + response.toString() + ", type: " + response.getMediaType());	
			}
			
			response = (HttpContentResponse) clientRequest.send();
			
			if (response.getStatus() != org.apache.http.HttpStatus.SC_OK) {
				System.err.println("Method failed: " + response.toString());
			} else {
				System.out.println("Second Success: " + response.toString() + ", type: " + response.getMediaType() + ", encoding: " + response.getEncoding());	
			}
			
			final JettyResponseListener listener = new JettyResponseListener();
			
			clientRequest.send(listener);
			
			final HttpResponse lresp = (HttpResponse) listener.get(5, TimeUnit.SECONDS);
			if (lresp.getStatus() != org.apache.http.HttpStatus.SC_OK) {
				System.err.println("Method failed: " + lresp.toString());
			} else {
				System.out.println("Third Listener Success: " + lresp.getReason() 
						+ "\nheaders: " + lresp.getHeaders() 
						+ "\ntype: " + listener.getContentType()
						+ "\nencoding: " + listener.getContentEncoding());	
			}

		} finally {
			client.destroy();
		}
	}

	public void testSimpleBigdataJettyClient() throws Exception {
		JettyHttpClient client = new JettyHttpClient();
		
		com.bigdata.util.http.IHttpResponse response = client.GET("http://ctc.io/");
		
		if (response.getStatusCode() == 200) {
			System.out.println("Jetty Success: " + response);
		} else {
			System.err.println("Jetty Failure: " + response);
		}
	}
	
	public void testEntityContentProviders() {
		// Let's see how standard ApaceForms and JettyForms compare.
		
		
	}

}
