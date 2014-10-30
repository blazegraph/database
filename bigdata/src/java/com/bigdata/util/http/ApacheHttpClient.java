package com.bigdata.util.http;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicNameValuePair;

/**
 * The ApacheHttpClient class is the first stage in the refactor.
 * 
 * It implements the IHttpClient interface using the Apache components. The
 * HttpClient code is updated to use this client, confirming the functional
 * equivalence.  Then the JettyHttpClient, implementing the same interface, is
 * used instead.
 * 
 * @author Martyn Cutcher
 *
 */
public class ApacheHttpClient implements IHttpClient {
	final HttpClient m_apache;
	
	public ApacheHttpClient() {
		m_apache = new org.apache.http.impl.client.DefaultHttpClient();
	}
	
	public IHttpResponse GET(final String url) throws Exception {
		org.apache.http.client.methods.HttpHead method = new org.apache.http.client.methods.HttpHead(
				url);

		// Execute the method.
		org.apache.http.HttpResponse response = m_apache.execute(method);

		return new ApacheClientResponse(response); //.getStatusLine().getStatusCode();
	}
	
	class ApacheClientResponse implements IHttpResponse {

		final HttpResponse m_response;
		
		public ApacheClientResponse(final HttpResponse response) {
			m_response = response;
		}
		
		public int getStatusCode() {
			return m_response.getStatusLine().getStatusCode();
		}
	}

	@Override
	public IHttpEntity createEntity() {
		return null;
	}
	
	class ApacheEntity implements IHttpEntity {
		final HttpEntity entity;
		
		ApacheEntity(HttpEntity raw) {
			entity = raw;
		}
	}
	
    public IHttpEntity getFormEntity(final Map<String, String[]> requestParams) 
    		throws Exception {
    	
    	final List<NameValuePair> formparams = new ArrayList<NameValuePair>();
    	
    	if (requestParams != null) {
            for (Map.Entry<String, String[]> e : requestParams.entrySet()) {
                final String name = e.getKey();
                final String[] vals = e.getValue();
                
                if (vals == null) {
                	formparams.add(new BasicNameValuePair(name, null));
                } else {
                    for (String val : vals) {
                    	formparams.add(new BasicNameValuePair(name, val));
                    }
                }
            } // next Map.Entry
    	}
    	
    	return new ApacheEntity(new UrlEncodedFormEntity(formparams, "UTF-8"));

    }
    
    class HttpRequest implements IHttpRequest {
    	final HttpUriRequest request;
    	HttpRequest(HttpUriRequest areq) {
    		request = areq;
    	}
    }

	@Override
	public IHttpRequest newRequest(final String uri, final String method) {
		final HttpUriRequest request;
		
		if (method.equals("GET")) {
			request = new HttpGet(uri);
		} else if (method.equals("POST")) {
			request = new HttpPost(uri);
		} else if (method.equals("DELETE")) {
			request = new HttpDelete(uri);
		} else if (method.equals("PUT")) {
			request = new HttpPut(uri);
		} else {
			throw new IllegalArgumentException();
		}

		return new HttpRequest(request);
	}
}
