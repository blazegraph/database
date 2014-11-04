package com.bigdata.rdf.sail.webapp.client;

import java.util.Map;

import org.apache.http.HttpEntity;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.util.Fields;


public class JettyConnectOptions extends AbstractConnectOptions {
	
	HttpEntity entity;
	final Request request;
	final HttpClient client;

	public JettyConnectOptions(String serviceURL, HttpClient client) {
		super(serviceURL);
		
		this.client = client;
		
		request = client.newRequest(serviceURL);
	}

    /**
     * Add query params to the request.
     */
    public void addFormContent(final Map<String, String[]> requestParams) 
    		throws Exception {
    	
    	
    	assert request != null;
    	
    	if (requestParams != null) {
    		
//    		final Fields fields = new Fields();
//    		
//            for (Map.Entry<String, String[]> e : requestParams.entrySet()) {
//                final String name = e.getKey();
//                final String[] vals = e.getValue();
//                
//                if (vals == null) {
//                	fields.add(name, null);
//                } else {
//                    for (String val : vals) {
//                    	fields.add(name, val);
//                    }
//                }
//            } // next Map.Entry
            
            entity = ApacheConnectOptions.getFormEntity(requestParams);
    	}
    	
    }

}
