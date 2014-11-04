package com.bigdata.rdf.sail.webapp.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.message.BasicNameValuePair;

public class ApacheConnectOptions extends AbstractConnectOptions {

    /** Request entity.
     */
    public HttpEntity entity = null;

	public ApacheConnectOptions(String serviceURL) {
		super(serviceURL);
	}

    /**
     * Add query params to a application/x-www-form-urlencoded entity.
     */
    public static HttpEntity getFormEntity(final Map<String, String[]> requestParams) 
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
    	
    	return new UrlEncodedFormEntity(formparams, "UTF-8");

    }

}
