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

import java.util.Map;

import org.apache.http.HttpEntity;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;

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
