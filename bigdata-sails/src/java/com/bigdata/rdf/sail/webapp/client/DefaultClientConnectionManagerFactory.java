/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
/*
 * Created on Mar 21, 2012
 */

package com.bigdata.rdf.sail.webapp.client;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Factory for {@link HttpClient}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         TODO Provide SPI resolution to configure this.
 */
public class DefaultClientConnectionManagerFactory implements IHttpClientFactory
{

    public static IHttpClientFactory getInstance() {
        
        return new DefaultClientConnectionManagerFactory();
        
    }

	/**
	 * {@inheritDoc}
	 * <p>
	 * This implementation:
	 * <ul>
	 * <li>Sets up redirect handling.</li>
	 * <li>Sets a default SSL context factory that trusts all certificates. This
	 * allows encrypted communications with any SSL endpoint, but it does not
	 * verify the identity associated with that certificate.</li>
	 * </ul>
	 */
    public HttpClient newInstance() {

		final HttpClient cm = new HttpClient(
				new SslContextFactory(true/* trustAll */));
        
        try {

        	cm.setFollowRedirects(true);
    		
			cm.start();

        } catch (Exception e) {
			
        	throw new RuntimeException("Unable to start HttpClient", e);
        	
		}

        return cm;

    }

}
