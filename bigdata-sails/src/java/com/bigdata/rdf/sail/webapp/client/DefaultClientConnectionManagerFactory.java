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

import org.apache.http.HttpHost;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.HttpParams;

/**
 * Factory for {@link ClientConnectionManager}.
 * <p>
 * Note: This does not implement
 * {@link org.apache.http.conn.ClientConnectionManagerFactory} because that
 * interface implies the use of the deprecated form of the
 * {@link ThreadSafeClientConnManager} constructor (the variant which accepts
 * the {@link HttpParams}s).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ClientConnectionManagerFactory.java 6174 2012-03-22 12:41:14Z
 *          thompsonbry $
 * 
 *          TODO Provide SPI resolution to configue this.
 */
public class DefaultClientConnectionManagerFactory 
//implements ClientConnectionManagerFactory 
{

    public static DefaultClientConnectionManagerFactory getInstance() {
        
        return new DefaultClientConnectionManagerFactory();
        
    }
    
    public ClientConnectionManager newInstance() {

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
    
    /**
     * Return a {@link SchemeRegistry} which has been pre-configured for HTTP
     * and HTTPS.
     */
    protected SchemeRegistry newSchemeRegistry() {
    
        final SchemeRegistry schemeRegistry = new SchemeRegistry();

        schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory
                .getSocketFactory()));
        
        schemeRegistry.register(new Scheme("https", 443, SSLSocketFactory
                .getSocketFactory()));

        return schemeRegistry;

    }
    
//    @Override
//    public ClientConnectionManager newInstance(HttpParams params,
//            SchemeRegistry schemeRegistry) {
//
//        return null;
//        
//    }

}
