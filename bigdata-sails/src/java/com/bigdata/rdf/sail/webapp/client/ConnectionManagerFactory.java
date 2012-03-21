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

/**
 * Factory for {@link ClientConnectionManager}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ConnectionManagerFactory {

    final private ClientConnectionManager cm;

    private ConnectionManagerFactory(final ClientConnectionManager cm) {

        this.cm = cm;

    }

    private static ConnectionManagerFactory instance = null;

    /**
     * Returns a shared {@link ClientConnectionManager}.
     */
    synchronized public static ClientConnectionManager getInstance() {

        if (instance == null) {

            final SchemeRegistry schemeRegistry = new SchemeRegistry();

            schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory
                    .getSocketFactory()));
            schemeRegistry.register(new Scheme("https", 443, SSLSocketFactory
                    .getSocketFactory()));

            final ThreadSafeClientConnManager cm = new ThreadSafeClientConnManager(
                    schemeRegistry);

            // Increase max total connection to 200
            cm.setMaxTotal(200);

            // Increase default max connection per route to 20
            cm.setDefaultMaxPerRoute(20);

            // Increase max connections for localhost to 50
            final HttpHost localhost = new HttpHost("locahost");

            cm.setMaxForRoute(new HttpRoute(localhost), 50);

            instance = new ConnectionManagerFactory(cm);

        }

        return instance.cm;

    }

}
