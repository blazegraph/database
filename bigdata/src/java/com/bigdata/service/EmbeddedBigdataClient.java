/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jul 25, 2007
 */

package com.bigdata.service;

import java.util.Properties;

/**
 * A client for an embedded federation (the client and the data services all run
 * in the same process).
 * 
 * @see EmbeddedBigdataFederation
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmbeddedBigdataClient extends AbstractBigdataClient {

    /**
     * 
     * @param properties
     *            See {@link EmbeddedBigdataFederation.Options}.
     */
    public EmbeddedBigdataClient(Properties properties) {

        super(properties);
        
    }
    
    /**
     * The federation and <code>null</code> iff not connected.
     */
    private EmbeddedBigdataFederation fed = null;

    synchronized public boolean isConnected() {
        
        return fed != null;
        
    }
    
    synchronized public void disconnect(boolean immediateShutdown) {
        
        if (fed != null) {

            if(immediateShutdown) {
                
                fed.shutdownNow();
                
            } else {
                
                fed.shutdown();
                
            }
            
        }
        
        fed = null;

    }
    
    synchronized public IBigdataFederation getFederation() {

        if (fed == null) {

            throw new IllegalStateException();

        }

        return fed;

    }

    synchronized public IBigdataFederation connect() {

        if (fed == null) {

            fed = new EmbeddedBigdataFederation(this, getProperties() );

        }

        return fed;

    }

}
