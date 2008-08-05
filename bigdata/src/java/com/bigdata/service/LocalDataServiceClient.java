/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Apr 1, 2008
 */

package com.bigdata.service;

import java.util.Properties;

import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.counters.CounterSet;
import com.bigdata.journal.AbstractTask;

/**
 * Client for a local (embedded) {@link DataService} exposed as an
 * {@link IBigdataFederation}. Code written to this API is directly portable to
 * scale-out {@link IBigdataFederation}s (but see below).
 * <p>
 * The {@link IDataService} provides full concurrency control and transaction
 * support, but there is no metadata index so all indices are monolithic (they
 * are never broken into key-range partitions).
 * <p>
 * Note: since all indices are stored within the same {@link IDataService} you
 * can write JOINs that enjoy certain optimizations by obtaining a lock on more
 * than one index at a time. Such JOINs must be written using
 * {@link AbstractTask} rather than {@link IIndexProcedure} since the latter
 * assumes access to a single index at a time. However, such code is not
 * portable to remote {@link IDataService}s nor to a federation that supports
 * key-range partitioned indices.
 * 
 * @see LocalDataServiceFederation
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalDataServiceClient extends AbstractClient {

    /**
     * Options understood by the {@link LocalDataServiceClient}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends DataService.Options, IBigdataClient.Options {
        
        /**
         * The port on which the federation will run an optional
         * <code>httpd</code> service (default {@value #DEFAULT_HTTPD_PORT})
         * -or- ZERO (0) to NOT start the <code>httpd</code> service. This
         * service may be used to view the {@link CounterSet} hierarchy for the
         * embedded data service.
         */
        String HTTPD_PORT = "httpd.port";
        
        String DEFAULT_HTTPD_PORT = "80";
        
    }
    
    /**
     * @param properties
     * 
     * @see Options
     */
    public LocalDataServiceClient(Properties properties) {
       
        super(properties);

    }

    /**
     * The federation and <code>null</code> iff not connected.
     */
    private LocalDataServiceFederation fed = null;

    synchronized public boolean isConnected() {
        
        return fed != null;
        
    }
    
    /**
     * Note: This also shutdown the local {@link IDataService}.
     */
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
    
    synchronized public LocalDataServiceFederation getFederation() {

        if (fed == null) {

            throw new IllegalStateException();

        }

        return fed;

    }

    synchronized public LocalDataServiceFederation connect() {

        if (fed == null) {

            fed = new LocalDataServiceFederation(this);

        }

        return fed;

    }

}
