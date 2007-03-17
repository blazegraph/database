/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Mar 15, 2007
 */

package com.bigdata.service;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.bigdata.objndx.IBatchOp;
import com.bigdata.service.DataService.RangeQueryResult;

/**
 * Client facing interface for communications with a data service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo provide implementations for embedded vs remote data service instances.
 *       Only remote data service instances discovered via the metadata locator
 *       service will support a scale-out solution.
 */
public class DataServiceClient implements IDataService {

    final IDataService delegate;
    
    public DataServiceClient(Properties properties) {
        
        // @todo provide option for other kinds of connections.
        delegate = new EmbeddedDataServiceClient(properties);
        
    }
    
    private class EmbeddedDataServiceClient extends DataService {
        
        EmbeddedDataServiceClient(Properties properties) {
            super(properties);
        }
        
    }

    /*
     * @todo implement remote data service client talking to NIO service
     * instance. this needs to locate the transaction manager service and
     * the metadata service for each index used by the client.
     */
    abstract private class NIODataServiceClient implements IDataService {
        
    }
    
    /**
     * Polite shutdown does not accept new requests and will shutdown once
     * the existing requests have been processed.
     */
    public void shutdown() {
        
        ((DataService)delegate).shutdown();
        
    }
    
    /**
     * Shutdown attempts to abort in-progress requests and shutdown as soon
     * as possible.
     */
    public void shutdownNow() {

        ((DataService)delegate).shutdownNow();

    }

    public void batchOp(long tx, String name, IBatchOp op) throws InterruptedException, ExecutionException {
        delegate.batchOp(tx, name, op);
    }

//    public void map(long tx, String name, byte[] fromKey, byte[] toKey, IMapOp op) throws InterruptedException, ExecutionException {
//        delegate.map(tx, name, fromKey, toKey, op);
//    }

    public RangeQueryResult rangeQuery(long tx, String name, byte[] fromKey, byte[] toKey, int flags) throws InterruptedException, ExecutionException {
        return delegate.rangeQuery(tx, name, fromKey, toKey, flags);
    }

    public void submit(long tx, IProcedure proc) throws InterruptedException, ExecutionException {
        delegate.submit(tx, proc);
    }

    public void abort(long tx) {
        delegate.abort(tx);
    }

    public long commit(long tx) {
        return delegate.commit(tx);
    }

}
