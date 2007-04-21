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

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.bigdata.btree.IBatchOp;
import com.bigdata.journal.ValidationError;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Implementation suitable for a standalone embedded database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo either decouple client operations from the data service (they are
 *       synchronous) or drop this class and just use DataService directly for
 *       an embedded data service (or potentially integrate a
 *       {@link DataService} and {@link TransactionService} instance together
 *       using delegation patterns).
 */
abstract public class EmbeddedDataService implements IDataService, IServiceShutdown {

    private final DataService delegate;
    
    /**
     * Pool of threads for decoupling client operations.
     */
    final protected ExecutorService opService;
    
    public EmbeddedDataService(Properties properties) {
        
        delegate = new DataService(properties);
    
        // setup thread pool for decoupling client operations.
        opService = Executors.newFixedThreadPool(100, DaemonThreadFactory
                .defaultThreadFactory());

    }
    
    /**
     * Polite shutdown does not accept new requests and will shutdown once
     * the existing requests have been processed.
     */
    public void shutdown() {
        
        delegate.shutdown();
        
    }
    
    /**
     * Shutdown attempts to abort in-progress requests and shutdown as soon
     * as possible.
     */
    public void shutdownNow() {

        delegate.shutdownNow();
        
    }

    public void batchOp(long tx, String name, IBatchOp op) throws InterruptedException, ExecutionException {
        delegate.batchOp(tx, name, op);
    }

//    public void map(long tx, String name, byte[] fromKey, byte[] toKey, IMapOp op) throws InterruptedException, ExecutionException {
//        delegate.map(tx, name, fromKey, toKey, op);
//    }

//    public ResultSet rangeQuery(long tx, String name, byte[] fromKey, byte[] toKey, int flags) throws InterruptedException, ExecutionException {
//        return delegate.rangeQuery(tx, name, fromKey, toKey, flags);
//    }

//    public void submit(long tx, IProcedure proc) throws InterruptedException, ExecutionException {
//        delegate.submit(tx, proc);
//    }

    public void abort(long tx) throws IOException {
        delegate.abort(tx);
    }

    public long commit(long tx) throws ValidationError, IOException {
        return delegate.commit(tx);
    }

}
