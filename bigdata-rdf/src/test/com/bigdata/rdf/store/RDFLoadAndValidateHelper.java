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
 * Created on May 8, 2008
 */

package com.bigdata.rdf.store;

import java.io.File;
import java.io.FilenameFilter;
import java.util.concurrent.TimeUnit;

import org.openrdf.rio.RDFFormat;

import com.bigdata.service.IBigdataClient;

/**
 * Helper class for concurrent data load and post-load verification.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFLoadAndValidateHelper {

    final IBigdataClient client;
    
    final int nthreads;
    
    final int bufferCapacity;
    
    final ConcurrentDataLoader service;

    final File file;
    
    final FilenameFilter filter;

    final boolean verifyData = false;

    final RDFFormat fallback = RDFFormat.RDFXML;
    
    final int nclients;
    
    final int clientNum;
    
    public RDFLoadAndValidateHelper(IBigdataClient client, int nthreads,
            int bufferCapacity, File file, FilenameFilter filter) {
        
        this(client,nthreads,bufferCapacity,file, filter, 1/*nclients*/,0/*clientNum*/);
        
    }
    
    public RDFLoadAndValidateHelper(IBigdataClient client, int nthreads,
            int bufferCapacity, File file, FilenameFilter filter, int nclients, int clientNum) {

        this.client = client;
        
        this.nthreads = nthreads;

        this.bufferCapacity = bufferCapacity;

        service = new ConcurrentDataLoader(client, nthreads, nclients,
                clientNum);

        this.file = file;
        
        this.filter = filter;
        
        this.nclients = nclients;
        
        this.clientNum = clientNum;

    }
    
    public void load(AbstractTripleStore db) throws InterruptedException {

        final ConcurrentDataLoader.RDFLoadTaskFactory loadTaskFactory = new ConcurrentDataLoader.RDFLoadTaskFactory(
                db, bufferCapacity, verifyData, fallback);

        // Setup counters.
        loadTaskFactory.setupCounters(service.getCounters(client
                .getFederation()));

        // notify will run tasks.
        loadTaskFactory.notifyStart();

        // read files and run tasks.
        service.process(file, filter, loadTaskFactory);

        // await completion of all tasks.
        service.awaitCompletion(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        // notify did run tasks.
        loadTaskFactory.notifyEnd();
        
        System.err.println(loadTaskFactory.reportTotals());

    }

    public void validate(AbstractTripleStore db) throws InterruptedException {

        final ConcurrentDataLoader.RDFVerifyTaskFactory verifyTaskFactory = new ConcurrentDataLoader.RDFVerifyTaskFactory(
                db, bufferCapacity, verifyData, fallback);

        // notify will run tasks.
        verifyTaskFactory.notifyStart();
        
        // read files and run tasks.
        service.process(file, filter, verifyTaskFactory);

        // await completion of all tasks.
        service.awaitCompletion(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        // notify did run tasks.
        verifyTaskFactory.notifyEnd();

        // Report on #terms and #stmts parsed, found, and not found
        System.err.println(verifyTaskFactory.reportTotals());
        
    }

    public void shutdownNow() {
        
        service.shutdownNow();
        
    }

    protected void finalize() throws Throwable {
        
        service.shutdownNow();
        
    }
    
}
