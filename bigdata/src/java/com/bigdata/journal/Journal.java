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
 * Created on Oct 8, 2006
 */

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.isolation.ReadOnlyIsolatedIndex;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.ReadCommittedTx.ReadCommittedIndex;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.IIndex;
import com.bigdata.objndx.IndexSegment;
import com.bigdata.objndx.ReadOnlyIndex;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.scaleup.MasterJournal;
import com.bigdata.scaleup.SlaveJournal;
import com.bigdata.scaleup.MasterJournal.Options;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Concrete implementation that does not handle {@link #overflow()} events.
 */
public class Journal extends AbstractJournal implements ITransactionManager, IIndexManager {

    public Journal(Properties properties) {
        
        super(properties);
        
    }
    
    public long commit() {

        return commitNow(nextTimestamp());

    }

    /*
     * ITransactionManager and friends.
     * 
     * @todo refactor into an ITransactionManager service. provide an
     * implementation that supports only a single Journal resource and an
     * implementation that supports a scale up/out architecture. the journal
     * should resolve the service using JINI. the timestamp service should
     * probably be co-located with the transaction service.
     */

    /**
     * The service used to generate commit timestamps.
     * 
     * @todo parameterize using {@link Options} so that we can resolve a
     *       low-latency service for use with a distributed database commit
     *       protocol.
     */
    protected final ITimestampService timestampFactory = LocalTimestampService.INSTANCE;

    public long nextTimestamp() {
        
        return timestampFactory.nextTimestamp();
        
    }

    public long newTx(IsolationEnum level) {

        final long startTime = nextTimestamp();

        switch (level) {

        case ReadCommitted:
            new ReadCommittedTx(this, startTime);
            break;
        
        case ReadOnly:
            new Tx(this, startTime, true);
            break;
        
        case ReadWrite:
            new Tx(this, startTime, false);
            break;

        default:
            throw new AssertionError("Unknown isolation level: " + level);
        }

        return startTime;
        
    }
    
}
