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
package com.bigdata.journal;

import java.util.Properties;

import com.bigdata.util.MillisecondTimestampFactory;

/**
 * Concrete implementation suitable for a local and unpartitioned database
 * introduces a trivial {@link ITransactionManager} and does NOT handle
 * {@link #overflow()} events.
 */
public class Journal extends ConcurrentJournal implements ITransactionManager {

    /**
     * 
     * @param properties See {@link com.bigdata.journal.Options}.
     */
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

    /**
     * A static instance is used so that different journals on the same JVM will
     * all use the same underlying time source.
     */
    private static final MillisecondTimestampFactory timestampFactory = new MillisecondTimestampFactory();

    /**
     * Waits for the next millsecond.
     */
    public long nextTimestamp() {

        return timestampFactory.nextMillis();

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

    public void wroteOn(long startTime, String[] resource) {
        
        /*
         * Ignored since the information is also in the local ITx.
         */
        
    }
    
}
