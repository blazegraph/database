package com.bigdata.resources;

import java.io.IOException;

import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.util.MillisecondTimestampFactory;

/**
 * Mock implementation used by some of the unit tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class MockLocalTransactionManager extends AbstractLocalTransactionManager {

    public MockLocalTransactionManager(ResourceManager resourceManager) {

        super(resourceManager);

    }

    public long nextTimestamp() {

        return MillisecondTimestampFactory.nextMillis();
        
    }

    public long lastCommitTime() throws IOException {
        
        return lastCommitTime;
        
    }

    synchronized public void notifyCommit(final long commitTime)
            throws IOException {

        if (commitTime > lastCommitTime) {

            lastCommitTime = commitTime;

        }
        
    }
    
    private volatile long lastCommitTime = 0L;

}