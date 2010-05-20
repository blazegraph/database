/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Apr 28, 2010
 */

package com.bigdata.journal.ha;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.bigdata.journal.CommitRecordIndex;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Name2Addr;
import com.bigdata.journal.ProxyTestCase;

/**
 * Unit tests bootstrap 3 journals in a specified failover chain and demonstrate
 * pipelined writes and the commit protocol.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestHAWritePipeline.java 2883 2010-05-19 16:35:18Z thompsonbry
 *          $
 * 
 * @todo test restart safe (but test quorum restart first).
 * 
 * @todo test quorum membership change
 * 
 * @todo binary compare of the journals after commit.
 */
public class TestHAWritePipeline extends ProxyTestCase<Journal> {

    /**
     * 
     */
    public TestHAWritePipeline() {
    }

    /**
     * @param name
     */
    public TestHAWritePipeline(String name) {
        super(name);
    }

    public void test_setupDestroyQuorum() {

        final Journal store = getStore(getProperties());

        try {
            
            final QuorumManager qm = store.getQuorumManager();

            final Quorum q = qm.getQuorum();

            final long token = q.token();

            qm.assertQuorumLeader(token);

            // @todo verify binary state of all stores.
            
        } finally {
        
            store.destroy();
            
        }
        
    }

    public void test_emptyAbort() {

        final Journal store = getStore(getProperties());
        try {

            final QuorumManager qm = store.getQuorumManager();

            final Quorum q = qm.getQuorum();

            final long token = q.token();

            qm.assertQuorumLeader(token);

            store.abort();

            qm.assertQuorumLeader(token);

        } finally {

            store.destroy();
            
        }

    }

    public void test_writeAbort() {

        final Journal store = getStore(getProperties());
        try {

            final QuorumManager qm = store.getQuorumManager();
            
            final Quorum q = qm.getQuorum();

            final long token = q.token();

            qm.assertQuorumLeader(token);
            
            store.write(getRandomData());
            
            qm.assertQuorumLeader(token);
            
            store.abort();

            qm.assertQuorumLeader(token);

        } finally {

            store.destroy();
            
        }

    }

    /**
     * Commit without writing any data onto the HA journal. However, since the
     * journal creates some persistent indices when it is created this is NOT an
     * "empty" commit. The registrations for the {@link CommitRecordIndex} and
     * the {@link Name2Addr} index will both be written by the first commit on
     * the journal. (In order to examine the behavior when no data have been
     * written since the last commit you need to do a 2nd commit.)
     */
    public void test_createAndCommit() {

        final Journal store = getStore(getProperties());
        try {

            final QuorumManager qm = store.getQuorumManager();

            final Quorum q = qm.getQuorum();

            final long token = q.token();

            qm.assertQuorumLeader(token);

            store.commit();

            qm.assertQuorumLeader(token);

       } finally {

            store.destroy();
            
        }

    }

    /**
     * 
     */
    public void test_createCommitWriteCommit() {

        final Journal store = getStore(getProperties());
        try {

            final QuorumManager qm = store.getQuorumManager();
            
            final Quorum q = qm.getQuorum();

            final long token = q.token();

            qm.assertQuorumLeader(token);

            // The 1st after a create is never empty. 
            store.commit();

            qm.assertQuorumLeader(token);

            // This is an empty commit : @todo verify no change to root blocks.
            store.commit();

            qm.assertQuorumLeader(token);

            /*
             * write a single record and then commit.
             */
            store.write(getRandomData());
            
            qm.assertQuorumLeader(token);
            
            store.commit();

            qm.assertQuorumLeader(token);
 
        } finally {

            store.destroy();
            
        }

    }

    /**
     * Test the ability to read on the followers in a quorum after a commit.
     * 
     * @todo test ability of a follower to read on another node in the quorum,
     *       including a master.
     */
    public void test_writeCommitReadBackOnQuorum() throws IOException {

        final Journal store = getStore(getProperties());
        try {

            final QuorumManager qm = store.getQuorumManager();
            
            final Quorum q = qm.getQuorum();

            final long token = q.token();
            
            final ByteBuffer expected = getRandomData();

            final long addr = store.write(expected);

            qm.assertQuorumLeader(token);
            
            store.commit();

            qm.assertQuorumLeader(token);
            
            for (int i = 0; i < q.replicationFactor(); i++) {

                assertEquals(expected, q.getHAGlue(i).readFromDisk(q.token(),
                        addr));

            }

            qm.assertQuorumLeader(token);
            
        } finally {

            store.destroy();

        }

    }

}
