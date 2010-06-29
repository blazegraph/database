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
import java.util.UUID;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.CommitRecordIndex;
import com.bigdata.journal.ForceEnum;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Name2Addr;
import com.bigdata.journal.ProxyTestCase;
import com.bigdata.journal.RootBlockView;
import com.bigdata.quorum.Quorum;
import com.bigdata.util.ChecksumUtility;

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

//            /*
//             * @todo we probably have to return the service which joined as the
//             * leader from getStore().
//             */
//            final Quorum<HAGlue,QuorumService<HAGlue>> q = store.getQuorum();
//
//            final long token = q.token();
//
//            q.getClient().assertLeader(token);
//
//            assertEquals(k, q.getMembers().length);

            // @todo verify binary state of all stores.
            
        } finally {
        
            store.destroy();
            
        }
        
    }

    public void test_emptyAbort() {

        final Journal store = getStore(getProperties());
        try {

            final Quorum<HAGlue,QuorumService<HAGlue>> q = store.getQuorum();

            final long token = q.token();

            q.assertLeader(token);

            store.abort();

            q.assertLeader(token);

        } finally {

            store.destroy();
            
        }

    }

    public void test_writeAbort() {

        final Journal store = getStore(getProperties());
        try {

            final Quorum<HAGlue,QuorumService<HAGlue>> q = store.getQuorum();

            final long token = q.token();

            q.assertLeader(token);
            
            store.write(getRandomData());
            
            q.assertLeader(token);
            
            store.abort();

            q.assertLeader(token);

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

            final Quorum<HAGlue,QuorumService<HAGlue>> q = store.getQuorum();

            final long token = q.token();

            q.assertLeader(token);

            store.commit();

            q.assertLeader(token);

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

            final Quorum<HAGlue,QuorumService<HAGlue>> q = store.getQuorum();

            final long token = q.token();

            q.assertLeader(token);

            // The 1st after a create is never empty. 
            store.commit();

            q.assertLeader(token);

            // This is an empty commit : @todo verify no change to root blocks.
            store.commit();

            q.assertLeader(token);

            /*
             * write a single record and then commit.
             */
            store.write(getRandomData());
            
            q.assertLeader(token);
            
            store.commit();

            q.assertLeader(token);
 
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

            final Quorum<HAGlue,QuorumService<HAGlue>> q = store.getQuorum();

            final long token = q.token();
            
            final ByteBuffer expected = getRandomData();

            final long addr = store.write(expected);

            q.assertLeader(token);
            
            store.commit();

            /*
             * The joined member services for the met quorum. The leader is in
             * the first position in this array. We verify (below) that the
             * joinOrder is for the [token] we obtained above, which is what
             * gives us the guarantee that this service is still the leader and
             * that the UUID in position zero is this service.
             */
            final UUID[] joinOrder = q.getJoinedMembers();

            /*
             * Verify the quorum is valid before we proceed with our assumptions
             * concerning the joinOrder.
             */
            q.assertQuorum(token);

            for (int i = 1; i < joinOrder.length; i++) {

                final UUID serviceId = joinOrder[i];
                
                final HAGlue remoteService = q.getClient().getService(serviceId);
                
                assertEquals(expected, remoteService.readFromDisk(q.token(),
                        store.getUUID(), addr));

            }

            q.assertQuorum(token);
            
        } finally {

            store.destroy();

        }

    }

//    public void test_getRootBlocksFromLeader() {
//        
//    }
//    
//    /**
//     * Replace our root blocks with those of the quorum leader (HA only).
//     * 
//     * FIXME This just grabs the root blocks of the quorum leader if there was
//     * no backing file.
//     * <p>
//     * In fact, we need to handle synchronization when the backing file exists
//     * and when it does not exist, however we will not be "in" the quorum on
//     * restart if we are not synchronized.
//     * <p>
//     * If we are not the leader of a quorum then we need to synchronize with the
//     * quorum leader.
//     * <p>
//     * Note: The delta can be obtained by comparing out current root block with
//     * the root block of the leader.
//     */
//    public void takeRootBlocksFromLeader(final Quorum<?,?> quorum) {
//
//        if (!quorum.isHighlyAvailable())
//            throw new UnsupportedOperationException();
//
//        if (quorum.getClient().isLeader(quorumToken)) {
//            // The leader can not issue this request to itself.
//            throw new UnsupportedOperationException();
//        }
//
//        if (_rootBlock.getLastCommitTime() != 0L) {
//            /*
//             * Journal already has committed state.
//             */
//            throw new UnsupportedOperationException();
//        }
//
//        try {
//
//            if (quorum.isHighlyAvailable())
//                System.err.println("HA JOURNAL INIT");
//
//            System.err.println("Requesting root block from leader.");
//            final UUID leaderId = quorum.getLeaderId();
//            quorum.assertQuorum(quorumToken);
//            // get the root blocks from the leader.
//            final byte[] tmp = quorum.getClient().getService(leaderId)
//                    .getRootBlock(getUUID());
//            // construct view from the root block.
//            final IRootBlockView rootBlock0 = new RootBlockView(//
//                    true, // isRootBlock0
//                    ByteBuffer.wrap(tmp), //
//                    new ChecksumUtility()//
//                    );
//            // construct view from the root block.
//            final IRootBlockView rootBlock1 = new RootBlockView(//
//                    true, // isRootBlock0
//                    ByteBuffer.wrap(tmp), //
//                    new ChecksumUtility()//
//                    );
//            System.err.println("Got root block from leader: " + rootBlock0);
//            if (rootBlock0.getCommitCounter() != 0L) {
//                throw new RuntimeException("Leader already has data: "
//                        + rootBlock0);
//            }
//
//            // overwrite our root blocks with the leader's.
//            _bufferStrategy.writeRootBlock(rootBlock0, ForceEnum.No);
//            _bufferStrategy.writeRootBlock(rootBlock1, ForceEnum.Force);
//
//            /*
//             * Do a low-level abort to discard the current write set and reload
//             * various things from the new root blocks.
//             */
//            _abort();
//
//        } catch (IOException ex) {
//
//            throw new RuntimeException(ex);
//
//        }
//
//    }

}
