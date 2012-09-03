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
package com.bigdata.journal.jini.ha;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.core.lookup.ServiceItem;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.quorum.QuorumException;
import com.bigdata.quorum.QuorumListener;
import com.bigdata.quorum.QuorumMember;

/**
 * {@link Quorum} with fixed membership and pipeline order. The quorum is met
 * when ALL services are joined. Join/leave is managed through discovery of
 * {@link HAGlue} services using jini/River.
 * 
 * FIXME AbstractJournal uses a met quorum versus a full quorum to decide the
 * vote on a commit. That needs to be changed since we have no way to handle
 * resynchronization on leave/join and that is necessary if we allow a commit in
 * which less than all services vote yes.
 * 
 * FIXME The {@link QuorumMember}/{@link QuorumService} interfaces are necessary
 * bits of the HA APIs. They need to be implemented for this quorum in order for
 * things to work.
 */
class StaticQuorum implements Quorum<HAGlue, QuorumService<HAGlue>> {

    /**
     * This service.
     */
    private final UUID serviceUUID;
    /**
     * The write replication pipeline.
     */
    private final UUID[] pipeline;
    /**
     * Cached discovery for {@link HAGlue} services.
     */
    private final HAJournalDiscoveryClient discoveryClient;
    /**
     * TODO Does not respect tokens.
     */
    private final long token = 0L;
    private final long lastValidToken = 0L;
//    /**
//     * The previous service in the write replication pipeline and
//     * <code>null</code> iff this is the leader.
//     */
//    private final UUID prior;
//    /**
//     * The next service in the write replication pipeline and <code>null</code>
//     * iff this is the tail of the pipeline.
//     */
//    private final UUID next;

    public StaticQuorum(final UUID serviceUUID, final UUID[] pipeline,
            final HAJournalDiscoveryClient discoveryClient) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        if (pipeline == null)
            throw new IllegalArgumentException();

        if (discoveryClient == null)
            throw new IllegalArgumentException();

        this.serviceUUID = serviceUUID;

        this.pipeline = pipeline;

        this.discoveryClient = discoveryClient;
        
        // Used to check for duplicates.
        final Set<UUID> set = new LinkedHashSet<UUID>();

        // Scan the pipeline UUIDs.
        for (int i = 0; i < pipeline.length; i++) {

            final UUID uuid = pipeline[i];

            if (uuid == null)
                throw new NullPointerException();

            if (!set.add(uuid))
                throw new RuntimeException("Duplicate in pipeline: uuid="
                        + uuid);

        }
        
        if (!set.contains(serviceUUID))
            throw new RuntimeException("Service not in pipeline: service="
                    + serviceUUID + ", pipeline=" + Arrays.toString(pipeline));

//        final UUID[] priorAndNext = getPipelinePriorAndNext(serviceUUID);
//        
//        this.prior = priorAndNext[0];
//
//        this.next = priorAndNext[1];

    }

    @Override
    public int replicationFactor() {

        return pipeline.length;
        
    }

    @Override
    public long token() {
        
        return token;
        
    }

    @Override
    public long lastValidToken() {

        return lastValidToken;
        
    }

    @Override
    public boolean isHighlyAvailable() {

        return replicationFactor() > 1;
        
    }

    @Override
    public boolean isQuorumMet() {

        final UUID[] joined = getJoined();
        
        if (joined.length == replicationFactor()) {
        
            return true;
            
        }

        return false;
        
    }

    @Override
    public void addListener(QuorumListener listener) {

        throw new UnsupportedOperationException();
        
    }

    @Override
    public void removeListener(QuorumListener listener) {

        throw new UnsupportedOperationException();
        
    }

    @Override
    public UUID[] getMembers() {

        return pipeline;
        
    }

    @Override
    public Map<Long, UUID[]> getVotes() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getCastVote(UUID serviceId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UUID[] getJoined() {

        final ServiceItem[] serviceItems = discoveryClient.getServiceItems(
                0/* maxCount */, null/* filter */);

        final UUID[] uuids = new UUID[serviceItems.length];

        for (int i = 0; i < serviceItems.length; i++) {

            uuids[i] = JiniUtil.serviceID2UUID(serviceItems[i].serviceID);
            
        }

        return uuids;

    }

    @Override
    public UUID[] getPipeline() {

        return pipeline;
        
    }

    @Override
    public UUID getLastInPipeline() {
        
        return pipeline[pipeline.length - 1];
        
    }

    @Override
    public UUID[] getPipelinePriorAndNext(final UUID serviceId) {

        // Used to figure out prior/next.
        UUID prior = null, next = null;

        // Scan the pipeline UUIDs.
        for (int i = 0; i < pipeline.length; i++) {

            final UUID uuid = pipeline[i];

            if (uuid.equals(serviceId)) {

                if (i > 0) {

                    prior = pipeline[i - 1];

                }

                if (i + 1 < pipeline.length) {

                    next = pipeline[i + 1];
                }

            }

        }

        return new UUID[] { prior, next };
        
    }

    @Override
    public long awaitQuorum() throws InterruptedException,
            AsynchronousQuorumCloseException {

        try {
        
            return awaitQuorum(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            
        } catch (TimeoutException e) {

            // Timeout is not expected.
            throw new AssertionError();
            
        }

    }

    @Override
    public long awaitQuorum(final long timeout, final TimeUnit units)
            throws InterruptedException, AsynchronousQuorumCloseException,
            TimeoutException {

        final long begin = System.nanoTime();

        final long nanos = units.toNanos(timeout);
        
        long remaining = nanos;
        
        boolean met = false; // assume false.
        
        while (!met) {
        
            // remaining = nanos - (now - begin) [aka elapsed]
            remaining = nanos - (System.nanoTime() - begin);
            
            if (remaining < 0L)
                throw new TimeoutException("nanos="+nanos+", timeout="+timeout+", units="+units);

            /*
             * TODO Should this be a blocking operation w/ timeout that can
             * notice service joins? Simply checking the caching discovery
             * client spins since the operation is non-blocking. Non-blocking
             * is Ok, but we should be seeing Condition events so we do not
             * spin in a tight loop.
             */
            met = isQuorumMet();
            
        }

        return token;

    }

    @Override
    public void awaitBreak() throws InterruptedException,
            AsynchronousQuorumCloseException {

        throw new UnsupportedOperationException();
        
    }

    @Override
    public void awaitBreak(long timeout, TimeUnit units)
            throws InterruptedException, AsynchronousQuorumCloseException,
            TimeoutException {

        throw new UnsupportedOperationException();
        
    }

    @Override
    public void assertQuorum(long token) {
    }

    @Override
    public void assertLeader(long token) {

        if (pipeline[0].equals(serviceUUID)) {
        
            return;
            
        }

        throw new QuorumException();
        
    }

    @Override
    public UUID getLeaderId() {

        return pipeline[0];
        
    }

    @Override
    public void start(QuorumService<HAGlue> client) {
    }

    @Override
    public void terminate() {
    }

    @Override
    public QuorumService<HAGlue> getClient() {
        return null;
    }

    @Override
    public QuorumMember<HAGlue> getMember() {
        return null;
    }

    @Override
    public QuorumActor<HAGlue, QuorumService<HAGlue>> getActor() {
        return null;
    }

}