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

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.quorum.QuorumListener;
import com.bigdata.quorum.QuorumMember;

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
     * The previous service in the write replication pipeline and
     * <code>null</code> iff this is the leader.
     */
    private final UUID prior;
    /**
     * The next service in the write replication pipeline and <code>null</code>
     * iff this is the tail of the pipeline.
     */
    private final UUID next;

    public StaticQuorum(final UUID serviceUUID, final UUID[] pipeline) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        if (pipeline == null)
            throw new IllegalArgumentException();

        this.serviceUUID = serviceUUID;

        this.pipeline = pipeline;

        // Used to check for duplicates.
        final Set<UUID> set = new LinkedHashSet<UUID>();

        // Used to figure out prior/next.
        UUID prior = null, next = null;

        // Scan the pipeline UUIDs.
        for (int i = 0; i < pipeline.length; i++) {

            final UUID uuid = pipeline[i];

            if (uuid == null)
                throw new NullPointerException();

            if (!set.add(uuid))
                throw new RuntimeException("Duplicate in pipeline: uuid="
                        + uuid);

            if (uuid.equals(serviceUUID)) {

                if (i > 0) {

                    prior = pipeline[i - 1];

                }

                if (i + 1 < pipeline.length) {

                    next = pipeline[i + 1];
                }

            }

        }

        if (!set.contains(serviceUUID))
            throw new RuntimeException("Service not in pipeline: service="
                    + serviceUUID + ", pipeline=" + Arrays.toString(pipeline));

        this.prior = prior;

        this.next = next;

    }

    @Override
    public int replicationFactor() {
        return pipeline.length;
    }

    @Override
    public long token() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long lastValidToken() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isHighlyAvailable() {
        return replicationFactor() > 1;
    }

    @Override
    public boolean isQuorumMet() {
        // TODO Auto-generated method stub
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
        return null;
    }

    @Override
    public UUID[] getPipeline() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UUID getLastInPipeline() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UUID[] getPipelinePriorAndNext(UUID serviceId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long awaitQuorum() throws InterruptedException,
            AsynchronousQuorumCloseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long awaitQuorum(long timeout, TimeUnit units)
            throws InterruptedException, AsynchronousQuorumCloseException,
            TimeoutException {
        // TODO Auto-generated method stub
        return 0;
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