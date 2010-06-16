package com.bigdata.ha;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.quorum.QuorumMember;
import com.bigdata.quorum.QuorumStateChangeListenerBase;
import com.bigdata.rawstore.IRawStore;

/**
 * {@link QuorumRead} implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <S>
 */
public class QuorumReadImpl<S extends HAReadGlue> extends
        QuorumStateChangeListenerBase implements QuorumRead<S> {

    static protected transient final Logger log = Logger
            .getLogger(QuorumReadImpl.class);

    protected final QuorumMember<S> member;

    public QuorumReadImpl(final QuorumMember<S> member) {
        
        this.member = member;
        
    }
    
    /**
     * Used to implement a round-robin policy.
     */
    private final AtomicInteger nextIndex = new AtomicInteger();

    /**
     * Return the {@link UUID} of the joined service to which this service
     * will direct a failover read. The default implementation uses a
     * round-robin policy.
     * 
     * @param joinedServiceIds
     *            The {@link UUID}s of the services currently joined with
     *            the quorum.
     * 
     * @return The {@link UUID} of the service to which the failover read
     *         will be directed.
     */
    protected UUID getNextBadReadServiceId(final UUID[] joinedServiceIds) {

        // This service.
        final UUID serviceId = member.getServiceId();

        /*
         * Note: This loop will always terminate if the quorum is joined
         * since there will be (k+1)/2 services on which we can read, which
         * is a minimum of one service which is not _this_ service even when
         * k := 3.
         */

        while (true) {

            final int i = nextIndex.incrementAndGet()
                    % joinedServiceIds.length;

            if (serviceId.equals(joinedServiceIds[i])) {

                // Do not issue the call to ourselves.
                continue;

            }

            // Issue the call to this service.
            return joinedServiceIds[i];

        }

    }

    /**
     * {@inheritDoc}
     * 
     * @todo If this blocks awaiting a quorum, then make sure that it is not
     *       invoked in a context where it is holding a lock on the local
     *       low-level store!
     */
    public byte[] readFromQuorum(final UUID storeId, final long addr)
            throws InterruptedException, IOException {

        if (storeId == null)
            throw new IllegalArgumentException();

        if (addr == IRawStore.NULL)
            throw new IllegalArgumentException();

        if (!member.getQuorum().isHighlyAvailable()) {

            // This service is not configured for high availability.
            throw new IllegalStateException();

        }

        // @todo monitoring hook (Nagios)?
        //
        // @todo counters (in WORMStrategy right now).
        if (log.isInfoEnabled())
            log.info("Failover read: storeId=" + storeId + ", addr=" + addr);

        // Block if necessary awaiting a met quorum.
        final long token = member.getQuorum().awaitQuorum();

        final UUID[] joinedServiceIds = member.getQuorum().getJoinedMembers();

        // Figure out which other joined service we will read on.
        final UUID otherId = getNextBadReadServiceId(joinedServiceIds);

        // The RMI interface for the service on which we will read.
        final S otherService = member.getService(otherId);

        /*
         * Read from that service. The request runs in the caller's thread.
         */
        try {

            final Future<byte[]> rf = otherService.readFromDisk(token,
                    storeId, addr);

            final byte[] a = rf.get();

            // Verify quorum is still valid.
            member.getQuorum().assertQuorum(token);

            return a;

        } catch (ExecutionException e) {

            throw new RuntimeException(e);

        }

    }

}
