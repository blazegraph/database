/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.ha;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumEvent;
import com.bigdata.quorum.QuorumEventEnum;
import com.bigdata.quorum.QuorumListener;
import com.bigdata.util.StackInfoReport;

/**
 * A {@link Future} that allows you to cancel a computation if an invariant is
 * violated. This class is specifically designed to monitor quorum related
 * invariants for HA.
 * <p>
 * Once an invariant is established, listening for the relevant quorum events
 * commences and a check is made to verify that the invariant holds on entry.
 * This pattern ensures there is no possibility of a missed event.
 * <p>
 * This {@link FutureTask} wrapper will return the value of the {@link Callable}
 * (or the specified result for the {@link Runnable}) iff the task completes
 * successfully without the invariant being violated.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <T>
 *            The generic type of the future.
 */
public abstract class FutureTaskInvariantMon<T> extends FutureTaskMon<T>
        implements QuorumListener {

    private static final Logger log = Logger.getLogger(FutureTaskInvariantMon.class);

    private final Quorum<HAGlue, QuorumService<HAGlue>> m_quorum;
    /**
     * The quorum token on entry.
     */
    private final long token;

    private final List<QuorumEventInvariant> m_triggers = new CopyOnWriteArrayList<QuorumEventInvariant>();

    public FutureTaskInvariantMon(final Callable<T> callable,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum) {

	    super(callable);
		
        if (quorum == null)
            throw new IllegalArgumentException();
	    
		m_quorum = quorum;
		
		token = quorum.token();
		
	}

    public FutureTaskInvariantMon(final Runnable runnable, final T result,
            Quorum<HAGlue, QuorumService<HAGlue>> quorum) {

        super(runnable, result);

        if (quorum == null)
            throw new IllegalArgumentException();

        m_quorum = quorum;
        
        token = quorum.token();
        
    }

    /**
     * Concrete implementations use this callback hook to establish the
     * invariants to be monitored.
     */
	abstract protected void establishInvariants();
	
	/**
	 * {@inheritDoc}
	 * <p>
	 * Hook to manage listener registration and establish invariants.
	 */
	@Override
    public void run() {
        boolean didStart = false;
        m_quorum.addListener(this);
        try {
            establishInvariants();

            didStart = true;
            super.run();
        } finally {
            m_quorum.removeListener(this);
            if (!didStart) {
                /*
                 * Guarantee cancelled unless run() invoked.
                 */
                cancel(true/* mayInterruptIfRunning */);
            }
        }
    }

    /**
     * Establish an invariant that the specified service is a member of the
     * quorum.
     * 
     * @param serviceId
     *            The service.
     */
    public void assertMember(final UUID serviceId) {
        m_triggers.add(new QuorumEventInvariant(QuorumEventEnum.MEMBER_REMOVE,
                serviceId));

        // now check that already a member and break if not
        assertMembership(m_quorum.getMembers(), serviceId);
    }

    /**
     * Establish an invariant that the specified service is joined with the met
     * quorum.
     * 
     * @param serviceId
     *            The service.
     */
    public void assertJoined(final UUID serviceId) {
        m_triggers.add(new QuorumEventInvariant(QuorumEventEnum.SERVICE_LEAVE,
                serviceId));

        // now check that already joined and break if not
        assertMembership(m_quorum.getJoined(), serviceId);
    }

    /**
     * Establish an invariant that the specified service is a not joined with
     * the met quorum.
     * 
     * @param serviceId
     *            The service.
     */
	public void assertNotJoined(final UUID serviceId) {
        m_triggers.add(new QuorumEventInvariant(QuorumEventEnum.SERVICE_JOIN,
                serviceId));

		// now check not already joined and break if it is
		if (isMember(m_quorum.getJoined(), serviceId))
			broken();
	}
	
    /**
     * Establish an invariant that the specified service is in the quorum
     * pipeline.
     * 
     * @param serviceId
     *            The service.
     */
    public void assertInPipeline(final UUID serviceId) {
        m_triggers.add(new QuorumEventInvariant(
                QuorumEventEnum.PIPELINE_REMOVE, serviceId));

        // now check that already in pipeline and break if not
        assertMembership(m_quorum.getPipeline(), serviceId);
    }

    /**
     * Establish an invariant that the quorum is met and remains met on the same
     * token (the one specified to the constructor).
     */
    public void assertQuorumMet() {
        m_triggers.add(new QuorumEventInvariant(QuorumEventEnum.QUORUM_BROKE,
                null/* serviceId */));

        // now check that quorum is met and break if not
        if (!m_quorum.isQuorumMet())
            broken();
        if (m_quorum.token() != token)
            broken();
    }

    /**
     * Establish an invariant that the quorum is fully met.
     */
    public void assertQuorumFullyMet() {
        // no-one must leave!
        m_triggers.add(new QuorumEventInvariant(QuorumEventEnum.SERVICE_LEAVE,
                null/* serviceId */));

		// now check that quorum is fully met on the current token and break if not
		if (!m_quorum.isQuorumFullyMet(m_quorum.token()))
			broken();
	}
	
	private void assertMembership(final UUID[] members, final UUID serviceId) {
		if (isMember(members, serviceId))
			return;
		
		broken();
	}
	
	private boolean isMember(final UUID[] members, final UUID serviceId) {
		for (UUID member : members) {
			if (member.equals(serviceId))
				return true;
		}
		
		return false;
	}

	/**
	 * Any QuorumEvent must be checked to see if it matches an Invariant trigger
	 */
	@Override
	public void notify(final QuorumEvent e) {
		boolean interrupt = false;

		for (QuorumEventInvariant inv : m_triggers) {
			if (inv.matches(e)) {
				interrupt = true;
				break;
			}
		}

		// interrupt the call thread
		if (interrupt) {
			broken();
		} else {
			if (log.isDebugEnabled())
				log.debug("Ignoring event: " + e);
		}
	}
	
	private void broken() {
		log.warn("BROKEN", new StackInfoReport());
		
		cancel(true/*mayInterruptIfRunning*/);
	}

	@SuppressWarnings("serial")
	private class QuorumEventInvariant implements QuorumEvent, Serializable {

	    private final QuorumEventEnum m_qe;
		private final UUID m_sid;

	    /**
	     * 
	     * @param qe
	     *            The {@link QuorumEvent} type (required).
	     * @param sid
	     *            The service {@link UUID} (optional). When <code>null</code>
	     *            the {@link QuorumEventEnum} will be matched for ANY service
	     *            {@link UUID}.
	     */
	    public QuorumEventInvariant(final QuorumEventEnum qe, final UUID sid) {
	        if (qe == null)
	            throw new IllegalArgumentException();
			m_qe = qe;
			m_sid = sid;
		}

		@Override
		public QuorumEventEnum getEventType() {
			return m_qe;
		}

		@Override
		public long lastValidToken() {
			throw new UnsupportedOperationException();
		}

		@Override
		public long token() {
	        throw new UnsupportedOperationException();
		}

		@Override
		public UUID getServiceId() {
			return m_sid;
		}

		@Override
		public long lastCommitTime() {
	        throw new UnsupportedOperationException();
		}
		
		public boolean matches(final QuorumEvent qe) {
			return	qe.getEventType() == m_qe
					&& (m_sid == null || m_sid.equals(qe.getServiceId()));
		}
		
	}

}
