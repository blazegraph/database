/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.bigdata.ha.FutureTaskInvariantMon;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.IndexManagerCallable;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.quorum.Quorum;

/**
 * The test-based implementation which allows us to run an invariant listener
 * remotely. When the task returns <code>true</code> it will mean that the
 * invariant has failed. You have to provide an implementation for the
 * {@link #establishInvariants(FutureTaskInvariantMon, Quorum)} method.
 * <p>
 * Note: The implementations must be serializable, so they can not be anonymous
 * inner classes.
 * <p>
 * Note: The normal usage patterns for the {@link FutureTaskInvariantMon} do not
 * require the submission of a remote task - instead the invariant monitor is
 * established by the service itself, which significantly simplifies the code
 * patterns.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
@SuppressWarnings("serial")
public abstract class InvariantTask extends IndexManagerCallable<Boolean> {

    /**
     * Logger for HA events.
     */
    private static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

    @Override
	public Boolean call() throws Exception {
		final AbstractJournal journal = (AbstractJournal) getIndexManager();
		final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
				.getQuorum();

		// slave task synchronizes and communicates with AtomicBoolean
		final AtomicBoolean val = new AtomicBoolean(false);

		final Callable<Boolean> slave = new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				if (haLog.isInfoEnabled())
					haLog.info("Started slave task");
				
				synchronized (val) {					
					try {
						while (true) {
							Thread.sleep(100);
						}
					} finally {
						val.set(true);
					}
				}
			}

		};

        /*
         * Create a that on the remote service. When submitted, this task will
         * monitor the invariant. The invariant to be monitored is established
         * when the task begins to execute.
         */
		final FutureTask<Boolean> ft = new FutureTaskInvariantMon<Boolean>(
				slave, quorum) {

			@Override
			protected void establishInvariants() {
				InvariantTask.this.establishInvariants(this,
						quorum);
			}

		};

		journal.getExecutorService().submit(ft);

		try {
			ft.get();
		} catch (CancellationException ce) {
			// set here on cancellation!
			val.set(true);
		}

		// co-operative synchronization ensures task completion.
		synchronized (val) {
			
			if (haLog.isInfoEnabled())
				haLog.info("InvariantTask returning: " + val.get());

			return val.get();
		}
	}

    /**
     * 
     * @param task
     * @param quorum
     */
    abstract protected void establishInvariants(
            FutureTaskInvariantMon<Boolean> task,
            Quorum<HAGlue, QuorumService<HAGlue>> quorum);
	
	static class ServiceJoined extends InvariantTask {

		UUID serviceId;
		
		public ServiceJoined(UUID serviceId) {
			this.serviceId = serviceId;
		}

		@Override
		protected void establishInvariants(FutureTaskInvariantMon<Boolean> task, Quorum<HAGlue, QuorumService<HAGlue>> quorum) {
			task.assertJoined(serviceId);
		}
	}


	static class ServiceDoesntJoin extends InvariantTask {
		UUID serviceId;
		
		public ServiceDoesntJoin(UUID serviceId) {
			this.serviceId = serviceId;
		}

		@Override
		protected void establishInvariants(FutureTaskInvariantMon<Boolean> task, Quorum<HAGlue, QuorumService<HAGlue>> quorum) {
			task.assertNotJoined(serviceId);
		}
	}


	static class QuorumMet extends InvariantTask {

		@Override
		protected void establishInvariants(FutureTaskInvariantMon<Boolean> task, Quorum<HAGlue, QuorumService<HAGlue>> quorum) {
			task.assertQuorumMet();
		}
	}

}
