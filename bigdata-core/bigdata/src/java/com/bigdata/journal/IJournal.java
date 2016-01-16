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
package com.bigdata.journal;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.rawstore.IMRMW;

/**
 * An persistence capable data structure supporting atomic commit, scalable
 * named indices, and transactions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IJournal extends IMRMW, IAtomicStore, IBTreeManager {

    /**
     * A copy of the properties used to initialize this journal.
     */
    public Properties getProperties();
    
    /**
     * Shutdown the journal politely. Scheduled operations will run to
     * completion, but no new operations will be scheduled.
     */
    public void shutdown();

    /**
     * Immediate shutdown.
     */
    public void shutdownNow();

	/**
	 * Return the object providing the local transaction manager for this
	 * journal.
	 */
	public ILocalTransactionManager getLocalTransactionManager();

   /**
    * The {@link Quorum} for this service -or- <code>null</code> if the service
    * is not running with a quorum.
    */
   Quorum<HAGlue, QuorumService<HAGlue>> getQuorum();

   /**
    * Await the service being ready to partitipate in an HA quorum. The
    * preconditions include:
    * <ol>
    * <li>receiving notice of the quorum token via {@link #setQuorumToken(long)}
    * </li>
    * <li>The service is joined with the met quorum for that token</li>
    * <li>If the service is a follower and it's local root blocks were at
    * <code>commitCounter:=0</code>, then the root blocks from the leader have
    * been installed on the follower.</li>
    * <ol>
    * 
    * @param timeout
    *           The timeout to await this condition.
    * @param units
    *           The units for that timeout.
    * 
    * @return the quorum token for which the service became HA ready.
    */
   long awaitHAReady(final long timeout, final TimeUnit units)
         throws InterruptedException, TimeoutException,
         AsynchronousQuorumCloseException;
   
   /**
    * Convenience method created in BLZG-1370 to factor out bigdata-jini
    * artifact dependencies.
    * 
    * This should return true IFF the underlying journal is com.bigdata.jini.ha.HAJournal. 
    * 
    * @return
    */
   public boolean isHAJournal();

  

}
