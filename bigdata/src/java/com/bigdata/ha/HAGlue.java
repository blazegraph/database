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
package com.bigdata.ha;

import java.io.IOException;
import java.rmi.Remote;
import java.util.concurrent.Future;

import com.bigdata.ha.msg.IHARootBlockRequest;
import com.bigdata.ha.msg.IHARootBlockResponse;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ITransactionService;
import com.bigdata.service.IService;

/**
 * A {@link Remote} interface for methods supporting high availability for a set
 * of journals or data services having shared persistent state.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Scale-out needs to add {@link AbstractJournal#closeForWrites(long)} to
 *       this API and reconcile it with the methods to send index segments (and
 *       historical journal files) across the wire. Perhaps we need both an
 *       HAJournalGlue and an HADataServiceGlue interface?
 * 
 * @todo The naming convention for these interfaces should be changed to follow
 *       the standard jini smart proxy naming pattern.
 */
public interface HAGlue extends HAGlueBase, HAPipelineGlue, HAReadGlue,
        HACommitGlue, ITransactionService, IService {

    /*
     * Administrative
     * 
     * @todo Move to an HAAdminGlue interface?
     */

    /**
     * This method may be issued to force the service to close and then reopen
     * its zookeeper connection. This is a drastic action which will cause all
     * <i>ephemeral</i> tokens for that service to be retracted from zookeeper.
     * When the service reconnects, it will reestablish those connections.
     * 
     * @todo Good idea? Bad idea?
     * 
     * @see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4
     */
    public Future<Void> bounceZookeeperConnection() throws IOException;
    
    /*
     * Synchronization.
     * 
     * Various methods to support synchronization between services as they join
     * a quorum.
     * 
     * @todo Move to an HASyncGlue interface?
     */

    /**
     * Return a root block for the persistence store. The initial root blocks
     * are identical, so this may be used to create a new journal in a quorum by
     * replicating the root blocks of the quorum leader.
     * 
     * @param msg
     *            The message requesting the root block.
     * 
     * @return The root block.
     */
    IHARootBlockResponse getRootBlock(IHARootBlockRequest msg)
            throws IOException;

    /**
     * The port that the NanoSparqlServer is running on.
     */
    int getNSSPort() throws IOException;
    
}
