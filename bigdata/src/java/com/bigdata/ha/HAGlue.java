package com.bigdata.ha;

import java.io.IOException;
import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.Future;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IRootBlockView;

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
        HACommitGlue {

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
     */
    public Future<Void> bounceZookeeperConnection();
    
    /*
     * Synchronization.
     * 
     * Various methods to support synchronization between services as they join
     * a quorum.
     * 
     * @todo Move to an HASyncGlue interface?
     */

    /**
     * Return the current root block for the persistence store. The initial root
     * blocks are identical, so this may be used to create a new journal in a
     * quorum by replicating the root blocks of the quorum leader.
     * 
     * @param storeId
     *            The {@link UUID} of the journal whose current root block will
     *            be returned.
     * 
     * @return The current root block.
     * 
     * @todo Whether or not we send the rootBlock0 flag depends on whether or
     *       not resynchronization guarantees that the root blocks (both of
     *       them) are the same for all services in the quorum.
     */
    byte[] getRootBlock(UUID storeId) throws IOException;

}
