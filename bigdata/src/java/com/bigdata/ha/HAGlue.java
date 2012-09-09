package com.bigdata.ha;

import java.io.IOException;
import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.Future;

import com.bigdata.journal.AbstractJournal;
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
        HACommitGlue, IService {

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
     * @param storeId
     *            The {@link UUID} of the journal whose root block will be
     *            returned (optional, defaults to the current Journal). This
     *            parameter is intended for scale-out if there is a need to
     *            fetch the root block of a historical journal (versus the live
     *            journal).
     * 
     * @return The root block.
     */
    byte[] getRootBlock(UUID storeId) throws IOException;

}
