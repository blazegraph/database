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
/*
 * Created on Apr 13, 2011
 */
package com.bigdata.rdf.sail;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.rdf.sail.BigdataSail.UnisolatedCallable;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.rdf.task.AbstractApiTask;
import com.bigdata.util.InnerCause;

/**
 * Task creates a KB for the given namespace iff no such KB exists. The correct
 * use of this class is as follows:
 * 
 * <pre>
 * AbstractApiTask.submitApiTask(indexManager, new CreateKBTask(namespace, properties)).get();
 * </pre>
 * 
 * @see DestroyKBTask
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class CreateKBTask extends AbstractApiTask<Void> {

    private static final transient Logger log = Logger.getLogger(CreateKBTask.class);
    private static final transient Logger txLog = Logger.getLogger("com.bigdata.txLog");
    
    /**
     * The effective properties that will be used to create the namespace.
     */
    private final Properties properties;
    
    /**
     * Return the effective properties that will be used to create the namespace.
     */
    protected Properties getProperties() {
       return properties;
    }

   public CreateKBTask(final String namespace, final Properties properties) {

      super(namespace, ITx.UNISOLATED, true/* isGRSRequired */);

      if (properties == null)
         throw new IllegalArgumentException();

      // Use the caller's properties as the default.
      this.properties = new Properties(properties);

      // override the namespace.
      this.properties.setProperty(BigdataSail.Options.NAMESPACE, namespace);

   }

   @Override
   final public boolean isReadOnly() {
      return false;
   }
    
    @Override
    public Void call() throws Exception {

        try {
            
            doRun();
            
        } catch (Throwable t) {

            if (InnerCause.isInnerCause(t, AsynchronousQuorumCloseException.class)) {

                /*
                 * The quorum is closed, so we stopped trying.
                 * 
                 * Note: This can also happen if the quorum has not been started
                 * yet. The HAJournalServer explicitly invokes the CreateKBTask
                 * when entering "RunMet" in order to handle this case.
                 */
                log.warn(t);

            } else {

                log.error(t, t);

            }

            throw new Exception(t);
            
        }

        return null;

    }

    /**
     * Note: This process is not robust if the leader is elected and becomes
     * HAReady and then fails over before the KB is created. The task should be
     * re-submitted by the new leader once that leader is elected.
     * 
     * @throws Exception 
     */
    private void doRun() throws Exception {
    
       final IIndexManager indexManager = getIndexManager();
       
        if (indexManager instanceof IJournal) {

            /*
             * Create a local triple store.
             * 
             * Note: This hands over the logic to some custom code located
             * on the BigdataSail.
             */

            final IJournal jnl = (IJournal) indexManager;

            final Quorum<HAGlue, QuorumService<HAGlue>> quorum = jnl
                    .getQuorum();

            boolean isSoloOrLeader;

            if (quorum == null) {

                isSoloOrLeader = true;

            } else {

                /*
                 * Wait for a quorum meet.
                 */
                final long token;
                try {
                    long tmp = quorum.token();
                    if (tmp == Quorum.NO_QUORUM) {
                        // Only log if we are going to wait.
                        log.warn("Awaiting quorum.");
                        tmp = quorum.awaitQuorum();
                    }
                    token = tmp;
                    assert token != Quorum.NO_QUORUM;
                } catch (AsynchronousQuorumCloseException e1) {
                    throw new RuntimeException(e1);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }

                /*
                 * Now wait until the service is HAReady.
                 */
                try {
                    jnl.awaitHAReady(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                } catch (AsynchronousQuorumCloseException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
                
                if (quorum.getMember().isLeader(token)) {
                    isSoloOrLeader = true;
                } else {
                    isSoloOrLeader = false;
                }
                
                final IJournal journal = jnl;
                if (journal.isGroupCommit()
                  && journal.getRootBlockView().getCommitCounter() == 0L) {
                  /*
                   * Force the GRS to be materialized. This is necessary for the
                   * initial KB create when using group commit and HA. (For HA the
                   * initial KB create is single threaded within the context of the
                   * leader election. However, this is not true for a standalone
                   * Journal.)
                   * 
                   * Note: This logic will fail if AbstractTask uses a
                   * DefaultResourceLocator that is based on the HAJournal and not
                   * on an IsolatedActionJournal because that will allow the
                   * GlobalRowStoreHelper.getGlobalRowStore() method to registerr
                   * the GSR index on the unisolated Name2Addr rather than the n2a
                   * class inside of the AbstractTask.
                   */
                  journal.getGlobalRowStore();
                  journal.commit();
               }
              
            }

            if (isSoloOrLeader) {

                /*
                 * Note: createLTS() writes on the GRS. This is an atomic row
                 * store. The change is automatically committed. The unisolated
                 * view of the BigdataSailConnection is being used solely to
                 * have the correct locks.
                 * 
                 * @see BLZG-2023, BLZG-2041
                 */
                // Wrap with SAIL.
                final BigdataSail sail = new BigdataSail(namespace, getIndexManager());
                try {
                    sail.initialize();
                    final UnisolatedCallable<Void> task = new UnisolatedCallable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            if (!sail.exists()) {
 
                    // create the appropriate as configured triple/quad store.
                    createLTS(jnl, getProperties());

                                if (txLog.isInfoEnabled())
                                    txLog.info("SAIL-CREATE-NAMESPACE: namespace=" + namespace); // FIXME BLZG-2041 document on wiki
                            }
                            return null;
                        }
                    };
                    try {
                        // Do work with appropriate locks are held.
                        sail.getUnisolatedConnectionLocksAndRunLambda(task);
                    } finally {
                        // Release locks.
                        task.releaseLocks();
                    }
                } finally {
                    sail.shutDown();
                }

            }

        } else {

         // Attempt to resolve the namespace.
             if (indexManager.getResourceLocator().locate(namespace, ITx.UNISOLATED) == null) {
    
            /*
             * Register triple store for scale-out.
                 * 
                 * Note: Scale-out does not have a global lock.
             */
    
            if (log.isInfoEnabled())
               log.info("Creating KB instance: namespace=" + namespace);
    
            final ScaleOutTripleStore lts = new ScaleOutTripleStore(
                  indexManager, namespace, ITx.UNISOLATED, getProperties());
    
                // Note: Commit in scale-out is shard-wise acid.
            lts.create();
    
                if (txLog.isInfoEnabled())
                   txLog.info("CREATE: namespace=" + namespace);

         } // if( tripleStore == null )

      }

   }

   /**
    * Create an {@link AbstractTripleStore} instance against a local database.
    * <p>
    * Note: For group commit, the caller will be holding the resource lock for
    * the namespace.
    * 
    * @param indexManager
    * @param properties
     * 
     * @throws IOException
    */
    private void createLTS(final IJournal indexManager, final Properties properties) throws IOException {
       
      final String namespace = properties.getProperty(
            BigdataSail.Options.NAMESPACE,
            BigdataSail.Options.DEFAULT_NAMESPACE);

      // throws an exception if there are inconsistent properties
      BigdataSail.checkProperties(properties);
      
//      /**
//       * Note: Unless group commit is enabled, we need to make this operation
//       * mutually exclusive with KB level writers in order to avoid the
//       * possibility of a triggering a commit during the middle of a
//       * BigdataSailConnection level operation (or visa versa).
//       * 
//       * Note: When group commit is not enabled, the indexManager will be a
//       * Journal class. When it is enabled, it will merely implement the
//       * IJournal interface.
//       * 
//       * @see #1143 (Isolation broken in NSS when groupCommit disabled)
//       */
//      final boolean isGroupCommit = indexManager.isGroupCommit();
//      boolean acquiredConnection = false;
//      try {
//
//         if (!isGroupCommit) {
//            try {
//               // acquire the unisolated connection permit.
//               ((Journal) indexManager).acquireUnisolatedConnection();
//               acquiredConnection = true;
//            } catch (InterruptedException e) {
//               throw new RuntimeException(e);
//            }
//         }

//      Note: Already checked by the caller.
//      
//         // Check for pre-existing instance.
//         {
//
//            final LocalTripleStore lts = (LocalTripleStore) indexManager
//                  .getResourceLocator().locate(namespace, ITx.UNISOLATED);
//
//            if (lts != null) {
//
//               return;
//
//            }
//
//         }

         // Create a new instance.
         {

            if (Boolean.parseBoolean(properties.getProperty(
                  BigdataSail.Options.ISOLATABLE_INDICES,
                  BigdataSail.Options.DEFAULT_ISOLATABLE_INDICES))) {

               /*
                * Isolatable indices: requires the use of a tx to create the KB
                * instance.
                * 
                * FIXME BLZG-2041: Verify test coverage of this code path.
                */

               final ITransactionService txService = indexManager
                        .getLocalTransactionManager().getTransactionService();

               final long txIdCreate = txService.newTx(ITx.UNISOLATED);

               boolean ok = false;
               try {

                  final AbstractTripleStore txCreateView = new LocalTripleStore(
                        indexManager, namespace, Long.valueOf(txIdCreate),
                        properties);

                  // create the kb instance within the tx.
                  txCreateView.create();

                  // commit the tx.
                  txService.commit(txIdCreate);

                  ok = true;

               } finally {

                  if (!ok)
                     txService.abort(txIdCreate);

               }

            } else {

               /*
                * Create KB without isolatable indices.
                */

               final LocalTripleStore lts = new LocalTripleStore(indexManager,
                     namespace, ITx.UNISOLATED, properties);

               lts.create();

            }

         }

//         /*
//          * Now that we have created the instance, either using a tx or the
//          * unisolated connection, locate the triple store resource and return
//          * it.
//          */
//         {
//
//            final LocalTripleStore lts = (LocalTripleStore) indexManager
//                  .getResourceLocator().locate(namespace, ITx.UNISOLATED);
//
//            if (lts == null) {
//
//               /*
//                * This should only occur if there is a concurrent destroy, which
//                * is highly unlikely to say the least.
//                */
//               throw new RuntimeException("Concurrent create/destroy: "
//                     + namespace);
//
//            }
//
//            return;
//
//         }

//      } catch (IOException ex) {
//
//         throw new RuntimeException(ex);
//
//      } finally {
//
//         if (!isGroupCommit && acquiredConnection) {
//
//            /**
//             * When group commit is not enabled, we need to release the
//             * unisolated connection.
//             * 
//             * @see #1143 (Isolation broken in NSS when groupCommit disabled)
//             */
//            ((Journal) indexManager).releaseUnisolatedConnection();
//
//         }
//
//      }

   }

}
