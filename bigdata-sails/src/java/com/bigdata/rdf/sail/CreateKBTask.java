/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
/*
 * Created on Apr 13, 2011
 */
package com.bigdata.rdf.sail;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.util.InnerCause;

/**
 * Task creates a KB for the given namespace iff no such KB exists.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class CreateKBTask implements Callable<Void> {

    private static final transient Logger log = Logger
            .getLogger(CreateKBTask.class);

    private final IIndexManager indexManager;
    private final String namespace;

    public CreateKBTask(final IIndexManager indexManager,
            final String namespace) {

        this.indexManager = indexManager;
        this.namespace = namespace;

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
     */
    private void doRun() {
    
        if (indexManager instanceof AbstractJournal) {

            /*
             * Create a local triple store.
             * 
             * Note: This hands over the logic to some custom code located
             * on the BigdataSail.
             */

            final Journal jnl = (Journal) indexManager;

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
            }

            if (isSoloOrLeader) {

                // Attempt to resolve the namespace.
                if (indexManager.getResourceLocator().locate(namespace,
                        ITx.UNISOLATED) == null) {

                    if(log.isInfoEnabled())
                    	log.info("Creating KB instance: namespace=" + namespace);

                    final Properties properties = new Properties(
                            jnl.getProperties());

                    // override the namespace.
                    properties.setProperty(BigdataSail.Options.NAMESPACE,
                            namespace);

                    // create the appropriate as configured triple/quad
                    // store.
                    BigdataSail.createLTS(jnl, properties);

                } // if( tripleStore == null )

            }

        } else {

            // Attempt to resolve the namespace.
            if (indexManager.getResourceLocator().locate(namespace,
                    ITx.UNISOLATED) == null) {

                /*
                 * Register triple store for scale-out.
                 */

				if (log.isInfoEnabled())
					log.info("Creating KB instance: namespace=" + namespace);

                final JiniFederation<?> fed = (JiniFederation<?>) indexManager;

                final Properties properties = fed.getClient()
                        .getProperties();

                final ScaleOutTripleStore lts = new ScaleOutTripleStore(
                        indexManager, namespace, ITx.UNISOLATED, properties);

                lts.create();

            } // if( tripleStore == null )

        }

    }
    
}