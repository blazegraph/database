package com.bigdata.rdf.sail.webapp;

import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.service.jini.JiniFederation;

/**
 * Task creates a KB for the given namespace iff no such KB exists.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class CreateKBTask implements Callable<Void> {

    private static final transient Logger log = Logger
            .getLogger(BigdataRDFServletContextListener.class);

    private final IIndexManager indexManager;
    private final String namespace;

    public CreateKBTask(final IIndexManager indexManager,
            final String namespace) {

        this.indexManager = indexManager;
        this.namespace = namespace;

    }

    public Void call() throws Exception {

        try {
            
            doRun();
            
        } catch (Throwable t) {

            log.error(t, t);

            throw new Exception(t);
            
        }

        return null;

    }

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

                final long token;
                try {
                    log.warn("Awaiting quorum.");
                    token = quorum.awaitQuorum();
                } catch (AsynchronousQuorumCloseException e1) {
                    throw new RuntimeException(e1);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
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

                    log.warn("Creating KB instance: namespace=" + namespace);

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

                log.warn("Creating KB instance: namespace=" + namespace);

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