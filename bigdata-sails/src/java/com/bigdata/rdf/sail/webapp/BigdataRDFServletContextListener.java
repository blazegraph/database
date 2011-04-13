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

package com.bigdata.rdf.sail.webapp;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;

import com.bigdata.Banner;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.jini.JiniClient;

/**
 * Listener provides life cycle management of the {@link IIndexManager} by
 * interpreting the configuration parameters in the {@link ServletContext}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataRDFServletContextListener implements
        ServletContextListener {

    static private final transient Logger log = Logger
            .getLogger(BigdataRDFServletContextListener.class);

    private Journal jnl = null;
    private JiniClient<?> jiniClient = null;
    private ITransactionService txs = null;
    private Long readLock = null;
    private long readLockTx;

    /**
     * <code>true</code> iff this class opened the {@link IIndexManager}, in
     * which case it will close it at the appropriate life cycle event.
     */
    private boolean closeIndexManager;
    
    public void contextInitialized(final ServletContextEvent e) {

        if(log.isInfoEnabled())
            log.info("");

        Banner.banner();
        
        final ServletContext context = e.getServletContext();

        final String namespace;
        {
         
            namespace = context.getInitParameter(ConfigParams.NAMESPACE);

            if (namespace == null)
                throw new RuntimeException("Required: "
                        + ConfigParams.NAMESPACE);

            if (log.isInfoEnabled())
                log.info("namespace: " + namespace);

        }

        final IIndexManager indexManager;
        if (context.getAttribute(IIndexManager.class.getName()) != null) {

            /*
             * The index manager object was directly set by the caller.
             */
            
            indexManager = (IIndexManager) context
                    .getAttribute(IIndexManager.class.getName());

            // the caller is responsible for the life cycle.
            closeIndexManager = false;
            
        } else {

            /*
             * The index manager will be open based on the specified property
             * file or config file.
             */
            
            final String propertyFile = context
                    .getInitParameter(ConfigParams.PROPERTY_FILE);

            if (propertyFile == null)
                throw new RuntimeException("Required config-param: "
                        + ConfigParams.PROPERTY_FILE);

            if (log.isInfoEnabled())
                log.info("propertyFile: " + propertyFile);

            indexManager = openIndexManager(propertyFile);
            
            // we are responsible for the life cycle.
            closeIndexManager = false;

        }

        txs = (indexManager instanceof Journal ? ((Journal) indexManager).getTransactionManager()
                .getTransactionService() : ((IBigdataFederation<?>) indexManager).getTransactionService());

        final long timestamp;
        {
        
            final String s = context.getInitParameter(ConfigParams.READ_LOCK);
            
            readLock = s == null ? null : Long.valueOf(s);
            
            if (readLock != null) {

                /*
                 * Obtain a read-only transaction which will assert a read lock
                 * for the specified commit time. The database WILL NOT release
                 * storage associated with the specified commit point while this
                 * server is running. Queries will read against the specified
                 * commit time by default, but this may be overridden on a query
                 * by query basis.
                 */

                try {

                    timestamp = txs.newTx(readLock);

                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

                log.warn("Holding read lock: readLock=" + readLock + ", tx: "
                        + timestamp);

            } else {

                /*
                 * The default for queries is to read against then most recent
                 * commit time as of the moment when the request is accepted.
                 */

                timestamp = ITx.READ_COMMITTED;

            }

        }

        final int queryThreadPoolSize;
        {

            final String s = context
                    .getInitParameter(ConfigParams.QUERY_THREAD_POOL_SIZE);

            queryThreadPoolSize = s == null ? ConfigParams.DEFAULT_QUERY_THREAD_POOL_SIZE
                    : Integer.valueOf(s);

            if (queryThreadPoolSize < 0) {

                throw new RuntimeException(ConfigParams.QUERY_THREAD_POOL_SIZE
                        + " : Must be non-negative, not: " + s);

            }

        }

        final SparqlEndpointConfig config = new SparqlEndpointConfig(
                namespace,timestamp,queryThreadPoolSize
                );

//        if (log.isInfoEnabled()) {
            /*
             * Log some information about the default kb (#of statements, etc).
             */
            // FIXME log.info("\n" + server.getKBInfo(config.namespace,
            // config.timestamp));
        // }

        {
        
            final boolean forceOverflow = Boolean.valueOf(context
                    .getInitParameter(ConfigParams.FORCE_OVERFLOW));

            if (forceOverflow && indexManager instanceof IBigdataFederation<?>) {

                log.warn("Forcing compacting merge of all data services: "
                        + new Date());

                ((AbstractDistributedFederation<?>) indexManager)
                        .forceOverflow(true/* compactingMerge */, false/* truncateJournal */);

                log.warn("Did compacting merge of all data services: "
                        + new Date());

            }

        }

        // Used by BigdataBaseServlet
        context.setAttribute(IIndexManager.class.getName(), indexManager);

        // Used by BigdataRDFBaseServlet
        context.setAttribute(BigdataRDFContext.class.getName(),
                new BigdataRDFContext(config, indexManager));

        if (log.isInfoEnabled())
            log.info("done");

    }

    public void contextDestroyed(final ServletContextEvent e) {

        if(log.isInfoEnabled())
            log.info("");
        
        if (txs != null && readLock != null) {

            try {
            
                txs.abort(readLockTx);
                
            } catch (IOException ex) {
                
                log
                        .error("Could not release transaction: tx="
                                + readLockTx, ex);
            
            }
        }

        if (jnl != null) {

            if (closeIndexManager)
                jnl.close();
            
            jnl = null;
            
        }
        
        if (jiniClient != null) {
        
            if (closeIndexManager)
                jiniClient.disconnect(true/* immediateShutdown */);
            
            jiniClient = null;
            
        }

    }

    /**
     * Open the {@link IIndexManager} identified by the property file.
     * 
     * @param propertyFile
     *            The property file (for a standalone bigdata instance) or the
     *            jini configuration file (for a bigdata federation). The file
     *            must end with either ".properties" or ".config".
     *            
     * @return The {@link IIndexManager}.
     */
    private IIndexManager openIndexManager(final String propertyFile) {

        final File file = new File(propertyFile);

        if (!file.exists()) {

            throw new RuntimeException("Could not find file: " + file);

        }

        boolean isJini = false;
        if (propertyFile.endsWith(".config")) {
            // scale-out.
            isJini = true;
        } else if (propertyFile.endsWith(".properties")) {
            // local journal.
            isJini = false;
        } else {
            /*
             * Note: This is a hack, but we are recognizing the jini
             * configuration file with a .config extension and the journal
             * properties file with a .properties extension.
             */
            throw new RuntimeException(
                    "File must have '.config' or '.properties' extension: "
                            + file);
        }

        final IIndexManager indexManager;
        try {

            if (isJini) {

                /*
                 * A bigdata federation.
                 */

                jiniClient = new JiniClient(new String[] { propertyFile });

                indexManager = jiniClient.connect();

            } else {

                /*
                 * Note: we only need to specify the FILE when re-opening a
                 * journal containing a pre-existing KB.
                 */
                final Properties properties = new Properties();
                {
                    // Read the properties from the file.
                    final InputStream is = new BufferedInputStream(
                            new FileInputStream(propertyFile));
                    try {
                        properties.load(is);
                    } finally {
                        is.close();
                    }
                    if (System.getProperty(BigdataSail.Options.FILE) != null) {
                        // Override/set from the environment.
                        properties.setProperty(BigdataSail.Options.FILE, System
                                .getProperty(BigdataSail.Options.FILE));
                    }
                }

                indexManager = jnl = new Journal(properties);

            }

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return indexManager;
        
    }
        
}
