/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 6, 2012
 */

package com.bigdata.rdf.sail.webapp;

import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.eclipse.jetty.server.Server;
import org.openrdf.sail.SailException;

import com.bigdata.BigdataStatics;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.util.config.NicUtil;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <S>
 */
public abstract class AbstractNanoSparqlServerTestCase<S extends IIndexManager>
        extends ProxyTestCase<S> {

    /**
     * The path used to resolve resources in this package when they are being
     * uploaded to the {@link NanoSparqlServer}.
     */
    protected static final String packagePath = "bigdata-sails/src/test/com/bigdata/rdf/sail/webapp/";

    /**
     * A jetty {@link Server} running a {@link NanoSparqlServer} instance.
     */
    protected Server m_fixture;

    /**
     * The namespace of the {@link AbstractTripleStore} instance against which
     * the test is running. A unique namespace is used for each test run, but
     * the namespace is based on the test name.
     */
    protected String namespace;

    /**
     * The effective {@link NanoSparqlServer} http end point (including the
     * ContextPath).
     */
    protected String m_serviceURL;

    protected TestMode testMode = null;

    /**
     * 
     */
    public AbstractNanoSparqlServerTestCase() {
        super();
    }
    
    /**
     * @param name
     */
    public AbstractNanoSparqlServerTestCase(String name) {
        super(name);
    }
    
    protected AbstractTripleStore createTripleStore(
            final IIndexManager indexManager, final String namespace,
            final Properties properties) {
        
    	if(log.isInfoEnabled())
    		log.info("KB namespace=" + namespace);
    
    	// Locate the resource declaration (aka "open"). This tells us if it
    	// exists already.
    	AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
    			.getResourceLocator().locate(namespace, ITx.UNISOLATED);
    
    	if (tripleStore != null) {
    
    		fail("exists: " + namespace);
    		
    	}
    
    	/*
    	 * Create the KB instance.
    	 */
    
    	if (log.isInfoEnabled()) {
    		log.info("Creating KB instance: namespace="+namespace);
    		log.info("Properties=" + properties.toString());
    	}
    
    	if (indexManager instanceof Journal) {
    
            // Create the kb instance.
    		tripleStore = new LocalTripleStore(indexManager, namespace,
    				ITx.UNISOLATED, properties);
    
    	} else {
    
    		tripleStore = new ScaleOutTripleStore(indexManager, namespace,
    				ITx.UNISOLATED, properties);
    	}
    
        // create the triple store.
        tripleStore.create();
    
        if(log.isInfoEnabled())
        	log.info("Created tripleStore: " + namespace);
    
        // New KB instance was created.
        return tripleStore;
    
    }

    protected void dropTripleStore(final IIndexManager indexManager,
            final String namespace) {

        if (log.isInfoEnabled())
            log.info("KB namespace=" + namespace);
    
    	// Locate the resource declaration (aka "open"). This tells us if it
    	// exists already.
        final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
                .getResourceLocator().locate(namespace, ITx.UNISOLATED);
    
    	if (tripleStore != null) {
    
    		if (log.isInfoEnabled())
    			log.info("Destroying: " + namespace);
    
    		tripleStore.destroy();
    		
    	}
    
    }

    @Override
    public void setUp() throws Exception {
        
    	super.setUp();
    
    	log.warn("Setting up test:" + getName());
    	
    	// guaranteed distinct namespace for the KB instance.
    	namespace = getName() + UUID.randomUUID();
    
        final AbstractTripleStore tripleStore = createTripleStore(
                getIndexManager(), namespace, getProperties());

    	if (tripleStore.isStatementIdentifiers()) {
            testMode = TestMode.sids;
        } else if (tripleStore.isQuads()) {
            testMode = TestMode.quads;
        } else {
            testMode = TestMode.triples;
        }
                
    	// Start the NSS with that KB instance as the default namespace.
        {
         
            final Map<String, String> initParams = new LinkedHashMap<String, String>();
            {

                initParams.put(ConfigParams.NAMESPACE, namespace);

                initParams.put(ConfigParams.CREATE, "false");

            }

            // Start server for that kb instance.
            m_fixture = NanoSparqlServer.newInstance(0/* port */,
                    getIndexManager(), initParams);

            m_fixture.start();

        }

        final int port = NanoSparqlServer.getLocalPort(m_fixture);
    
    	// log.info("Getting host address");
    
        final String hostAddr = NicUtil.getIpAddress("default.nic", "default",
                true/* loopbackOk */);
    
        if (hostAddr == null) {
    
            fail("Could not identify network address for this host.");
    
        }
    
        m_serviceURL = new URL("http", hostAddr, port,
                BigdataStatics.getContextPath()).toExternalForm();
    
        if (log.isInfoEnabled())
            log.info("Setup done: name=" + getName() + ", namespace="
                    + namespace + ", serviceURL=" + m_serviceURL);
    
    }

    @Override
    public void tearDown() throws Exception {

        // if (log.isInfoEnabled())
        log.warn("tearing down test: " + getName());

        if (m_fixture != null) {

            m_fixture.stop();

            m_fixture = null;

        }

        final IIndexManager m_indexManager = getIndexManager();

        if (m_indexManager != null && namespace != null) {

            dropTripleStore(m_indexManager, namespace);

        }

        // m_indexManager = null;

        namespace = null;

        m_serviceURL = null;

        log.info("tear down done");

        super.tearDown();

    }

//    /**
//     * Returns a view of the default triple store for the test using the sail
//     * interface.
//     * 
//     * @throws SailException
//     */
//    protected BigdataSail getSail() throws SailException {
//
//        return getSail(namespace);
//
//    }

    /**
     * Returns a view of the named triple store using the sail interface.
     * 
     * @throws SailException
     */
    protected BigdataSail getSail(final String namespace) throws SailException {

        final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                .getResourceLocator().locate(namespace, ITx.UNISOLATED);

        if (tripleStore == null) {
         
            // Not found.
            return null;
        }

        final BigdataSail sail = new BigdataSail(tripleStore);
        
        sail.initialize();

        return sail;

    }

}