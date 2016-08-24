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
package com.bigdata.rdf.sail.webapp;

import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import com.bigdata.BigdataStatics;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.CreateKBTask;
import com.bigdata.rdf.sail.DestroyKBTask;
import com.bigdata.rdf.sail.webapp.client.HttpClientConfigurator;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.task.AbstractApiTask;
import com.bigdata.util.config.NicUtil;

import junit.framework.TestCase2;

/**
 * Unit tests for the {@link NanoSparqlServer} with a focus on the ability to
 * override the init parameters, the default http port, etc. This test suite is
 * written without the proxy mechanisms to make this easier to debug.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestNanoSparqlServer extends TestCase2 {

    

//    protected AbstractTripleStore createTripleStore(
//            final IIndexManager indexManager, final String namespace,
//            final Properties properties) {
//        
//        if(log.isInfoEnabled())
//            log.info("KB namespace=" + namespace);
//    
//        // Locate the resource declaration (aka "open"). This tells us if it
//        // exists already.
//        AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
//                .getResourceLocator().locate(namespace, ITx.UNISOLATED);
//    
//        if (tripleStore != null) {
//    
//            fail("exists: " + namespace);
//            
//        }
//    
//        /*
//         * Create the KB instance.
//         */
//    
//        if (log.isInfoEnabled()) {
//            log.info("Creating KB instance: namespace="+namespace);
//            log.info("Properties=" + properties.toString());
//        }
//    
//        if (indexManager instanceof Journal) {
//    
//            // Create the kb instance.
//            tripleStore = new LocalTripleStore(indexManager, namespace,
//                    ITx.UNISOLATED, properties);
//    
//        } else {
//    
//            tripleStore = new ScaleOutTripleStore(indexManager, namespace,
//                    ITx.UNISOLATED, properties);
//        }
//    
//        // create the triple store.
//        tripleStore.create();
//    
//        if(log.isInfoEnabled())
//            log.info("Created tripleStore: " + namespace);
//    
//        // New KB instance was created.
//        return tripleStore;
//    
//    }

//    protected void dropTripleStore(final IIndexManager indexManager,
//            final String namespace) {
//
//        if (log.isInfoEnabled())
//            log.info("KB namespace=" + namespace);
//    
//        // Locate the resource declaration (aka "open"). This tells us if it
//        // exists already.
//        final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
//                .getResourceLocator().locate(namespace, ITx.UNISOLATED);
//    
//        if (tripleStore != null) {
//    
//            if (log.isInfoEnabled())
//                log.info("Destroying: " + namespace);
//    
//            tripleStore.destroy();
//            
//        }
//    
//    }

    private Server m_fixture;
	protected String namespace;
	protected Journal m_indexManager;
	private String m_rootURL;
	private String m_serviceURL;
	private RemoteRepositoryManager m_repo;
	private HttpClient m_client;

	/**
     * Simple start/kill in which we verify that the default KB was NOT created
     * and that the explicitly create KB instance can still be resolved. This is
     * basically a test of the ability to override the init parameters in
     * <code>web.xml</code> to specify the {@link ConfigParams#NAMESPACE} and
     * {@link ConfigParams#CREATE} properties. If those overrides are not
     * applied then the default KB will be created and this test will fail. If
     * the test fails, the place to look is {@link NanoSparqlServer} where it is
     * overriding the init parameters for the {@link WebAppContext}.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/730" >
     *      Allow configuration of embedded NSS jetty server using jetty-web.xml
     *      </a>
     */
    public void test_start_stop() {

        final AbstractTripleStore tripleStore = (AbstractTripleStore) m_indexManager
                .getResourceLocator().locate(namespace, ITx.UNISOLATED);

        assertNotNull("Explicitly create KB not found: namespace=" + namespace,
                tripleStore);

        final AbstractTripleStore tripleStore2 = (AbstractTripleStore) m_indexManager
                .getResourceLocator().locate(
                        BigdataSail.Options.DEFAULT_NAMESPACE, ITx.UNISOLATED);

        /*
         * Note: A failure here means that our override of
         * ConfigParams.NAMESPACE was ignored.
         */
        assertNull("Default KB should not exist.", tripleStore2);

    }

	protected Properties getTripleStoreProperties() {
		 final Properties tripleStoreProperties = new Properties();
	     {
	         
	         tripleStoreProperties.setProperty(
	                 BigdataSail.Options.TRUTH_MAINTENANCE, "false");
	
	         tripleStoreProperties.setProperty(BigdataSail.Options.TRIPLES_MODE,
	                 "true");
	
	     }
	     
	     return tripleStoreProperties;
	}

	@Override
	public void setUp() throws Exception {
	
	        log.warn("Setting up test:" + getName());
	        
	        final Properties journalProperties = new Properties();
	        {
	            journalProperties.setProperty(Journal.Options.BUFFER_MODE,
	                    BufferMode.MemStore.name());
	        }
	
	        // guaranteed distinct namespace for the KB instance.
	        namespace = getName() + UUID.randomUUID();
	
	        m_indexManager = new Journal(journalProperties);
	        
	        // Properties for the KB instance.  
	        final Properties tripleStoreProperties = this.getTripleStoreProperties();
	
	        // Create the triple store instance.
	//        final AbstractTripleStore tripleStore = createTripleStore(m_indexManager,
	//                namespace, tripleStoreProperties);
	         AbstractApiTask.submitApiTask(m_indexManager,
	               new CreateKBTask(namespace, tripleStoreProperties)).get();
	
	        // Override namespace.  Do not create the default KB.
	        final Map<String, String> initParams = new LinkedHashMap<String, String>();
	        {
	
	            initParams.put(ConfigParams.NAMESPACE, namespace);
	
	            initParams.put(ConfigParams.CREATE, "false");
	            
	        }
	
	        // Start server for that kb instance.
	        m_fixture = NanoSparqlServer.newInstance(0/* port */,
	                m_indexManager, initParams);
	
	        m_fixture.start();
	
	//        final WebAppContext wac = NanoSparqlServer.getWebApp(m_fixture);
	//
	//        wac.start();
	//
	//        for (Map.Entry<String, String> e : initParams.entrySet()) {
	//
	//            wac.setInitParameter(e.getKey(), e.getValue());
	//
	//        }
	
	        final int port = NanoSparqlServer.getLocalPort(m_fixture);
	
	        // log.info("Getting host address");
	
	        final String hostAddr = NicUtil.getIpAddress("default.nic", "default",
	                true/* loopbackOk */);
	
	        if (hostAddr == null) {
	
	            fail("Could not identify network address for this host.");
	
	        }
	
	        m_rootURL = new URL("http", hostAddr, port, ""/* contextPath */
	        ).toExternalForm();
	
	        m_serviceURL = new URL("http", hostAddr, port,
	                BigdataStatics.getContextPath()).toExternalForm();
	
	        if (log.isInfoEnabled())
	            log.info("Setup done: \nname=" + getName() + "\nnamespace="
	                    + namespace + "\nrootURL=" + m_rootURL + "\nserviceURL="
	                    + m_serviceURL);
	
	       	m_client = HttpClientConfigurator.getInstance().newInstance();
	        
	        m_repo = new RemoteRepositoryManager(m_serviceURL, m_client,
	                m_indexManager.getExecutorService());
	
	    }

	@Override
	public void tearDown() throws Exception {
	
	        // if (log.isInfoEnabled())
	        log.warn("tearing down test: " + getName());
	
	        if (m_fixture != null) {
	
	            m_fixture.stop();
	
	            m_fixture = null;
	
	        }
	
	        if (m_indexManager != null && namespace != null) {
	           
	//            dropTripleStore(m_indexManager, namespace);
	           AbstractApiTask.submitApiTask(m_indexManager,
	               new DestroyKBTask(namespace)).get();
	
	            m_indexManager = null;
	
	        }
	
	        namespace = null;
	
	        m_rootURL = null;
	        m_serviceURL = null;
	        
	        m_repo.close();
	        
	        m_client.stop();
	        
	        log.info("tear down done");
	
	        super.tearDown();
	
	    }

}
