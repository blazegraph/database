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
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.BigdataStatics;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.CreateKBTask;
import com.bigdata.rdf.sail.DestroyKBTask;
import com.bigdata.rdf.sail.webapp.client.HttpClientConfigurator;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.task.AbstractApiTask;
import com.bigdata.util.config.NicUtil;

import junit.framework.TestCase2;

/**
 * Unit tests for the {@link NanoSparqlServer} with a focus on the ability to
 */
public class TestServiceWhiteList extends TestCase2 {
	
    private static final String SOME_SERVICE_ENDPOINT = "http://someService.com/test";
	private Server m_fixture;
	protected String namespace;
	protected Journal m_indexManager;
	private String m_rootURL;
	private String m_serviceURL;
	private RemoteRepositoryManager m_repo;
	private HttpClient m_client;
	private final Map<String, String> initParams = new LinkedHashMap<String, String>();

    public void testServiceWhiteList() throws Exception {

        TupleQueryResult res = m_repo.getRepositoryForNamespace(namespace). //
        		prepareTupleQuery("SELECT ?b { ?b <http://purl.org/dc/elements/1.1/title> ?title . " + 
        							"SERVICE <" + SOME_SERVICE_ENDPOINT + "> { } }").evaluate();
        
        int resCount = 0;;
        while(res.hasNext()){
        	res.next();
        	resCount++;
        }
        
        assertEquals(0, resCount);
        
        boolean exceptionThrown = false;
        
        try {
        	res = m_repo.getRepositoryForNamespace(namespace). //
				prepareTupleQuery("SELECT ?b { ?b <http://purl.org/dc/elements/1.1/title> ?title . " + 
						"SERVICE <" + SOME_SERVICE_ENDPOINT + "1> { } }").evaluate();
        	
        } catch(Exception e) {
          	exceptionThrown = e.toString().contains("Service URI http://someService.com/test1 is not allowed");
        }
        
        assertTrue(exceptionThrown);
       
    }

	protected Properties getTripleStoreProperties() {
		 final Properties tripleStoreProperties = new Properties();
	     {
	         
	         tripleStoreProperties.setProperty(BigdataSail.Options.TRIPLES_MODE,
	                 "true");
	         
	         tripleStoreProperties.setProperty(Journal.Options.BUFFER_MODE,
	                    BufferMode.MemStore.name());
	         
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
	        
	        namespace = "testWhiteList" + UUID.randomUUID();
	        
	        m_indexManager = new Journal(journalProperties);
	        
	        AbstractApiTask.submitApiTask(m_indexManager,
		               new CreateKBTask(namespace, journalProperties)).get();
	       
	        {
	        	
	            initParams.put(ConfigParams.SERVICE_WHITELIST, SOME_SERVICE_ENDPOINT);
	            
	            initParams.put(ConfigParams.NAMESPACE, namespace);
	        	
	            initParams.put(ConfigParams.CREATE, "true");
	            	            
	        }
	
	        // Start server for that kb instance.
	        m_fixture = NanoSparqlServer.newInstance(0/* port */,
	                m_indexManager, initParams);
	
	        m_fixture.start();
	
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
	        
	        ServiceRegistry.getInstance().remove(new URIImpl(SOME_SERVICE_ENDPOINT));
	        ServiceRegistry.getInstance().setWhitelistEnabled(false);
	        
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
