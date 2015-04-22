package com.bigdata.blueprints;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
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
import com.bigdata.rdf.sail.webapp.ConfigParams;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
import com.bigdata.rdf.sail.webapp.client.HttpClientConfigurator;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.task.AbstractApiTask;
import com.bigdata.util.config.NicUtil;

/**
 * Unit tests for the {@link NanoSparqlServer} with a focus on the ability to
 * override the init parameters, the default http port, etc. This test suite is
 * written without the proxy mechanisms to make this easier to debug.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public abstract class AbstractTestNSSBlueprintsClient extends TestCase{

    protected static final transient Logger log = Logger.getLogger(AbstractTestNSSBlueprintsClient.class);

    private Server m_fixture;
	protected String namespace;
	protected Journal m_indexManager;
	private String m_rootURL;
	private String m_serviceURL;
	private RemoteRepositoryManager m_repo;
	private HttpClient m_client;
	private int m_port;
	
	public String getServiceURL()
	{
		return m_serviceURL;
	}
	
	public String getNamespace()
	{
		return namespace;
	}
	
	public int getPort()
	{
		return m_port;
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
	
	            initParams.put(ConfigParams.CREATE, "true");
	            
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
	
	        m_port = NanoSparqlServer.getLocalPort(m_fixture);
	
	        // log.info("Getting host address");
	
	        final String hostAddr = NicUtil.getIpAddress("default.nic", "default",
	                true/* loopbackOk */);
	
	        if (hostAddr == null) {
	
	            fail("Could not identify network address for this host.");
	
	        }
	
	        m_rootURL = new URL("http", hostAddr, m_port, ""/* contextPath */
	        ).toExternalForm();
	
	        m_serviceURL = new URL("http", hostAddr, m_port,
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
