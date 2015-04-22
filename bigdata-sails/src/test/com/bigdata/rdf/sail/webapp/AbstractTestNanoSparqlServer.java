package com.bigdata.rdf.sail.webapp;

import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.server.Server;

import com.bigdata.BigdataStatics;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.CreateKBTask;
import com.bigdata.rdf.sail.DestroyKBTask;
import com.bigdata.rdf.sail.webapp.client.HttpClientConfigurator;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryDecls;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.rdf.task.AbstractApiTask;
import com.bigdata.util.config.NicUtil;

import junit.framework.TestCase2;

public abstract class AbstractTestNanoSparqlServer extends TestCase2 {

	private Server m_fixture;
	protected String namespace;
	protected Journal m_indexManager;
	private String m_rootURL;
	private String m_serviceURL;
	private RemoteRepositoryManager m_repo;
	private HttpClient m_client;

	public AbstractTestNanoSparqlServer() {
		super();
	}

	public AbstractTestNanoSparqlServer(String name) {
		super(name);
	}

	public String getServiceURL() {
		return m_serviceURL;
	}

	public void createNamespace(String namespace, Properties properties) {
		try {
			properties.setProperty(RemoteRepositoryDecls.OPTION_CREATE_KB_NAMESPACE, namespace);
			m_repo.createRepository(namespace, properties);
		} catch (Exception e) {
			log.info(e.toString());
		}
	}

	public void createNamespace(String namespace) {
		createNamespace(namespace,getTripleStoreProperties());
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