package com.bigdata.gom;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.gpo.ILinkSet;
import com.bigdata.gom.om.IObjectManager;
import com.bigdata.gom.om.NanoSparqlObjectManager;
import com.bigdata.gom.om.ObjectManager;
import com.bigdata.gom.om.ObjectMgrModel;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.webapp.ConfigParams;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
import com.bigdata.rdf.sail.webapp.client.DefaultClientConnectionManagerFactory;
import com.bigdata.rdf.sail.webapp.client.IRemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.util.config.NicUtil;

import junit.framework.TestCase;

/**
 * Similar to TestGOM but is setup to connect to the NanoSparqlServer using a
 * RemoteRepository
 * 
 * @author Martyn Cutcher
 * 
 */
public class TestRemoteGOM extends TestCase {
	protected static final Logger log = Logger.getLogger(IObjectManager.class);

	Server m_server = null;
	RemoteRepository m_repo = null;
	private String m_serviceURL;

	private ClientConnectionManager m_cm;

	private IIndexManager m_indexMgr;

	private String m_namespace;

	private BigdataSailRepository repo;

	private BigdataSailRepositoryConnection m_cxn;

	public void testSimpleDirectData() throws Exception {
		final IObjectManager om = new NanoSparqlObjectManager(m_repo, m_namespace);
		final ValueFactory vf = om.getValueFactory();
					
		final URL n3 = TestGOM.class.getResource("testgom.n3");
		
		print(n3);
		load(n3);
		
		final URI s = vf.createURI("gpo:#root");

		final URI rootAttr = vf.createURI("attr:/root");
		om.getGPO(s).getValue(rootAttr);
		final URI rootId = (URI) om.getGPO(s).getValue(rootAttr);
		
		final IGPO rootGPO = om.getGPO(rootId);
		
		log.info("--------");
		log.info(rootGPO.pp());
		
		log.info(rootGPO.getType().pp());
		
		log.info(rootGPO.getType().getStatements());
		
		final URI typeName = vf.createURI("attr:/type#name");
		assertTrue("Company".equals(rootGPO.getType().getValue(typeName).stringValue()));
		
		// find set of workers for the Company
		final URI worksFor = vf.createURI("attr:/employee#worksFor");
	    ILinkSet linksIn = rootGPO.getLinksIn(worksFor);
	    Iterator<IGPO> workers = linksIn.iterator();
	    while (workers.hasNext()) {
	    	log.info("Returned: " + workers.next().pp());
	    }
	}

	public void testSimpleCreate() throws RepositoryException, IOException {
		final IObjectManager om = new NanoSparqlObjectManager(m_repo, m_namespace);
		final ValueFactory vf = om.getValueFactory();
		
		final URI keyname = vf.createURI("attr:/test#name");
		final Resource id = vf.createURI("gpo:test#1");
		om.checkValue(id);
		final int transCounter = om.beginNativeTransaction();
		try {
			IGPO gpo = om.getGPO(id);
			
			gpo.setValue(keyname, vf.createLiteral("Martyn"));			
			
			om.commitNativeTransaction(transCounter);
		} catch (Throwable t) {
			om.rollbackNativeTransaction();
			
			throw new RuntimeException(t);
		}
		
		// clear cached data
		((ObjectMgrModel) om).clearCache();
		
		IGPO gpo = om.getGPO(id); // reads from backing journal
		
		assertTrue("Martyn".equals(gpo.getValue(keyname).stringValue()));
	}

	public void testIncrementalUpdates() throws RepositoryException, IOException {
		
		final IObjectManager om = new NanoSparqlObjectManager(m_repo, m_namespace);
		final ValueFactory vf = om.getValueFactory();

		final int transCounter = om.beginNativeTransaction();
		final URI name = vf.createURI("attr:/test#name");
		final URI ni = vf.createURI("attr:/test#ni");
		final URI age = vf.createURI("attr:/test#age");
		final URI mob = vf.createURI("attr:/test#mobile");
		final URI gender = vf.createURI("attr:/test#mail");
		try {
			// warmup
			for (int i = 0; i < 10000; i++) {
				final IGPO tst = om.createGPO();
				tst.setValue(name, vf.createLiteral("Test" + i));
			}

			// go for it
			final long start = System.currentTimeMillis();

			final int creates = 20000;
			for (int i = 0; i < creates; i++) {
				final IGPO tst = om.createGPO();
				tst.setValue(name, vf.createLiteral("Name" + i));
				tst.setValue(ni, vf.createLiteral("NI" + i));
				tst.setValue(age, vf.createLiteral(i));
				tst.setValue(mob, vf.createLiteral("0123-" + i));
				tst.setValue(gender, vf.createLiteral(1 % 3 == 0));
			}

			om.commitNativeTransaction(transCounter);
			
			final long duration = (System.currentTimeMillis()-start);
			
			// Note that this is a concervative estimate for statements per second since there is
			//	only one per object, requiring the object URI and the Value to be added.
			System.out.println("Creation rate of " + (creates*1000/duration) + " objects per second");
			System.out.println("Creation rate of " + (creates*5*1000/duration) + " statements per second");
		} catch (Throwable t) {
			t.printStackTrace();
			
			om.rollbackNativeTransaction();
			
			throw new RuntimeException(t);
		}
		
	}

	public void setUp() throws Exception {
		Properties properties = new Properties();

		// create a backing file for the database
		File journal = File.createTempFile("bigdata", ".jnl");
		properties.setProperty(BigdataSail.Options.FILE, journal
				.getAbsolutePath());
		properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW
				.toString());
		properties.setProperty(AbstractTripleStore.Options.TEXT_INDEX, "false");
		properties.setProperty(
				IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "200");
		properties
				.setProperty(
						"com.bigdata.namespace.kb.spo.SPO.com.bigdata.btree.BTree.branchingFactor",
						"200");
		properties
				.setProperty(
						"com.bigdata.namespace.kb.spo.POS.com.bigdata.btree.BTree.branchingFactor",
						"200");
		properties
				.setProperty(
						"com.bigdata.namespace.kb.spo.OSP.com.bigdata.btree.BTree.branchingFactor",
						"200");
		properties
				.setProperty(
						"com.bigdata.namespace.kb.spo.BLOBS.com.bigdata.btree.BTree.branchingFactor",
						"200");
		properties
				.setProperty(
						"com.bigdata.namespace.kb.lex.TERM2ID.com.bigdata.btree.BTree.branchingFactor",
						"200");
		properties
				.setProperty(
						"com.bigdata.namespace.kb.lex.ID2TERM.com.bigdata.btree.BTree.branchingFactor",
						"200");

		// instantiate a sail and a Sesame repository
		BigdataSail sail = new BigdataSail(properties);
		repo = new BigdataSailRepository(sail);
		repo.initialize();

		//m_cxn = repo.getConnection();
		//m_cxn.setAutoCommit(false);

		m_namespace = "kb";

		final Map<String, String> initParams = new LinkedHashMap<String, String>();
		{

			initParams.put(ConfigParams.NAMESPACE, m_namespace);

			initParams.put(ConfigParams.CREATE, "false");

		}
		
		m_indexMgr = repo.getDatabase().getIndexManager();
		m_server = NanoSparqlServer.newInstance(0/* port */, m_indexMgr,
				initParams);

		m_server.start();

		final int port = m_server.getConnectors()[0].getLocalPort();

		final String hostAddr = NicUtil.getIpAddress("default.nic", "default",
				true/* loopbackOk */);

		if (hostAddr == null) {

			fail("Could not identify network address for this host.");

		}

		m_serviceURL = new URL("http", hostAddr, port, "/sparql"/* file */)
				.toExternalForm();

		// final HttpClient httpClient = new DefaultHttpClient();

		// m_cm = httpClient.getConnectionManager();

		m_cm = DefaultClientConnectionManagerFactory.getInstance()
				.newInstance();

		m_repo = new RemoteRepository(m_serviceURL,
				new DefaultHttpClient(m_cm), m_indexMgr.getExecutorService());

	}

	public void tearDown() throws Exception {

		// if (log.isInfoEnabled())
		log.warn("tearing down test: " + getName());

		if (m_server != null) {

			m_server.stop();

			m_server = null;

		}

		if (m_indexMgr != null && m_namespace != null) {

			final AbstractTripleStore tripleStore = (AbstractTripleStore) m_indexMgr
					.getResourceLocator().locate(m_namespace, ITx.UNISOLATED);

			if (tripleStore != null) {

				if (log.isInfoEnabled())
					log.info("Destroying: " + m_namespace);

				tripleStore.destroy();

			}

		}

		// m_indexManager = null;

		m_namespace = null;

		m_serviceURL = null;

		if (m_cm != null) {

			m_cm.shutdown();

			m_cm = null;

		}

		m_repo = null;

		log.info("tear down done");

		super.tearDown();

	}
	
	/**
	 * Utility to load n3 statements from a resource
	 */
	private void load(final URL n3) throws IOException, RDFParseException, RepositoryException {
		InputStream in = n3.openConnection().getInputStream();
		Reader reader = new InputStreamReader(in);
		
		// FIXME: Loads into server directly, should change later to load view ObjectManager
		m_cxn = repo.getConnection();
		m_cxn.setAutoCommit(false);
		m_cxn.add(reader, "kb", RDFFormat.N3);
		m_cxn.commit();
		m_cxn.close();
	}

	
	void print(final URL n3) throws IOException {
		if (log.isInfoEnabled()) {
			InputStream in = n3.openConnection().getInputStream();
			Reader reader = new InputStreamReader(in);
			try {
			char[] buf = new char[256];
			int rdlen = 0;
			while ((rdlen = reader.read(buf)) > -1) {
				if (rdlen == 256)
					System.out.print(buf);
				else
					System.out.print(new String(buf, 0, rdlen));
			}
			} finally {
				reader.close();
			}
		}
	}
}
