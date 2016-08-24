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
 * Created on Mar 19, 2012
 */
package com.bigdata.gom;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.server.Server;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.BigdataStatics;
import com.bigdata.gom.om.IObjectManager;
import com.bigdata.gom.om.NanoSparqlObjectManager;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.ConfigParams;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
import com.bigdata.rdf.sail.webapp.client.HttpClientConfigurator;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.util.config.NicUtil;

/**
 * Similar to TestGOM but is setup to connect to the NanoSparqlServer using a
 * RemoteRepository
 * 
 * @author Martyn Cutcher
 * 
 */
public class RemoteGOMTestCase extends TestCase implements IGOMProxy  {

    private static final Logger log = Logger.getLogger(RemoteGOMTestCase.class);

    protected Server m_server;

	protected HttpClient m_client;
	protected RemoteRepositoryManager m_repo;
	
	protected String m_serviceURL;

	protected IIndexManager m_indexManager;

	protected String m_namespace;

	protected BigdataSailRepository repo;

    protected ValueFactory m_vf;
    protected IObjectManager om;

	public static Test suite() {

		final RemoteGOMTestCase delegate = new RemoteGOMTestCase(); // !!!! THIS CLASS
															// !!!!

		/*
		 * Use a proxy test suite and specify the delegate.
		 */

		final ProxyTestSuite suite = new ProxyTestSuite(delegate, "Remote GOM tests");
		
		suite.addTestSuite(TestGPO.class);
		suite.addTestSuite(TestGOM.class);
		suite.addTestSuite(TestOwlGOM.class);

		return suite;
	}

	//	protected BigdataSailRepositoryConnection m_cxn;
	
    protected Properties getProperties() throws Exception {
    	
        final Properties properties = new Properties();

        // create a backing file for the database
        final File journal = File.createTempFile("bigdata", ".jnl");
        properties.setProperty(BigdataSail.Options.FILE, journal
                .getAbsolutePath());
        properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW
                .toString());
        properties.setProperty(AbstractTripleStore.Options.TEXT_INDEX, "false");
        properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
//        properties.setProperty(
//                IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "200");
//        properties
//                .setProperty(
//                        "com.bigdata.namespace.kb.spo.SPO.com.bigdata.btree.BTree.branchingFactor",
//                        "200");
//        properties
//                .setProperty(
//                        "com.bigdata.namespace.kb.spo.POS.com.bigdata.btree.BTree.branchingFactor",
//                        "200");
//        properties
//                .setProperty(
//                        "com.bigdata.namespace.kb.spo.OSP.com.bigdata.btree.BTree.branchingFactor",
//                        "200");
//        properties
//                .setProperty(
//                        "com.bigdata.namespace.kb.spo.BLOBS.com.bigdata.btree.BTree.branchingFactor",
//                        "200");
//        properties
//                .setProperty(
//                        "com.bigdata.namespace.kb.lex.TERM2ID.com.bigdata.btree.BTree.branchingFactor",
//                        "200");
//        properties
//                .setProperty(
//                        "com.bigdata.namespace.kb.lex.ID2TERM.com.bigdata.btree.BTree.branchingFactor",
//                        "200");

        return properties;
        
    }

    @Override
    public void setUp() throws Exception {

        // instantiate a sail and a Sesame repository
        final BigdataSail sail = new BigdataSail(getProperties());
        repo = new BigdataSailRepository(sail);
        repo.initialize();

        //m_cxn = repo.getConnection();
        //m_cxn.setAutoCommit(false);

        m_namespace = BigdataSail.Options.DEFAULT_NAMESPACE;

        final Map<String, String> initParams = new LinkedHashMap<String, String>();
        {

            initParams.put(ConfigParams.NAMESPACE, m_namespace);

            initParams.put(ConfigParams.CREATE, "false");

        }
        
        m_indexManager = repo.getSail().getIndexManager();
        m_server = NanoSparqlServer.newInstance(0/* port */, m_indexManager,
                initParams);

        m_server.start();

        final int port = NanoSparqlServer.getLocalPort(m_server);

        final String hostAddr = NicUtil.getIpAddress("default.nic", "default",
                true/* loopbackOk */);

        if (hostAddr == null) {

            fail("Could not identify network address for this host.");

        }

        m_serviceURL = new URL("http", hostAddr, port,
                BigdataStatics.getContextPath() /* file */)
        		// BigdataStatics.getContextPath() + "/sparql"/* file */)
                .toExternalForm();

        // final HttpClient httpClient = new DefaultHttpClient();

        // m_cm = httpClient.getConnectionManager();
       	m_client = HttpClientConfigurator.getInstance().newInstance();
        
        m_repo = new RemoteRepositoryManager(m_serviceURL, m_client, m_indexManager.getExecutorService());
        
        om = new NanoSparqlObjectManager(m_repo.getRepositoryForDefaultNamespace(),
                m_namespace); 

    }

    // FIXME This is probably not tearing down the backing file for the journal!
    @Override
    public void tearDown() throws Exception {

        if (log.isInfoEnabled())
            log.info("tearing down test: " + getName());

        if (om != null) {
            om.close();
            om = null;
        }
        
        if (m_server != null) {

            m_server.stop();

            m_server = null;

        }
        
        m_repo.close();
        
        m_repo = null;
        
        m_client.stop();
        m_client = null;

        m_serviceURL = null;

        if (m_indexManager != null && m_namespace != null) {

            final AbstractTripleStore tripleStore = (AbstractTripleStore) m_indexManager
                    .getResourceLocator().locate(m_namespace, ITx.UNISOLATED);

            if (tripleStore != null) {

                if (log.isInfoEnabled())
                    log.info("Destroying: " + m_namespace);

                tripleStore.destroy();

            }
            
        }
        
        if (m_indexManager != null) {
        	m_indexManager.destroy();
        }

        m_indexManager = null;

        m_namespace = null;

        super.tearDown();

        log.info("tear down done");

    }
    
    /**
     * Utility to load statements from a resource
     */
    @Override
    public void load(final URL n3, final RDFFormat rdfFormat)
            throws IOException, RDFParseException, RepositoryException {
        final InputStream in = n3.openConnection().getInputStream();
        try {
            final Reader reader = new InputStreamReader(in);

            // FIXME: Loads into server directly, should change later to load
            // view ObjectManager
            final BigdataSailRepositoryConnection m_cxn = repo.getConnection();
            try {
                m_cxn.setAutoCommit(false);
                m_cxn.add(reader, "kb", rdfFormat);
                m_cxn.commit();
            } finally {
                m_cxn.close();
            }
        } finally {
            in.close();
        }
    }
	
	protected void print(final URL n3) throws IOException {
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

	@Override
	public IObjectManager getObjectManager() {
		return om;
	}

	@Override
	public ValueFactory getValueFactory() {
		return m_vf;
	}

	@Override
	public void proxySetup() throws Exception {
		setUp();
	}

	@Override
	public void proxyTearDown() throws Exception {
		tearDown();
	}
}
