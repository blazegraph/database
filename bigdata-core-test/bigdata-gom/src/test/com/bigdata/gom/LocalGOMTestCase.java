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
package com.bigdata.gom;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.BigdataStatics;
import com.bigdata.gom.om.IObjectManager;
import com.bigdata.gom.om.ObjectManager;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.store.AbstractTripleStore;

public class LocalGOMTestCase extends TestCase implements IGOMProxy {

    private static final Logger log = Logger.getLogger(LocalGOMTestCase.class);

    protected BigdataSailRepository m_repo;
    protected BigdataSail m_sail;
    protected ValueFactory m_vf;
    protected IObjectManager om;
    
    public LocalGOMTestCase() {
    }

    public LocalGOMTestCase(String name) {
        super(name);
    }

	public static Test suite() {

		final LocalGOMTestCase delegate = new LocalGOMTestCase(); // !!!! THIS CLASS
															// !!!!

		/*
		 * Use a proxy test suite and specify the delegate.
		 */

		final ProxyTestSuite suite = new ProxyTestSuite(delegate, "Local GOM tests");
		
		suite.addTestSuite(TestGPO.class);
		suite.addTestSuite(TestGOM.class);
		suite.addTestSuite(TestOwlGOM.class);

		return suite;
	}

		/*
		 * List any non-proxied tests (typically bootstrapping tests).
		 */

		protected Properties getProperties() throws Exception {
    	
        final Properties properties = new Properties();

        // create a backing file for the database
        final File journal = File.createTempFile("bigdata", ".jnl");
        properties.setProperty(
                BigdataSail.Options.FILE,
                journal.getAbsolutePath()
                );
        properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());
        properties.setProperty(AbstractTripleStore.Options.TEXT_INDEX, "false");
//        properties.setProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "200");
//        properties.setProperty("com.bigdata.namespace.kb.spo.SPO.com.bigdata.btree.BTree.branchingFactor", "200");
//        properties.setProperty("com.bigdata.namespace.kb.spo.POS.com.bigdata.btree.BTree.branchingFactor", "200");
//        properties.setProperty("com.bigdata.namespace.kb.spo.OSP.com.bigdata.btree.BTree.branchingFactor", "200");
//        properties.setProperty("com.bigdata.namespace.kb.spo.BLOBS.com.bigdata.btree.BTree.branchingFactor", "200");
//        properties.setProperty("com.bigdata.namespace.kb.lex.TERM2ID.com.bigdata.btree.BTree.branchingFactor", "200");
//        properties.setProperty("com.bigdata.namespace.kb.lex.ID2TERM.com.bigdata.btree.BTree.branchingFactor", "200");
        properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");

        return properties;
        
    }
    
    protected void setUp() throws Exception {

        // instantiate a sail and a Sesame repository
        m_sail = new BigdataSail(getProperties());
        m_repo = new BigdataSailRepository(m_sail);
        m_repo.initialize();
        m_vf = m_sail.getValueFactory();
        // Note: This uses a mock endpoint URL.
        om = new ObjectManager("http://localhost"
                + BigdataStatics.getContextPath() + "/sparql", m_repo);

    }
    
    protected void tearDown() throws Exception {
//        try {
//            final long start = System.currentTimeMillis();
            // m_repo.close();
        m_sail.__tearDownUnitTest();
        m_sail = null;
        m_repo = null;
        m_vf = null;
        if (om != null) {
            om.close();
            om = null;
        }
//            final long dur = System.currentTimeMillis() - start;
//            if (log.isInfoEnabled())
//                log.info("Sail shutdown: " + dur + "ms");
//        } catch (SailException e) {
//            e.printStackTrace();
//        }
    }
    
    protected void print(final URL n3) throws IOException {
        if (log.isInfoEnabled()) {
            final InputStream in = n3.openConnection().getInputStream();
            final Reader reader = new InputStreamReader(in);
            try {
                final char[] buf = new char[256];
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

    /**
     * Utility to load n3 statements from a resource
     */
    public void load(final URL n3, final RDFFormat rdfFormat)
            throws IOException, RDFParseException, RepositoryException {

        final InputStream in = n3.openConnection().getInputStream();
        try {
            final Reader reader = new InputStreamReader(in);
            try {

                final BigdataSailRepositoryConnection cxn = m_repo
                        .getConnection();
                try {
                    cxn.setAutoCommit(false);
                    cxn.add(reader, "", rdfFormat);
                    cxn.commit();
                } finally {
                    cxn.close();
                }
            } finally {
                reader.close();
            }
        } finally {
            in.close();
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
