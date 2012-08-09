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
package com.bigdata.gom;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.om.ObjectManager;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.store.AbstractTripleStore;

abstract public class LocalGOMTestCase extends TestCase {

    private static final Logger log = Logger.getLogger(LocalGOMTestCase.class);

    protected BigdataSailRepository m_repo;
    protected BigdataSail m_sail;
    protected ValueFactory m_vf;
    protected ObjectManager om;

    public LocalGOMTestCase() {
    }

    public LocalGOMTestCase(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        
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

        // instantiate a sail and a Sesame repository
        m_sail = new BigdataSail(properties);
        m_repo = new BigdataSailRepository(m_sail);
        m_repo.initialize();
        m_vf = m_sail.getValueFactory();
        // Note: This uses a mock endpoint URL.
        om = new ObjectManager("http://localhost/sparql", m_repo);
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
    protected void load(final URL n3, final RDFFormat rdfFormat)
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

    protected void showClassHierarchy(final Iterator<IGPO> classes,
            final int indent) {
        StringBuilder out = new StringBuilder();
        showClassHierarchy(out, classes, indent);
        System.out.println("Hierarchy: " + out.toString());
    }

    private void showClassHierarchy(StringBuilder out, Iterator<IGPO> classes,
            int indent) {
        while (classes.hasNext()) {
            final IGPO clss = classes.next();
            out.append(indentOut(clss, indent + 1));
            showClassHierarchy(out,
                    clss.getLinksIn(RDFS.SUBCLASSOF).iterator(), indent + 1);
        }
    }

    String indents = "\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t";

    private Object indentOut(IGPO clss, int indent) {
        Value lbl = clss.getValue(RDFS.LABEL);
        final String display = lbl == null ? clss.getId().stringValue() : lbl
                .stringValue();
        return indents.substring(0, indent) + display;
    }

    protected void showOntology(IGPO onto) {
        System.out.println("Ontology: " + onto.pp());
        Iterator<IGPO> parts = onto.getLinksIn().iterator();
        while (parts.hasNext()) {
            IGPO part = parts.next();
            System.out.println("Onto Part: " + part.pp());
        }
    }

}
