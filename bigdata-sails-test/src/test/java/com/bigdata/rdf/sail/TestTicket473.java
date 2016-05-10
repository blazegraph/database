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

package com.bigdata.rdf.sail;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.sail.SailException;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.lexicon.Id2TermWriteProc;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.service.AbstractTransactionService;

/**
 * Test suite for <a
 * href="https://sourceforge.net/apps/trac/bigdata/ticket/473">
 * PhysicalAddressResolutionException after reopen using RWStore and
 * recycler</a>. The root cause for this exception was traced to recycling the
 * root not when it was not direct in the BTree writeCheckpoint() code. The
 * BTree was dirty because {@link Id2TermWriteProc} was having a side-effect on
 * the {@link BTree#getCounter()} when processing a blank node if the
 * {@link AbstractTripleStore.Options#STORE_BLANK_NODES} was <code>false</code>.
 * 
 * @author <a href="mailto:martyncutcher@users.sourceforge.net">Martyn
 *         Cutcher</a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTicket473 extends TestCase {

    private static final Logger log = Logger.getLogger(TestTicket473.class);
    
    /**
     * 
     */
    public TestTicket473() {
    }

    /**
     * @param arg0
     */
    public TestTicket473(String arg0) {
        super(arg0);
    }

//    @Override
    private Properties getProperties() {
        
        final Properties props = new Properties();
        props.setProperty(BigdataSail.Options.NAMESPACE,"foo.bar.snapdragon.kb");
        props.setProperty(com.bigdata.journal.Options.BUFFER_MODE,BufferMode.DiskRW.name());
        props.setProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY,"4000");
        props.setProperty(IndexMetadata.Options.BTREE_BRANCHING_FACTOR,"128");

        // TODO Also fails with sessionProtection (minReleaseAge:=0).
        props.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE,"1");
        props.setProperty(AbstractTripleStore.Options.TEXT_INDEX,"false");
        props.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,"false");
        props.setProperty(AbstractTripleStore.Options.JUSTIFY,"false");
        props.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.STATEMENT_IDENTIFIERS,"false");
        props.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.QUADS_MODE,"false");
        props.setProperty(com.bigdata.journal.Options.MAXIMUM_EXTENT,"209715200");
        props.setProperty(BigdataSail.Options.BUFFER_CAPACITY,"100000");
        props.setProperty(AbstractTripleStore.Options.BLOOM_FILTER,"false");
        props.setProperty(com.bigdata.journal.Options.FILE,"ticket473.jnl");

        return props;
        
    }
    
    private BigdataSail getSail(final Properties properties) {
        return new BigdataSail(properties);
    }
    
    /**
     * This seems to hinge on simple updates, commits and re-opens.
     * <p>
     * This test does not require reading current state.
     */
    public void test_stressTicket473() throws Exception {
        BigdataSail sail = null;
        try {
            for (int i = 0; i < 20; i++) {
                log.info("Opening sail");
                sail = getSail(getProperties());
                try {
                    sail.initialize();
                    loadOntology(sail,
                            "data/lehigh/univ-bench.owl");
                    doTicket473Commit(sail);
                } finally {
                    sail.shutDown();
                }
            }
        } finally {
            if (sail != null)
                sail.__tearDownUnitTest();
        }
    }

    private void doTicket473Commit(final BigdataSail sail) throws Exception {

        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection conn = (BigdataSailRepositoryConnection) repo
//                .getReadWriteConnection();
                .getUnisolatedConnection();
        conn.setAutoCommit(false);

        try {

            final ValueFactory vf = conn.getValueFactory();

            final URI pred = vf.createURI("http://foo.bar/ontoloty/hasGate");

            for (int r = 0; r < 10; r++) {
                for (int i = 0; i < 10; i++) {
                    
                    final URI subj = vf.createURI("http://foo.bar/note/" + r + "_"
                            + i);

                    final URI obj = vf
                            .createURI("http://foo.bar/gate/" + r + "_" + i);
                    
                    conn.add(subj, pred, obj, (Resource) null);
                    
                }

                conn.commit();

                Thread.sleep(100);

            }
            Thread.sleep(10000);

        } finally {

            conn.close();
        }

    }

    private void loadOntology(final BigdataSail sail, final String fileName)
            throws SailException, InterruptedException {

        final File file = new File(fileName);

        final String baseURI = file.toURI().toString();
        
        final AtomicLong nmodified = new AtomicLong();
        
        final Resource defaultContext = null;
        
        final BigdataSailConnection conn = sail.getUnisolatedConnection();

        try {

            final RDFFormat format = RDFFormat.forFileName(fileName);

            final RDFParserFactory rdfParserFactory = RDFParserRegistry
                    .getInstance().get(format);

            final RDFParser rdfParser = rdfParserFactory.getParser();

            rdfParser.setValueFactory(conn.getTripleStore().getValueFactory());

            rdfParser.setVerifyData(true);

            rdfParser.setStopAtFirstError(true);

            rdfParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

            rdfParser.setRDFHandler(new AddStatementHandler(conn, nmodified,
                    defaultContext));

            /*
             * Run the parser, which will cause statements to be inserted.
             */
            InputStream is = null;
            try {
            	is = new BufferedInputStream(new FileInputStream(
                    file));
            } catch (java.io.FileNotFoundException f) {
            	//Might be a resource from the classpath
            	is = this.getClass().getClassLoader().getResourceAsStream(fileName);
            	assert is != null;
            }
           
            try {
                rdfParser.parse(is, baseURI);
            } finally {
                is.close();
            }

            // Commit the mutation.
            conn.commit();

        } catch (Throwable t) {

            conn.rollback();
            throw new RuntimeException(t);

        } finally {

            conn.close();

        }

    }

    /**
     * Helper class adds statements to the sail as they are visited by a parser.
     */
    private static class AddStatementHandler extends RDFHandlerBase {

        private final BigdataSailConnection conn;
        private final AtomicLong nmodified;
        private final Resource[] defaultContexts;

        public AddStatementHandler(final BigdataSailConnection conn,
                final AtomicLong nmodified, final Resource defaultContext) {
            this.conn = conn;
            this.nmodified = nmodified;
            final boolean quads = conn.getTripleStore().isQuads();
            if (quads && defaultContext != null) {
                // The default context may only be specified for quads.
                this.defaultContexts = new Resource[] { defaultContext };
            } else {
                this.defaultContexts = new Resource[0];
            }
        }

        public void handleStatement(final Statement stmt)
                throws RDFHandlerException {

            try {

                conn.addStatement(//
                        stmt.getSubject(), //
                        stmt.getPredicate(), //
                        stmt.getObject(), //
                        (Resource[]) (stmt.getContext() == null ?  defaultContexts
                                : new Resource[] { stmt.getContext() })//
                        );

            } catch (SailException e) {

                throw new RDFHandlerException(e);

            }

            nmodified.incrementAndGet();

        }

    }

}
