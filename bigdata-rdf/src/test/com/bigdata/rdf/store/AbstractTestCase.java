/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 14, 2006
 */

package com.bigdata.rdf.store;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import junit.framework.TestCase2;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.sesame.admin.RdfAdmin;
import org.openrdf.sesame.admin.StdOutAdminListener;
import org.openrdf.sesame.admin.UpdateException;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.sesame.sail.RdfRepository;
import org.openrdf.sesame.sail.SailInitializationException;
import org.openrdf.sesame.sail.SailUtil;
import org.openrdf.sesame.sail.StatementIterator;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.isolation.IIsolatableIndex;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Options;
import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.sail.BigdataRdfRepository;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.util.KeyOrder;

/**
 * <p>
 * Abstract harness for testing under a variety of configurations. In order to
 * test a specific configuration, create a concrete instance of this class. The
 * configuration can be described using a mixture of a <code>.properties</code>
 * file of the same name as the test class and custom code.
 * </p>
 * <p>
 * When debugging from an IDE, it is very helpful to be able to run a single
 * test case. You can do this, but you MUST define the property
 * <code>testClass</code> as the name test class that has the logic required
 * to instantiate and configure an appropriate object manager instance for the
 * test.
 * </p>
 */
abstract public class AbstractTestCase
    extends TestCase2
{

    //
    // Constructors.
    //

    public AbstractTestCase() {}
    
    public AbstractTestCase(String name) {super(name);}

    //************************************************************
    //************************************************************
    //************************************************************
    
    /**
     * Invoked from {@link TestCase#setUp()} for each test in the suite.
     */
    public void setUp(ProxyTestCase testCase) throws Exception {

        begin = System.currentTimeMillis();
        
        log.info("\n\n================:BEGIN:" + testCase.getName()
                + ":BEGIN:====================");

    }

    /**
     * Invoked from {@link TestCase#tearDown()} for each test in the suite.
     */
    public void tearDown(ProxyTestCase testCase) throws Exception {

        long elapsed = System.currentTimeMillis() - begin;
        
        log.info("\n================:END:" + testCase.getName()
                + " ("+elapsed+"ms):END:====================\n");

    }
    
    private long begin;
    
    public void tearDown() throws Exception {
        
        super.tearDown();
        
    }

    //
    // Properties
    //
    
    private Properties m_properties;
    
    /**
     * <p>
     * Returns properties read from a hierarchy of sources. The underlying
     * properties read from those sources are cached, but a new properties
     * object is returned on each invocation (to prevent side effects by the
     * caller).
     * </p>
     * <p>
     * In general, a test configuration critically relies on both the properties
     * returned by this method and the appropriate properties must be provided
     * either through the command line or in a properties file.
     * </p>
     * 
     * @return A new properties object.
     */
    public Properties getProperties() {
        
        if( m_properties == null ) {
            
            /*
             * Read properties from a hierarchy of sources and cache a
             * reference.
             */
            
            m_properties = super.getProperties();
//            m_properties = new Properties( m_properties );

            m_properties.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());

            /*
             * Use a temporary file for the test. Such files are always deleted when
             * the journal is closed or the VM exits.
             */
            m_properties.setProperty(Options.CREATE_TEMP_FILE,"true");
            m_properties.setProperty(Options.DELETE_ON_EXIT,"true");
            
        }        
        
        return m_properties;
        
    }

    /**
     * This method is invoked from methods that MUST be proxied to this class.
     * {@link GenericProxyTestCase} extends this class, as do the concrete
     * classes that drive the test suite for specific GOM integration test
     * configuration. Many method on this class must be proxied from
     * {@link GenericProxyTestCase} to the delegate. Invoking this method from
     * the implementations of those methods in this class provides a means of
     * catching omissions where the corresponding method is NOT being delegated.
     * Failure to delegate these methods means that you are not able to share
     * properties or object manager instances across tests, which means that you
     * can not do configuration-based testing of integrations and can also wind
     * up with mutually inconsistent test fixtures between the delegate and each
     * proxy test.
     */
    
    protected void checkIfProxy() {
        
        if( this instanceof ProxyTestCase ) {
            
            throw new AssertionError();
            
        }
        
    }

    //************************************************************
    //************************************************************
    //************************************************************
    //
    // Test helpers.
    //

    protected final long N = IRawTripleStore.N;

    protected final long NULL = IRawTripleStore.NULL;
    
    abstract protected AbstractTripleStore getStore();
    
    abstract protected AbstractTripleStore reopenStore(AbstractTripleStore store);

    public void assertEquals(SPO expected, SPO actual) {
        
        assertEquals(null,expected,actual);
        
    }
    
    public void assertEquals(String msg, SPO expected, SPO actual) {
        
        if(!expected.equals(actual)) {
                    
            if( msg == null ) {
                msg = "";
            } else {
                msg = msg + " : ";
            }

            fail(msg+"Expecting: "+expected+" not "+actual);
            
        }
        
    }
    
    public void assertEquals(SPO[] expected, SPO[] actual) {

        assertEquals(null,expected,actual);
        
    }
    
    public void assertEquals(String msg, SPO[] expected, SPO[] actual) {
    
        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }

        if( expected == null && actual == null ) {
            
            return;
            
        }
        
        if( expected == null && actual != null ) {
            
            fail( msg+"Expected a null array." );
            
        }
        
        if( expected != null && actual == null ) {
            
            fail( msg+"Not expecting a null array." );
            
        }
        
        if (expected.length != actual.length) {
            /*
             * Only do message construction if we know that the assert will
             * fail.
             */
            assertEquals(msg + "length differs.", expected.length,
                    actual.length);
        }
        
        for( int i=0; i<expected.length; i++ ) {
            
            try {

                assertEquals(expected[i], actual[i]);
                
            } catch (AssertionFailedError ex) {
                
                /*
                 * Only do the message construction once the assertion is known
                 * to fail.
                 */
                
                fail(msg + "values differ: index=" + i, ex);
                
            }
            
        }
        
    }
    
    /**
     * Dumps the lexicon in a variety of ways.
     * 
     * @param store
     * 
     * @todo ClientIndexView does not disclose whether or not the index is
     *       isolatable so this will not work for a bigadata federation.
     */
    void dumpTerms(AbstractTripleStore store) {

        // Same #of terms in the forward and reverse indices.
        assertEquals("#terms", store.getIdTermIndex().rangeCount(null, null),
                store.getTermIdIndex().rangeCount(null, null));
        
        /**
         * Dumps the forward mapping.
         */
        {

            System.err.println("terms index (forward mapping).");

            IIndex ndx = store.getTermIdIndex();

            final boolean isolatableIndex = ndx instanceof IIsolatableIndex;

            IEntryIterator itr = ndx.rangeIterator(null, null);

            while (itr.hasNext()) {

                // the term identifier.
                Object val = itr.next();

                /*
                 * The sort key for the term. This is not readily decodable. See
                 * RdfKeyBuilder for specifics.
                 */
                byte[] key = itr.getKey();

                /*
                 * deserialize the term identifier (packed long integer).
                 */
                final long id;
                try {

                    id = (isolatableIndex ? new DataInputBuffer((byte[]) val)
                            .unpackLong() : (Long) val);

                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

                System.err.println(BytesUtil.toString(key) + ":" + id);

            }

        }

        /**
         * Dumps the reverse mapping.
         */
        {

            System.err.println("ids index (reverse mapping).");

            IIndex ndx = store.getIdTermIndex();

            final boolean isolatableIndex = ndx instanceof IIsolatableIndex;

            IEntryIterator itr = ndx.rangeIterator(null, null);

            while (itr.hasNext()) {

                // the serialized term.
                Object val = itr.next();

                // the sort key for the term identifier.
                byte[] key = itr.getKey();

                // decode the term identifier from the sort key.
                final long id = KeyBuilder.decodeLong(key, 0);

                _Value term = (isolatableIndex ? _Value
                        .deserialize((byte[]) val) : (_Value) val);

                System.err.println(id + ":" + term);

            }

        }
        
        /**
         * Dumps the term:id index.
         */
        for( Iterator<Long> itr = ((AbstractTripleStore)store).termIdIndexScan(); itr.hasNext(); ) {
            
            System.err.println("term->id : "+itr.next());
            
        }

        /**
         * Dumps the id:term index.
         */
        for( Iterator<Value> itr = ((AbstractTripleStore)store).idTermIndexScan(); itr.hasNext(); ) {
            
            System.err.println("id->term : "+itr.next());
            
        }

        /**
         * Dumps the terms in term order.
         */
        for( Iterator<Value> itr = ((AbstractTripleStore)store).termIterator(); itr.hasNext(); ) {
            
            System.err.println("termOrder : "+itr.next());
            
        }

    }
    
    /**
     * Method verifies that the <i>actual</i> {@link Iterator}
     * produces the expected objects in the expected order.  Objects
     * are compared using {@link Object#equals( Object other )}.  Errors
     * are reported if too few or too many objects are produced, etc.
     * 
     * @todo refactor to {@link TestCase2}.
     */
    static public void assertSameItr(Object[] expected, Iterator<?> actual) {

        assertSameIterator("", expected, actual);

    }

//    /**
//     * Method verifies that the <i>actual</i> {@link Iterator}
//     * produces the expected objects in the expected order.  Objects
//     * are compared using {@link Object#equals( Object other )}.  Errors
//     * are reported if too few or too many objects are produced, etc.
//     */
//    static public void assertSameItr(String msg, Object[] expected,
//            Iterator<?> actual) {
//
//        int i = 0;
//
//        while (actual.hasNext()) {
//
//            if (i >= expected.length) {
//
//                fail(msg + ": The iterator is willing to visit more than "
//                        + expected.length + " objects.");
//
//            }
//
//            Object g = actual.next();
//
//            //        if (!expected[i].equals(g)) {
//            try {
//                assertSameValue(expected[i], g);
//            } catch (AssertionFailedError ex) {
//                /*
//                 * Only do message construction if we know that the assert will
//                 * fail.
//                 */
//                fail(msg + ": Different objects at index=" + i + ": expected="
//                        + expected[i] + ", actual=" + g);
//            }
//
//            i++;
//
//        }
//
//        if (i < expected.length) {
//
//            fail(msg + ": The iterator SHOULD have visited " + expected.length
//                    + " objects, but only visited " + i + " objects.");
//
//        }
//
//    }

    /**
     * Uploads an file into an {@link RdfRepository}.
     *
     * @see RdfAdmin
     */
    protected void upload(RdfRepository repo, String resource, String baseURL,
            RDFFormat format)
            throws IOException, UpdateException {

        InputStream rdfStream = getClass().getResourceAsStream(resource);

        if (rdfStream == null) {

            // If we do not find as a Resource then try the file system.
            rdfStream = new BufferedInputStream(new FileInputStream(resource));

        }

        try {

            RdfAdmin admin = new RdfAdmin(repo);

            final boolean validate = true;

            admin.addRdfModel(rdfStream, baseURL, new StdOutAdminListener(),
                    format, validate);

        } finally {

            rdfStream.close();

        }

    }

    /*
     * compares two RDF models for equality.
     */

    /**
     * Wraps up the {@link AbstractTripleStore} as an {@link RdfRepository} to
     * facilitate using {@link #modelsEqual(RdfRepository, RdfRepository)} for
     * ground truth testing.
     * 
     * FIXME This is extremely slow.  Find a faster way to write this!  It is
     * making some of the unit tests quite painful.
     * 
     * @see SailUtil#modelsEqual(org.openrdf.sesame.sail.RdfSource, org.openrdf.sesame.sail.RdfSource)
     * which might be used instead.  However, it does not provide any details on
     * how the models differ.
     */
    public boolean modelsEqual(RdfRepository expected, InferenceEngine inf)
            throws SailInitializationException {

        BigdataRdfRepository repo = new BigdataRdfRepository(inf);
        
        Properties properties = new Properties(getProperties());
        
        properties.setProperty(BigdataRdfRepository.Options.TRUTH_MAINTENANCE,
                "" + true);
        
        repo.initialize( properties );
        
        return modelsEqual(expected,repo);
        
    }
    
    /**
     * Compares two RDF graphs for equality (same statements) - does NOT handle
     * bnodes, which much be treated as variables for RDF semantics.
     * 
     * @param expected
     * 
     * @param actual
     * 
     * @return true if all statements in the expected graph are in the actual
     *         graph and if the actual graph does not contain any statements
     *         that are not also in the expected graph.
     * 
     * @todo Sesame probably bundles this logic in
     *       {@link SailUtil#modelsEqual(org.openrdf.sesame.sail.RdfSource, org.openrdf.sesame.sail.RdfSource)}
     *       in a manner that handles bnodes.
     */
    public static boolean modelsEqual(RdfRepository expected,
            BigdataRdfRepository actual) {

        Collection<Statement> expectedRepo = getStatements(expected);

//        Collection<Statement> actualRepo= getStatements(actual);

        int actualSize = 0; 
        boolean sameStatements1 = true;
        {

            StatementIterator it = actual.getStatements(null, null, null);

            try {

                for (; it.hasNext();) {

                    Statement stmt = it.next();

                    if (!expected.hasStatement(stmt.getSubject(), stmt
                            .getPredicate(), stmt.getObject())) {

                        sameStatements1 = false;

                        log("Not expecting: " + stmt);

                    }

                    actualSize++; // count #of statements actually visited.
                    
                }

            } finally {

                it.close();

            }
            
            log("all the statements in actual in expected? " + sameStatements1);

        }

        int expectedSize = 0;
        boolean sameStatements2 = true;
        {

            for (Iterator<Statement> it = expectedRepo.iterator(); it.hasNext();) {

                Statement stmt = it.next();

                if (!actual.hasStatement(stmt.getSubject(),
                        stmt.getPredicate(), stmt.getObject())) {

                    sameStatements2 = false;

                    log("    Expecting: " + stmt);

                }
                
                expectedSize++; // counts statements actually visited.

            }

            log("all the statements in expected in actual? " + sameStatements2);

        }

        final boolean sameSize = expectedSize == actualSize;
        
        log("size of 'expected' repository: " + expectedSize);

        log("size of 'actual'   repository: " + actualSize);

        return sameSize && sameStatements1 && sameStatements2;

    }

    private static Collection<Statement> getStatements(RdfRepository repo) {

        /*
         * Note: do NOT use a hash set here since it will hide conceal the
         * presence of duplicate statements in either graph.
         */

        Collection<Statement> c = new LinkedList<Statement>();

        StatementIterator statIter = repo.getStatements(null, null, null);

        while (statIter.hasNext()) {

            Statement stmt = statIter.next();

            c.add(stmt);

        }

        statIter.close();

        return c;

    }

    private static void log(String s) {

        log.info(s);

    }

    static public void assertSameSPOs(SPO[] expected, ISPOIterator actual) {

        assertSameSPOs("", expected, actual);

    }

    static public void assertSameSPOs(String msg, SPO[] expected, ISPOIterator actual) {

        /*
         * clone expected[] and put into the same order as the iterator.
         */
        
        expected = expected.clone();
        
        KeyOrder keyOrder = actual.getKeyOrder();

        Arrays.sort(expected, keyOrder.getComparator());
        
        int i = 0;

        while (actual.hasNext()) {

            if (i >= expected.length) {

                fail(msg + ": The iterator is willing to visit more than "
                        + expected.length + " objects.");

            }

            SPO g = actual.next();
            
            if (!expected[i].equals(g)) {
                
                /*
                 * Only do message construction if we know that the assert will
                 * fail.
                 */
                fail(msg + ": Different objects at index=" + i + ": expected="
                        + expected[i] + ", actual=" + g);
            }

            i++;

        }

        if (i < expected.length) {

            fail(msg + ": The iterator SHOULD have visited " + expected.length
                    + " objects, but only visited " + i + " objects.");

        }

    }

    static public void assertSameStatements(Statement[] expected, StatementIterator actual) {

        assertSameStatements("", expected, actual);

    }

    /**
     * @todo since there is no way to know the natural order for the statement
     *       iterator we can not sort expected into the same order. therefore
     *       this should test for the same statements in any order
     */
    static public void assertSameStatements(String msg, Statement[] expected, StatementIterator actual) {

        int i = 0;

        while (actual.hasNext()) {

            if (i >= expected.length) {

                fail(msg + ": The iterator is willing to visit more than "
                        + expected.length + " objects.");

            }

            Statement g = actual.next();
            
            if (!expected[i].equals(g)) {
                
                /*
                 * Only do message construction if we know that the assert will
                 * fail.
                 */
                fail(msg + ": Different objects at index=" + i + ": expected="
                        + expected[i] + ", actual=" + g);
            }

            i++;

        }

        if (i < expected.length) {

            fail(msg + ": The iterator SHOULD have visited " + expected.length
                    + " objects, but only visited " + i + " objects.");

        }

    }

}
