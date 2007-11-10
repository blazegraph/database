/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
/*
 * Created on Oct 14, 2006
 */

package com.bigdata.rdf.store;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import junit.framework.TestCase2;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.sesame.sail.StatementIterator;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.isolation.IIsolatableIndex;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Options;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOComparator;
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
             * If an explicit filename is not specified...
             */
            if(m_properties.get(Options.FILE)==null) {

                /*
                 * Use a temporary file for the test. Such files are always deleted when
                 * the journal is closed or the VM exits.
                 */

                m_properties.setProperty(Options.CREATE_TEMP_FILE,"true");
            
                m_properties.setProperty(Options.DELETE_ON_EXIT,"true");
                
            }
            
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

    /**
     * Verify that the iterator visits the expected {@link SPO}s in any order
     * without duplicates.
     * 
     * @param store
     *            Used to resolve term identifiers for messages.
     * @param expected2
     * @param itr
     */
    static public void assertSameSPOsAnyOrder(AbstractTripleStore store, SPO[] expected2, ISPOIterator itr) {
        
        Map<SPO,SPO> expected = new TreeMap<SPO,SPO>(SPOComparator.INSTANCE);

        for(SPO tmp : expected2 ) {
            
            expected.put(tmp,tmp);
            
        }
        
        int i = 0;
        
        while(itr.hasNext()) {
            
            SPO actualSPO = itr.next();
            
            log.info("actual: "+actualSPO.toString(store));
            
            SPO expectedSPO = expected.remove(actualSPO);

            if(expectedSPO==null) {
                
                fail("Not expecting: "+actualSPO.toString(store)+" at index="+i);
                
            }

//            log.info("expected: "+expectedSPO.toString(store));

            assertEquals(expectedSPO.type, actualSPO.type);
            
            i++;
            
        }
        
        if(!expected.isEmpty()) {
            
            // @todo convert term identifiers before rendering.
            log.info("Iterator empty but still expecting: "+expected.values());
            
            fail("Expecting: "+expected.size()+" more statements");
            
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
