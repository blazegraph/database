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

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import junit.framework.TestCase;
import junit.framework.TestCase2;

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

//    /**
//     * The name of the optional property whose boolean value indicates whether
//     * or not {@link #dropStore()} should be invoked before each test
//     * (default is "false").
//     */
//    public static final String dropBeforeTest = "dropBeforeTest";
//
//    /**
//     * The name of the optional property whose boolean value indicates whether
//     * or not {@link #dropStore()} should be invoked before each test
//     * (default is "false" ).
//     */
//    public static final String dropAfterTest = "dropAfterTest";

    //
    // Constructors.
    //

    public AbstractTestCase() {}
    
    public AbstractTestCase(String name) {super(name);}

//    //
//    // Test suite.
//    //
//    
//    /**
//     * <p>
//     * Appends to the <i>suite</i> the tests defined by this module.
//     * </p>
//     * 
//     * @param suite
//     *            A suite - may be empty.
//     */
//
//    public static void addGenericSuite(ProxyTestSuite suite) {
//
//        if (suite == null) {
//
//            throw new IllegalArgumentException(
//                    "The ProxyTestSuite may not be null.");
//
//        }
//
//        /*
//         * Create sub-suites (proxy suites) for each package defined in this
//         * module.
//         */
////        ProxyTestSuite suite2 = new ProxyTestSuite(suite.getDelegate(),"org.CognitiveWeb.generic");
////        ProxyTestSuite suite3 = new ProxyTestSuite(suite.getDelegate(),"org.CognitiveWeb.generic.gql");
////        ProxyTestSuite suite4 = new ProxyTestSuite(suite.getDelegate(),"org.CognitiveWeb.generic.isomorph");
//        
//        /*
//         * Test suites for the core generic apis.
//         */
//
////        suite2.addTestSuite(IGenericProxyTestCase.class);
//
//        /*
//         * Combine into the parent test suite using a structure that mirrors the
//         * package structure.
//         */
//        
////        suite2.addTest(suite3);
////        suite2.addTest(suite4);
////        suite.addTest(suite2);
//        
//    }

    //************************************************************
    //************************************************************
    //************************************************************
    
    /**
     * Invoked from {@link TestCase#setUp()} for each test in the suite.
     */
//  * This
//  * method writes a test header into the log and is responsible for invoking
//  * {@link #dropStore()} if the optional boolean property
//  * {@link #dropBeforeTest} was specified and has the value "true".

    public void setUp(ProxyTestCase testCase) throws Exception {

        log.info("\n\n================:BEGIN:" + testCase.getName()
                + ":BEGIN:====================");

//        if (new Boolean(getProperties().getProperty(dropBeforeTest,
//                "false")).booleanValue()) {
//
//            try {
//                dropStore();
//            } catch (Throwable ex) {
//                log.error("Could not drop store.", ex);
//            }
//
//        }

    }

    /**
     * Invoked from {@link TestCase#tearDown()} for each test in the suite.
     */
//    * This
//    * method writes a test trailer into the log and is responsible for invoking
//    * {@link #dropStore()} if the optional boolean property
//    * {@link #dropAfterTest} was specified and has the value "true".

    public void tearDown(ProxyTestCase testCase) throws Exception {

//        if (isStoreOpen()) {
//
//            log.warn("object manager not closed: test=" + testCase.getName()
//                    + ", closing now.");
//
//            try {
//                closeStore();
//            } catch (Throwable ex) {
//                log.error("Could not close object manager.", ex);
//            }
//
//        }
//
//        if (new Boolean(getProperties().getProperty(dropAfterTest,
//                "false")).booleanValue()) {
//
//            try {
//                dropStore();
//            } catch (Throwable ex) {
//                log.error("Could not drop store.", ex);
//            }
//
//        }

        log.info("\n================:END:" + testCase.getName()
                + ":END:====================\n");

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
        }
        /*
         * Wrap up the cached properties so that they are not modifable by the
         * caller (no side effects between calls).
         */
        return new Properties( m_properties );
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
    
//    //
//    // ObjectManager
//    //
//    
//    private IObjectManager m_om;
//    
//    /**
//     * <p>
//     * Return the configured object manager. A new object manager will be
//     * created if there is not one that is currently configured.
//     * </p>
//     * 
//     * @return The configured object manager.
//     */
//    public IObjectManager getObjectManager() {
//        checkIfProxy();
//        if( m_om == null ) {
//            m_om = openStore();
//        }
//        return m_om;
//    }
//    
//    /**
//     * <p>
//     * Closes the current object manager and then opens a new object manager.
//     * onto the configured store.
//     * </p>
//     * 
//     * @return A new object manager.
//     */
//    public IObjectManager reopenStore() {
//        checkIfProxy();
//        closeStore();
//        m_om = null; // make sure that this is cleared.
//        m_om = openStore(); // make sure that this is re-assigned.
//        return m_om;
//    }
//
//    /**
//     * <p>
//     * Opens and returns an object manager instance for the configured
//     * persistence layer and store.  The object manager is configured
//     * using the properties returned by {@link #getProperties()}.
//     * </p>
//     * 
//     * @return A new object manager instance.
//     * 
//     * @exception IllegalStateException
//     *                if there is a current object manager.
//     */
//    protected IObjectManager openStore() {
//        checkIfProxy();
//        if( m_om != null ) {
//            throw new IllegalStateException("ObjectManager exists.");
//        }
//        m_om = ObjectManagerFactory.INSTANCE.newInstance(getProperties());
//        return m_om;
//    }
//
//    /**
//     * <p>
//     * Closes the current object manager.
//     * </p>
//     * 
//     * @exception IllegalStateException
//     *                if the object manager is not configured.
//     */
//    protected void closeStore() {
//        checkIfProxy();
//        if( m_om == null ) {
//            throw new IllegalStateException("ObjectManager does not exist.");
//        }
//        m_om.close();
//        m_om = null;
//    }
//    
//    /**
//     * <p>
//     * Return true iff the object manager exists.
//     * </p>
//     * 
//     * @return True if there is a configured object manager.
//     */
//    protected boolean isStoreOpen() {
//        checkIfProxy();
//        return m_om != null;
//    }
//
//    /**
//     * <p>
//     * Drop the configured database in use for the tests. This method may be
//     * automatically invoked before and/or after each test by declaring the
//     * appropriate property.
//     * </p>
//     * <p>
//     * The semantics of "drop" depend on the GOM implementation and persistence
//     * layer under test and can range from clearing a transient object manager,
//     * to deleting the files corresponding to the store on disk, to dropping a
//     * test database in a federation. As a rule, the object manager must be
//     * closed when this method is invoked.
//     * </p>
//     * 
//     * @see #dropBeforeTest
//     * @see #dropAfterTest
//     */
//    abstract protected void dropStore();
//    
//    //
//    // Unique property name factory.
//    //
//    
//    private Random r = new Random();
//    
//    /**
//     * <p>
//     * A property name is derived from the test name plus a random integer to
//     * avoid side effects. The store is NOT tested to verify that this property
//     * name is unique, so it is possible that tests will occasionally fail
//     * through rare collisions with existing property names.
//     * </p>
//     * <p>
//     * Note: This method also gets used to generate unique association names and
//     * object names.
//     * </p>
//     */
//    protected String getUniquePropertyName() {
//        return getName()+"-"+r.nextInt();
//    }
//    
//    /**
//     * Similar to {@link #getUniquePropertyName()} but embeds <i>name</i> in
//     * the returned value.
//     * 
//     * @see #getUniquePropertyName()
//     */
//    protected String getUniquePropertyName(String name) {
//        return getName()+"-"+name+"-"+r.nextInt();
//    }


    //************************************************************
    //************************************************************
    //************************************************************
    //
    // Test helpers.
    //

    /**
     * <p>
     * Return the name of a journal file to be used for a unit test. The file is
     * created using the temporary file creation mechanism, but it is then
     * deleted. Ideally the returned filename is unique for the scope of the
     * test and will not be reported by the journal as a "pre-existing" file.
     * </p>
     * <p>
     * Note: This method is not advised for performance tests in which the disk
     * allocation matters since the file is allocated in a directory choosen by
     * the OS.
     * </p>
     * 
     * @return The unique filename.
     * 
     * @throws IOException
     */
    
    String getTestJournalFile() throws IOException {

        File tmp = File.createTempFile("test-"+getName()+"-", ".jnl");
        
        if( ! tmp.delete() ) {
            
            throw new RuntimeException("Unable to remove empty test file: "
                    + tmp);
            
        }
        
        return tmp.toString();
        
    }

    /**
     * Delete the test file (if any). Note that test files are NOT created when
     * testing the {@link BufferMode#Transient} journal. A warning message that
     * the file could not be deleted generally means that you forgot to close
     * the journal in your test.
     */
    
    void deleteTestJournalFile(String filename) {
        
        try {
            
            File file = new File(filename);
            
            if ( file.exists() && ! file.delete()) {
                
                System.err.println("Warning: could not delete: " + file.getAbsolutePath());
                
            }
            
        } catch (Throwable t) {
            
            System.err.println("Warning: " + t);
            
        }
        
    }

    /**
     * Helper method verifies that the contents of <i>actual</i> from
     * position() to limit() are consistent with the expected byte[]. A
     * read-only view of <i>actual</i> is used to avoid side effects on the
     * position, mark or limit properties of the buffer.
     * 
     * @param expected
     *            Non-null byte[].
     * @param actual
     *            Buffer.
     */
    public void assertEquals(byte[] expected, ByteBuffer actual ) {

        if( expected == null ) throw new IllegalArgumentException();
        
        if( actual == null ) fail("actual is null");
        
        /* Create a read-only view on the buffer so that we do not mess with
         * its position, mark, or limit.
         */
        actual = actual.asReadOnlyBuffer();
        
        final int len = actual.remaining();
        
        final byte[] actual2 = new byte[len];
        
        actual.get(actual2);

        assertEquals(expected,actual2);
        
    }

}
