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
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Random;

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

        log.info("\n\n================:BEGIN:" + testCase.getName()
                + ":BEGIN:====================");

    }

    /**
     * Invoked from {@link TestCase#tearDown()} for each test in the suite.
     */
    public void tearDown(ProxyTestCase testCase) throws Exception {

        log.info("\n================:END:" + testCase.getName()
                + ":END:====================\n");

        deleteTestFile();
        
    }
    
    public void tearDown() throws Exception {
        
        super.tearDown();
        
        deleteTestFile();
        
    }

    /**
     * Note: your unit must close the store for delete to work.
     */
    protected void deleteTestFile() {

        if(m_properties==null) return; // never requested.
        
        String val;
        
        val = (String) m_properties.getProperty(Options.FILE);
        
        if(val!= null) {
        
            File file = new File(val);
            
            if(file.exists()) {

                val = (String) m_properties.getProperty(Options.DELETE_ON_EXIT);
        
                if(val==null) {
                    
                    val = (String) m_properties.getProperty(Options.DELETE_ON_CLOSE);
                    
                }
                
                if(Boolean.parseBoolean(val)) {
                    
                    System.err.println("Attempting to delete file: "+file);
                    
                    if(!file.delete()) {
                    
                        log.warn("Could not delete file: "+file);

                    }
                    
                }
            
            }
            
        }

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
//          m_properties = new Properties( m_properties );
         
            /*
             * Wrap up the cached properties so that they are not modifable by the
             * caller (no side effects between calls).
             */
            
            /*
             * Use a temporary file for the test. Such files are always deleted when
             * the journal is closed or the VM exits.
             */
            m_properties.setProperty(Options.CREATE_TEMP_FILE,"true");
//            m_properties.setProperty(Options.DELETE_ON_CLOSE,"true");
            m_properties.setProperty(Options.DELETE_ON_EXIT,"true");
            
        }        
        
        return m_properties;
        
    }

    /**
     * Re-open the same backing store.
     * 
     * @param store
     *            the existing store.
     * 
     * @return A new store.
     * 
     * @exception Throwable
     *                if the existing store is not closed, e.g., from failure to
     *                obtain a file lock, etc.
     */
    protected Journal reopenStore(Journal store) {
        
        // close the store.
        store.close();
        
        if(!store.isStable()) {
            
            throw new UnsupportedOperationException("The backing store is not stable");
            
        }
        
        // Note: clone to avoid modifying!!!
        Properties properties = (Properties)getProperties().clone();
        
        // Turn this off now since we want to re-open the same store.
        properties.setProperty(Options.CREATE_TEMP_FILE,"false");
        
        // The backing file that we need to re-open.
        File file = store.getFile();
        
        assertNotNull(file);
        
        // Set the file property explictly.
        properties.setProperty(Options.FILE,file.toString());
        
        return new Journal( properties );
        
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

    /**
//     * <p>
//     * Return the name of a journal file to be used for a unit test. The file is
//     * created using the temporary file creation mechanism, but it is then
//     * deleted. Ideally the returned filename is unique for the scope of the
//     * test and will not be reported by the journal as a "pre-existing" file.
//     * </p>
//     * <p>
//     * Note: This method is not advised for performance tests in which the disk
//     * allocation matters since the file is allocated in a directory choosen by
//     * the OS.
//     * </p>
//     * 
//     * @param properties
//     *            The configured properties. This is used to extract metadata
//     *            about the journal test configuration that is included in the
//     *            generated filename. Therefore this method should be invoked
//     *            after you have set the properties, or at least the
//     *            {@link Options#BUFFER_MODE}.
//     * 
//     * @return The unique filename.
//     * 
//     * @see {@link #getProperties()}, which sets the "deleteOnClose" flag for
//     *      unit tests.
//     */
//    protected String getTestJournalFile(Properties properties) {
//        
//        return getTestJournalFile(getName(),properties);
//        
//    }
//
//    static public String getTestJournalFile(String name,Properties properties) {
//
//        // Used to name the file.
//        String bufferMode = properties.getProperty(Options.BUFFER_MODE);
//        
//        // Used to name the file.
//        if( bufferMode == null ) bufferMode = "default";
//        
//        try {
//
//            // Create the temp. file.
//            File tmp = File.createTempFile("test-" + bufferMode + "-"
//                    + name + "-", ".jnl");
//            
//            // Delete the file otherwise the Journal will attempt to open it.
//            if (!tmp.delete()) {
//
//                throw new RuntimeException("Unable to remove empty test file: "
//                        + tmp);
//
//            }
//
//            // make sure that the file is eventually removed.
//            tmp.deleteOnExit();
//            
//            return tmp.toString();
//            
//        } catch (IOException ex) {
//            
//            throw new RuntimeException(ex);
//            
//        }
//        
//    }
//
//    /**
//     * Version of {@link #deleteTestJournalFile(String)} that obtains the name
//     * of the journal file from the {@link Options#FILE} property (if any) on
//     * {@link #getProperties()}.
//     */
//    protected void deleteTestJournalFile() {
//    
//        String filename = getProperties().getProperty(Options.FILE);
//        
//        if( filename != null ) {
//            
//            deleteTestJournalFile(filename);
//            
//        }
//        
//    }
//    
//    /**
//     * Delete the test file (if any). Note that test files are NOT created when
//     * testing the {@link BufferMode#Transient} journal. A warning message that
//     * the file could not be deleted generally means that you forgot to close
//     * the journal in your test.
//     * 
//     * @param filename
//     *            The filename (optional).
//     */
//    protected void deleteTestJournalFile(String filename) {
//        
//        if( filename == null ) return;
//        
//        try {
//            
//            File file = new File(filename);
//            
//            if ( file.exists() && ! file.delete()) {
//                
//                System.err.println("Warning: could not delete: " + file.getAbsolutePath());
//                
//            }
//            
//        } catch (Throwable t) {
//            
//            System.err.println("Warning: " + t);
//            
//        }
//        
//    }

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
    public static void assertEquals(byte[] expected, ByteBuffer actual ) {

        if( expected == null ) throw new IllegalArgumentException();
        
        if( actual == null ) fail("actual is null");
        
        if( actual.hasArray() && actual.arrayOffset() == 0 ) {
            
            assertEquals(expected,actual.array());
            
            return;
            
        }
        
        /* Create a read-only view on the buffer so that we do not mess with
         * its position, mark, or limit.
         */
        actual = actual.asReadOnlyBuffer();
        
        final int len = actual.remaining();
        
        final byte[] actual2 = new byte[len];
        
        actual.get(actual2);

        assertEquals(expected,actual2);
        
    }

    /**
     * A random number generated - the seed is NOT fixed.
     */
    protected Random r = new Random();

    /**
     * Returns random data that will fit in N bytes. N is choosen randomly in
     * 1:1024.
     * 
     * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code> of
     *         random length and having random contents.
     */
    public ByteBuffer getRandomData() {
        
        final int nbytes = r.nextInt(1024) + 1;
        
        byte[] bytes = new byte[nbytes];
        
        r.nextBytes(bytes);
        
        return ByteBuffer.wrap(bytes);
        
    }
    
}
