/*

 Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Oct 2, 2008
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

import junit.framework.TestCase;

import com.bigdata.io.TestCase3;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractIndexManagerTestCase<S extends IIndexManager> extends TestCase3 {

    protected final static boolean INFO = log.isInfoEnabled();

    protected final static boolean DEBUG = log.isDebugEnabled();
    
    //
    // Constructors.
    //

    public AbstractIndexManagerTestCase() {}
    
    public AbstractIndexManagerTestCase(String name) {super(name);}

    //************************************************************
    //************************************************************
    //************************************************************
    
    /**
     * Invoked from {@link TestCase#setUp()} for each test in the suite.
     */
    public void setUp(ProxyTestCase testCase) throws Exception {

        if(INFO)
        log.info("\n\n================:BEGIN:" + testCase.getName()
                + ":BEGIN:====================");

    }

    /**
     * Invoked from {@link TestCase#tearDown()} for each test in the suite.
     */
    public void tearDown(ProxyTestCase testCase) throws Exception {

        if(INFO)
        log.info("\n================:END:" + testCase.getName()
                + ":END:====================\n");
        
    }
    
    public void tearDown() throws Exception {
        
        super.tearDown();
        
    }

    //
    // Properties
    //

    @Override
    public Properties getProperties() {
        
        return super.getProperties();
        
    }
    
    /**
     * Open/create an {@link IIndexManager} using the given properties.
     */
    abstract protected S getStore(Properties properties);
    
    /**
     * Close and then re-open an {@link IIndexManager} backed by the same
     * persistent data.
     * 
     * @param store
     *            the existing store.
     * 
     * @return A new store.
     * 
     * @exception Throwable
     *                if the existing store is closed or if the store can not be
     *                re-opened, e.g., from failure to obtain a file lock, etc.
     */
    abstract protected S reopenStore(S store);
    
    /**
     * This method is invoked from methods that MUST be proxied to this class.
     * {@link GenericProxyTestCase} extends this class, as do the concrete
     * classes that drive the test suite for specific GOM integration test
     * configuration. Many methods on this class must be proxied from
     * {@link GenericProxyTestCase} to the delegate. Invoking this method from
     * the implementations of those methods in this class provides a means of
     * catching omissions where the corresponding method is NOT being delegated.
     * Failure to delegate these methods means that you are not able to share
     * properties or object manager instances across tests, which means that you
     * can not do configuration-based testing of integrations and can also wind
     * up with mutually inconsistent test fixtures between the delegate and each
     * proxy test.
     */
    
    final protected void checkIfProxy() {
        
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

//    /**
//     * Helper method verifies that the contents of <i>actual</i> from
//     * position() to limit() are consistent with the expected byte[]. A
//     * read-only view of <i>actual</i> is used to avoid side effects on the
//     * position, mark or limit properties of the buffer.
//     * 
//     * @param expected
//     *            Non-null byte[].
//     * @param actual
//     *            Buffer.
//     */
//    public static void assertEquals(final byte[] expected, ByteBuffer actual) {
//
//        if (expected == null)
//            throw new IllegalArgumentException();
//
//        if (actual == null)
//            fail("actual is null");
//
//        if (actual.hasArray() && actual.arrayOffset() == 0
//                && actual.position() == 0
//                && actual.limit() == actual.capacity()) {
//
//            assertEquals(expected, actual.array());
//
//            return;
//
//        }
//        
//        /* Create a read-only view on the buffer so that we do not mess with
//         * its position, mark, or limit.
//         */
//        actual = actual.asReadOnlyBuffer();
//        
//        final int len = actual.remaining();
//        
//        final byte[] actual2 = new byte[len];
//        
//        actual.get(actual2);
//
//        assertEquals(expected,actual2);
//        
//    }

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
        
        final byte[] bytes = new byte[nbytes];
        
        r.nextBytes(bytes);
        
        return ByteBuffer.wrap(bytes);
        
    }
    
    /**
     * Test helper evaluates a {@link Callable} and fails unless the expected
     * exception is thrown. This is typically used to perform correct rejection
     * tests for methods.
     * 
     * @param c
     *            The {@link Callable}.
     * @param expected
     *            The expected exception.
     * 
     * @todo refactor into junit-ext.
     */
    protected void fail(final Callable c,
            final Class<? extends Throwable> expected) {
        
        if (c == null)
            throw new IllegalArgumentException();

        if (expected == null)
            throw new IllegalArgumentException();
        
        try {
            
            c.call();
            
        } catch (Throwable t) {
            
            if (t.getClass().isAssignableFrom(expected)) {
            
                if (INFO)
                    log.info("Ignoring expected exception: " + t);
                
                return;
            
            } else {
        
                fail("Expecting: " + expected + ", not " + t, t);
                
            }

        }

        fail("Expecting: " + expected);
        
    }
    
}
