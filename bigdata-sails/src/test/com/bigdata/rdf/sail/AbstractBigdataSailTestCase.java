/*
 * Copyright SYSTAP, LLC 2006-2008.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Apr 15, 2008
 */

package com.bigdata.rdf.sail;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;
import junit.framework.TestCase2;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.store.IRawTripleStore;

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
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBigdataSailTestCase extends TestCase2 {

    //
    // Constructors.
    //
    
    /**
     * 
     */
    public AbstractBigdataSailTestCase() {
    }

    /**
     * @param name
     */
    public AbstractBigdataSailTestCase(String name) {
        super(name);
    }

    //************************************************************
    //************************************************************
    //************************************************************
    
    /**
     * Invoked from {@link TestCase#setUp()} for each test in the suite.
     */
    protected void setUp(ProxyBigdataSailTestCase testCase) throws Exception {

        begin = System.currentTimeMillis();
        
        if (log.isInfoEnabled())
            log.info("\n\n================:BEGIN:" + testCase.getName()
                    + ":BEGIN:====================");

    }

    /**
     * Invoked from {@link TestCase#tearDown()} for each test in the suite.
     */
    protected void tearDown(ProxyBigdataSailTestCase testCase) throws Exception {

        long elapsed = System.currentTimeMillis() - begin;
        
        if (log.isInfoEnabled())
            log.info("\n================:END:" + testCase.getName() + " ("
                    + elapsed + "ms):END:====================\n");

    }
    
    private long begin;

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

            // transient means that there is nothing to delete after the test.
            m_properties.setProperty(Options.BUFFER_MODE,BufferMode.Transient.toString());
            
            // disregard the inherited properties.
//            m_properties = new Properties();
            
//            m_properties = new Properties( m_properties );

            m_properties.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());
//            m_properties.setProperty(Options.BUFFER_MODE,BufferMode.Transient.toString());

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
        
        return new Properties(m_properties);
        
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
        
        if( this instanceof ProxyBigdataSailTestCase ) {
            
            throw new AssertionError();
            
        }
        
    }

    //************************************************************
    //************************************************************
    //************************************************************
    //
    // Test helpers.
    //

    protected static final long N = IRawTripleStore.N;

    protected static final long NULL = IRawTripleStore.NULL;
    
    /*
     * test fixtures
     */
    
//    private BigdataSail sail;
    
//    protected void setUp(ProxyBigdataSailTestCase testCase) throws Exception {
//     
//        sail = new BigdataSail(getProperties());
//        
////        sail.database.create();
//        
//    }
//    
//    protected void tearDown(ProxyBigdataSailTestCase testCase) throws Exception {
//        
//        if (sail != null) {
//
//            sail.shutDown();
//            
//        }
//                
//    }
    
    abstract protected BigdataSail getSail(Properties properties);

    abstract protected BigdataSail reopenSail(BigdataSail sail);

    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself.
     * 
     * @param f
     *            A file or directory.
     */
    protected void recursiveDelete(File f) {
        
        if(f.isDirectory()) {
            
            File[] children = f.listFiles();
            
            for(int i=0; i<children.length; i++) {
                
                recursiveDelete( children[i] );
                
            }
            
        }
        
        if (f.exists()) {

            log.warn("Removing: " + f);

            if (!f.delete()) {

                throw new RuntimeException("Could not remove: " + f);

            }

        }

    }
    
}
