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

package com.bigdata.journal;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;

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
abstract public class AbstractJournalTestCase
    extends AbstractIndexManagerTestCase<Journal>
{

    //
    // Constructors.
    //

    public AbstractJournalTestCase() {}
    
    public AbstractJournalTestCase(String name) {super(name);}

    //************************************************************
    //************************************************************
    //************************************************************
    
    /**
     * Invoked from {@link TestCase#setUp()} for each test in the suite.
     */
    public void setUp(ProxyTestCase testCase) throws Exception {

        super.setUp(testCase);
        
//        if(log.isInfoEnabled())
//        log.info("\n\n================:BEGIN:" + testCase.getName()
//                + ":BEGIN:====================");

    }

    /**
     * Invoked from {@link TestCase#tearDown()} for each test in the suite.
     */
    public void tearDown(ProxyTestCase testCase) throws Exception {

        super.tearDown(testCase);

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

    final protected Journal getStore(final Properties properties) {
        
        return new Journal(properties);
        
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
     *                if the existing store is closed or if the store can not be
     *                re-opened, e.g., from failure to obtain a file lock, etc.
     */
    protected Journal reopenStore(final Journal store) {
        
        // close the store.
        store.close();
        
        if(!store.isStable()) {
            
            throw new UnsupportedOperationException("The backing store is not stable");
            
        }
        
        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties)getProperties().clone();
        
        // Turn this off now since we want to re-open the same store.
        properties.setProperty(Options.CREATE_TEMP_FILE,"false");
        
        // The backing file that we need to re-open.
        final File file = store.getFile();
        
        assertNotNull(file);
        
        // Set the file property explictly.
        properties.setProperty(Options.FILE,file.toString());
        
        return new Journal( properties );
        
    }

}
