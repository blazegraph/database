/*

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
/*
 * Created on Oct 2, 2008
 */

package com.bigdata.rdf.sail.webapp;

import java.util.Properties;

import junit.framework.TestCase;
import junit.framework.TestCase2;

import com.bigdata.journal.IIndexManager;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractIndexManagerTestCase<S extends IIndexManager> extends TestCase2 {

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
    public void setUp(ProxyTestCase<S> testCase) throws Exception {

        if(log.isInfoEnabled())
        log.info("\n\n================:BEGIN:" + testCase.getName()
                + ":BEGIN:====================");

    }

    /**
     * Invoked from {@link TestCase#tearDown()} for each test in the suite.
     */
    public void tearDown(ProxyTestCase<S>  testCase) throws Exception {

        if(log.isInfoEnabled())
        log.info("\n================:END:" + testCase.getName()
                + ":END:====================\n");
      
		/*
		 * Note: The test suite runs against a single IIndexManager so we do not
		 * want to check this after each test in the suite.
		 */
//        TestHelper.checkJournalsClosed(testCase, this);

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

    abstract protected S getIndexManager();
    
    

    
//    /**
//     * Open/create an {@link IIndexManager} using the given properties.
//     */
//    abstract protected S getStore(Properties properties);
//    
//    /**
//     * Close and then re-open an {@link IIndexManager} backed by the same
//     * persistent data.
//     * 
//     * @param store
//     *            the existing store.
//     * 
//     * @return A new store.
//     * 
//     * @exception Throwable
//     *                if the existing store is closed or if the store can not be
//     *                re-opened, e.g., from failure to obtain a file lock, etc.
//     */
//    abstract protected S reopenStore(S store);
    
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

	public void tearDownAfterSuite() {
	}
    
}
