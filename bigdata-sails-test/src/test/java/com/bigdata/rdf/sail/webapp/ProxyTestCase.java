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
/*
 * Created on Oct 14, 2006
 */

package com.bigdata.rdf.sail.webapp;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import junit.extensions.proxy.IProxyTest;
import junit.framework.Test;

import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.repository.RepositoryResult;

import com.bigdata.journal.AbstractJournalTestCase;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.IPreparedGraphQuery;

/**
 * <p>
 * This class provides proxy delegation logic for abstract methods declared by
 * {@link AbstractJournalTestCase} and is used to extend the set of tests that will be
 * applied to all implementations of the generic object model Java API. If you
 * want to test a new implementation, you MUST extend the
 * {@link AbstractJournalTestCase} instead and implement its abstract methods for your
 * implementation. This class provides an implementation neutral way to add new
 * tests, not a means for testing specific generic object model Java API
 * implementations.
 * </p>
 * <p>
 * In order to add new tests for the generic object model Java APIs, you extend
 * this class and write test methods.
 * </p>
 * 
 * @see AbstractJournalTestCase
 */
public abstract class ProxyTestCase<S extends IIndexManager>
    extends AbstractIndexManagerTestCase<S>
    implements IProxyTest
{

    public ProxyTestCase() {}
    public ProxyTestCase(String name){super(name);}
    
    //************************************************************
    //************************ IProxyTest ************************
    //************************************************************

    private AbstractIndexManagerTestCase<S> m_delegate = null;

    @SuppressWarnings("unchecked")
	@Override
    public void setDelegate(final Test delegate) {

        m_delegate = (AbstractIndexManagerTestCase<S>)delegate;

    }

    @Override
    public Test getDelegate() throws IllegalStateException {

        return m_delegate;

    }

    /**
     * Returns the delegate after first making sure that it is non-null and
     * extends {@link AbstractJournalTestCase}.
     */

    public AbstractIndexManagerTestCase<S> getOurDelegate() {

        if (m_delegate == null) {

            /*
             * This property gives the class name of the concrete instance of
             * AbstractTestSuite that we need to instantiate so that we can run
             * or debug a single test at a time! This is designed to support
             * running or debugging a single test that has failed after running
             * the entire test suite in an IDE such as Eclipse.
             * 
             * Note: We reach out to System.getProperty() and not
             * getProperties() to avoid infinite recursion through
             * getOurDelegate. The only way this makes sense anyway is when you
             * define -DtestClass=... as a JVM property.
             * 
             * @todo document.
             */
            final String testClass = System.getProperty("testClass");
            if (testClass == null) {

                throw new IllegalStateException(
                        "testClass: property not defined, could not configure delegate.");

            }
            try {
                final Class<?> cl = Class.forName(testClass);
                m_delegate = (AbstractIndexManagerTestCase<S>) cl.newInstance();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            // throw new IllegalStateException
            // ( "The delegate is not configured."
            // );

        }

        if (m_delegate instanceof AbstractIndexManagerTestCase) {

            return (AbstractIndexManagerTestCase<S>) m_delegate;

        }

        throw new IllegalStateException("The delegate MUST extend "
                + AbstractIndexManagerTestCase.class.getName() + ", not "
                + m_delegate.getClass().getName());

    }

    //************************************************************
    //********************* proxied methods **********************
    //************************************************************

    /*
     * Note: All methods on the delegate MUST be proxied here or they will be
     * answered by our base class which is the same Class as the delegate, but
     * whose instance fields have not been initialized! (An instance of the
     * proxy is created for each test, while one instance of the delegate serves
     * an entire suite of tests.)
     */
    
    private int startupActiveThreads = 0;
    
    @Override
    public void setUp() throws Exception {

        startupActiveThreads = Thread.currentThread().getThreadGroup().activeCount();
    	
        getOurDelegate().setUp(this);
        
    }

    private static boolean s_checkThreads = true;

    @Override
    public void tearDown() throws Exception {

        getOurDelegate().tearDown(this);
        
        if (s_checkThreads) {

            final ThreadGroup grp = Thread.currentThread().getThreadGroup();
            final int tearDownActiveThreads = grp.activeCount();
            if (startupActiveThreads != tearDownActiveThreads) {
                final Thread[] threads = new Thread[tearDownActiveThreads];
                grp.enumerate(threads);
                final StringBuilder info = new StringBuilder();
                boolean first = true;
                for (Thread t : threads) {
                    if (t == null)
                        continue;
                    if (!first)
                        info.append(',');
                    info.append("[" + t.getName() + "]");
                    first = false;
                }

                final String failMessage = "Threads left active after task"
                        + ": test="
                        + getName()//
                        + ", delegate=" + getOurDelegate().getClass().getName()
                        + ", startupCount=" + startupActiveThreads
                        + ", teardownCount=" + tearDownActiveThreads
                        + ", thisThread=" + Thread.currentThread().getName()
                        + ", threads: " + info;

                if (grp.activeCount() != startupActiveThreads)
                    log.error(failMessage);

                /*
                 * Wait up to 2 seconds for threads to die off so the next test
                 * will run more cleanly.
                 */
                for (int i = 0; i < 20; i++) {
                    Thread.sleep(100);
                    if (grp.activeCount() != startupActiveThreads)
                        break;
                }

            }

        }

        super.tearDown();

    }

    @Override
    public Properties getProperties() {
        return getOurDelegate().getProperties();
    }

    @Override
    public S getIndexManager() {
        return getOurDelegate().getIndexManager();
    }

}
