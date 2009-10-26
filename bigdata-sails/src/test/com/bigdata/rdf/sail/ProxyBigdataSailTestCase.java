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

package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Properties;

import junit.extensions.proxy.IProxyTest;
import junit.framework.Test;

import org.openrdf.model.Resource;


/**
 * <p>
 * This class provides proxy delegation logic for abstract methods declared by
 * {@link AbstractBigdataSailTestCase} and is used to extend the set of tests
 * that will be applied to all implementations of the generic object model Java
 * API. If you want to test a new implementation, you MUST extend the
 * {@link AbstractBigdataSailTestCase} instead and implement its abstract
 * methods for your implementation. This class provides an implementation
 * neutral way to add new tests, not a means for testing specific generic object
 * model Java API implementations.
 * </p>
 * <p>
 * In order to add new tests for the generic object model Java APIs, you extend
 * this class and write test methods.
 * </p>
 * 
 * @see AbstractBigdataTestCase
 */
public abstract class ProxyBigdataSailTestCase
    extends AbstractBigdataSailTestCase
    implements IProxyTest
{

    public ProxyBigdataSailTestCase() {}
    public ProxyBigdataSailTestCase(String name){super(name);}
    
    //************************************************************
    //************************ IProxyTest ************************
    //************************************************************

    private Test m_delegate = null;

    public void setDelegate(Test delegate) {

        m_delegate = delegate;

    }

    public Test getDelegate() throws IllegalStateException {

        return m_delegate;

    }

    /**
     * Returns the delegate after first making sure that it is non-null and
     * extends {@link AbstractBigdataTestCase}.
     */
    public AbstractBigdataSailTestCase getOurDelegate() {

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
            String testClass = System.getProperty("testClass");
            if (testClass == null) {

                throw new IllegalStateException(
                        "testClass: property not defined, could not configure delegate.");

            }
            try {
                Class cl = Class.forName(testClass);
                m_delegate = (Test) cl.newInstance();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            // throw new IllegalStateException
            // ( "The delegate is not configured."
            // );

        }

        if (m_delegate instanceof AbstractBigdataSailTestCase) {

            return (AbstractBigdataSailTestCase) m_delegate;

        }

        throw new IllegalStateException("The delegate MUST extend "
                + AbstractBigdataSailTestCase.class.getName() + ", not "
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
    
    protected void setUp() throws Exception {
        getOurDelegate().setUp(this);
    }

    protected void tearDown() throws Exception {
        getOurDelegate().tearDown(this);
    }

    /**
     * The properties as configured by the delegate.
     */
    public Properties getProperties() {
        return getOurDelegate().getProperties();
    }

    /**
     * Create a SAIL using the delegate and using the properties as configured
     * by the delegate.
     */
    protected BigdataSail getSail() {
        return getOurDelegate().getSail(getProperties());
    }
    
    /**
     * Create a SAIL using the delegate using the specified properties
     * (typically overriding one or more properties).
     */
    protected BigdataSail getSail(Properties properties) {
        return getOurDelegate().getSail(properties);
    }
    
    /**
     * Close and then re-open the SAIL.
     */
    protected BigdataSail reopenSail(BigdataSail sail) {
        return getOurDelegate().reopenSail(sail);
    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     */
    static public void assertSameIterationAnyOrder(
            final Resource[] expected,
            final CloseableIteration<?, ? extends Exception> actual)
            throws Exception {

        assertSameIterationAnyOrder("", expected, actual);

    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     */
    @SuppressWarnings("unchecked")
    static public void assertSameIterationAnyOrder(String msg,
            final Resource[] expected,
            final CloseableIteration<?, ? extends Exception> actual)
            throws Exception {

        // Populate a map that we will use to realize the match and
        // selection without replacement logic.

        final int nrange = expected.length;

        final java.util.Map range = new java.util.HashMap();

        for (int j = 0; j < nrange; j++) {

            range.put(expected[j], expected[j]);

        }

        // Do selection without replacement for the objects visited by
        // iterator.

        for (int j = 0; j < nrange; j++) {

            if (!actual.hasNext()) {

                fail(msg + ": Index exhausted while expecting more object(s)"
                        + ": index=" + j);

            }

            final Object actualObject = actual.next();

            if (range.remove(actualObject) == null) {

                fail("Object not expected" + ": index=" + j + ", object="
                        + actualObject);

            }

        }

        if (actual.hasNext()) {

            fail("Iterator will deliver too many objects.");

        }

    }

}
