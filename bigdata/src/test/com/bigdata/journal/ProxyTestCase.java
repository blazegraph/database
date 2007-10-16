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

import java.util.Properties;

import junit.extensions.proxy.IProxyTest;
import junit.framework.Test;
import junit.framework.TestCase2;

/**
 * <p>
 * This class provides proxy delegation logic for abstract methods declared by
 * {@link AbstractTestCase} and is used to extend the set of tests that will be
 * applied to all implementations of the generic object model Java API. If you
 * want to test a new implementation, you MUST extend the
 * {@link AbstractTestCase} instead and implement its abstract methods for your
 * implementation. This class provides an implementation neutral way to add new
 * tests, not a means for testing specific generic object model Java API
 * implementations.
 * </p>
 * <p>
 * In order to add new tests for the generic object model Java APIs, you extend
 * this class and write test methods.
 * </p>
 * 
 * @see AbstractTestCase
 */

public abstract class ProxyTestCase
    extends AbstractTestCase
    implements IProxyTest
{

    public ProxyTestCase() {}
    public ProxyTestCase(String name){super(name);}
    
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
     * extends {@link AbstractTestCase}.
     */

    public AbstractTestCase getOurDelegate() {

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

        if (m_delegate instanceof AbstractTestCase) {

            return (AbstractTestCase) m_delegate;

        }

        throw new IllegalStateException("The delegate MUST extend "
                + AbstractTestCase.class.getName() + ", not "
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
    
    public void setUp() throws Exception {
        getOurDelegate().setUp(this);
    }

    public void tearDown() throws Exception {
        getOurDelegate().tearDown(this);
    }

    public Properties getProperties() {
        return getOurDelegate().getProperties();
    }

    public Journal reopenStore(Journal store) {
        return getOurDelegate().reopenStore(store);
    }

}
