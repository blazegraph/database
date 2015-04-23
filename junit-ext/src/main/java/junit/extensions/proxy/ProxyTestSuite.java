/* 
 * Licensed to the SYSTAP, LLC under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * SYSTAP, LLC licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed  under the  License is distributed on an "AS IS" BASIS,
 * WITHOUT  WARRANTIES OR CONDITIONS  OF ANY KIND, either  express  or
 * implied.
 * 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package junit.extensions.proxy;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.log4j.Logger;

/**
 * <p>
 * A simple wrapper around {@link TestSuite} that permits the caller to specify
 * the delegate {@link Test} for either directly or recursively contained
 * {@link IProxyTest}s added to a {@link ProxyTestSuite}. There are three cases
 * for junit:
 * <ol>
 * <li>An instantiated test. This is an instance of some class that extends
 * {@link TestCase}. If the class implements {@link IProxyTest} then the
 * <i>delegate </i> will be set on the test instance and will be available when
 * that test runs.</li>
 * <li>A test case. This is a class named to
 * {@link TestSuite#addTestSuite(java.lang.Class)}. That method scans the test
 * case class and generates an instantiated test for each conforming test method
 * identified in the test case. The delegate is then set per above.</li>
 * <li>A test suite. The delegate declared in a {@link ProxyTestSuite}
 * constructor will be flowed down to each test added either directly or
 * indirectly to that {@link ProxyTestSuite}. The various constructors all
 * invoke this method to flow down the delegate. In addition, the
 * {@link #addTest(Test)} and {@link #addTestSuite(Class)} methods also
 * invoke this method to flow down the delegate to instantiated tests that
 * implement {@link IProxyTest}.</li>
 * </ol>
 * </p>
 * <p>
 * The only case when flow down does not occur is when you have created a
 * standard test suite, addeded it to a proxy test suite, and <em>then</em>
 * you add additional tests to the standard test suite. There is no event
 * notification model in junit and this action is not noticed by the {@link
 * ProxyTestSuite}.
 * </p>
 */

public class ProxyTestSuite
    extends TestSuite
{

    /**
     * The {@link Logger} is named for this class.
     */

    protected static final Logger log = Logger.getLogger
	( ProxyTestSuite.class
	  );

    /**
     * The delegate that will be used by each {@link IProxyTest}added to this
     * {@link ProxyTestSuite}.
     */

    private final Test m_delegate;

    /**
     * <p>
     * Invoked automatically by the constructors.
     * </p>
     * 
     * @exception IllegalArgumentException
     *                if the <i>delegate </i> is <code>null</code>.
     */

    final private void checkDelegate( Test delegate )
    {

	if (delegate == null) {

	    throw new IllegalArgumentException("The delegate is null.");

        }

//        if (m_delegate != null) {
//
//            throw new IllegalStateException("The delegate is already set.");
//
//        }

	log.debug
	    ( "Delegate is "+delegate.getClass().getName()
	      );
	
    }

    /**
     * Returns the <i>delegate </i> supplied to the constructor.
     */

    public Test getDelegate() {

        return m_delegate;

    }

    /**
     * Creates an empty unnamed test suite. The <i>delegate </i> will be
     * assigned to tests added to this test suite that implement
     * {@link IProxyTest}.
     * 
     * @param delegate
     *            The delegate (non-null).
     */

    public ProxyTestSuite( Test delegate )
    {
	
        super();

	checkDelegate( delegate );
	
	m_delegate = delegate;

    }

    /**
     * Creates a named test suite and populates it with test instances
     * identified by scanning the <i>testClass </i>. The <i>delegate </i> will
     * be assigned to tests added to this test suite that implement
     * {@link IProxyTest}, including those created from <i>testClass </i>.
     * 
     * @param delegate
     *            The delegate (non-null).
     * @param testClass
     *            A class containing one or more tests.
     * @param name
     *            The name of the test suite (optional).
     */

    public ProxyTestSuite( Test delegate, Class testClass, String name )
    {
	
	/*
	 * Let the super class do all the heavy lifting. Note that it can not set
	 * the delegate for us since we can not set our private fields until after
	 * we have invoked the super class constructor.
	 */

	super( testClass, name );

	checkDelegate( delegate );
	
	m_delegate = delegate;

	/*
	 * Apply delegate to all instantiated tests that were created by
	 * junit from testClass and which implement IProxyTest.
	 */
	flowDown(this);
	
    }

    /**
     * Creates an unnamed test suite and populates it with test instances
     * identified by scanning the <i>testClass </i>. The <i>delegate </i> will
     * be assigned to tests added to this test suite that implement
     * {@link IProxyTest}, including those created from <i>testClass </i>.
     * 
     * @param delegate
     *            The delegate (non-null).
     * @param testClass
     *            A class containing one or more tests.
     */

    public ProxyTestSuite( Test delegate, Class testClass )
    {

	/*
	 * Let the super class do all the heavy lifting. Note that it can not set
	 * the delegate for us since we can not set our private fields until after
	 * we have invoked the super class constructor.
	 */

	super( testClass );

	// Remember the delegate.

	checkDelegate( delegate );
	
	m_delegate = delegate;

	/*
	 * Apply delegate to all instantiated tests that were created by
	 * junit from testClass and which implement IProxyTest.
	 */
	flowDown(this);

    }

    /**
     * Creates an empty named test suite. The declared will be assigned to tests
     * added to this test suite that implement {@link IProxyTest}.
     * 
     * @param delegate
     *            The delegate (non-null).
     * @param name
     *            The test suite name (optional).
     */
    public ProxyTestSuite( Test delegate, String name )
    {

        super( name );

	checkDelegate( delegate );
	
	m_delegate = delegate;

    }

    /**
     * We override the implementation of {@link
     * TestSuite#addTestSuite( Class theClass )} to wrap the
     * <i>testClass</i> in another instance of this {@link
     * ProxyTestSuite} class using the same delegate that was provided
     * to our constructor.  This causes the delegation to be inherited
     * by any recursively contained <i>testClass</i> which implements
     * {@link IProxyTest}.
     */     

    public void addTestSuite( Class testClass )
    {

        if( m_delegate == null ) {
            
            /*
             * The delegate will be null if this method gets invoked from the
             * super class constructor since that will happen before we have a
             * chance to set the [m_delegate] field. The constructors handle
             * this situation by explictly flowing down the delegate after the
             * super class constructor has been invoked.
             */
            
            super.addTestSuite( testClass );
            
            return;
            
        }
        
	// This is the default implementation from TestSuite:
	//
	// addTest(new TestSuite(testClass));
	//
	// Our implementation just substitutes new ProxyTestSuite(
	// delegate, testClass ) for new TestSuite( testClass ).
	//

	ProxyTestSuite proxyTestSuite = new ProxyTestSuite
	    ( m_delegate,
	      testClass
	      );

	addTest( proxyTestSuite );
	
    }

    /**
     * If the suite is not a {@link ProxyTestSuite}, then the tests in the
     * suite are recursively enumerated and a proxy test suite is created with
     * the same name and tests. This ensures that the common delegate flows down
     * through all tests even when using the traditional
     * <code>static Test suite() {...}</code> construction.
     * 
     * @param test
     *            A test.
     */
    public void addTest(Test test) {

        if( m_delegate == null ) {
            
            /*
             * The delegate will be null if this method gets invoked from the
             * super class constructor since that will happen before we have a
             * chance to set the [m_delegate] field.  The constructors handle
             * this situation by explictly flowing down the delegate after the
             * super class constructor has been invoked.
             */
            
            super.addTest( test );
            
            return;
            
        }

        if ( ! (test instanceof ProxyTestSuite)) {
            
            /*
             * Flow down the delegate to any container IProxyTest instances.
             */
            
            flowDown( test );
            
        }

        super.addTest( test );
        
    }
    
    protected void flowDown(Test t) {

        if (m_delegate == null) {

            throw new AssertionError("delegate is not set.");

        }
        
        if ( t instanceof TestSuite ) {
            
            flowDown( (TestSuite) t );
            
        } else if (t instanceof IProxyTest) {

            log.debug("Setting delegate on " + t.getClass() + " to "
                    + m_delegate.getClass());

            ((IProxyTest) t).setDelegate(m_delegate);

        }

    }
    
    /**
     * <p>
     * Sets the delegate on each instantiated {@link Test} that implements
     * {@link IProxyTest}.
     * </p>
     */

    @SuppressWarnings("unchecked")
    protected void flowDown( final TestSuite suite ) {
        
        if( m_delegate == null ) {
        
            throw new AssertionError("delegate is not set.");
            
        }
        
	for( java.util.Enumeration e= suite.tests(); e.hasMoreElements(); ) {

 	    Test t = (Test)e.nextElement();

 	    flowDown( t );
 	    
	}

    }

	
    
}
