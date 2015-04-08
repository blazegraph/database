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
/*
 * Created on May 22, 2006
 */
package junit.extensions.proxy;

import java.util.Enumeration;

import org.apache.log4j.Logger;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestFailure;
import junit.framework.TestResult;
import junit.framework.TestSuite;

/**
 * The tests in this class are responsible for verifying some assumptions about
 * the default behavior of junit when creating hierarchical tests from classes
 * and suites and verifying the correct behavior of the {@link ProxyTestSuite},
 * which must flowdown the delegate to all tests (added to itself or to any
 * nested test suite) that implement the {@link IProxyTest}interface.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 */

public class TestProxySetup extends TestCase {

    /**
     * The {@link Logger} is named for this class.
     */

    protected static final Logger log = Logger.getLogger
	( TestProxySetup.class
	  );

    /**
     * 
     */
    public TestProxySetup() {
        super();
    }

    /**
     * @param name
     */
    public TestProxySetup(String name) {
        super(name);
    }

    /**
     * Test argument checking on {@link ProxyTestSuite} constructor.
     */
    public void test_ctor() {
        
        // local fixtures.
        final class MyTestCase extends TestCase{};
        final Test delegate = new TestCase(){};
        final Class testClass = MyTestCase.class;
        final String name = "name";

        // ctor(Test delegate)
        try {
            new ProxyTestSuite(null);
            assertFalse("Expected exception", true);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        // ctor(Test delegate,Class testClass)
        try {
            new ProxyTestSuite(null,(Class)null);
            assertFalse("Expected exception", true);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        catch(NullPointerException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        try {
            new ProxyTestSuite(delegate,(Class)null);
            assertFalse("Expected exception", true);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        catch(NullPointerException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        try {
            new ProxyTestSuite(null,testClass);
            assertFalse("Expected exception", true);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        // ctor(Test delegate,String name): name is _optional_
        try {
            new ProxyTestSuite(null,(String)null);
            assertFalse("Expected exception", true);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        try {
            new ProxyTestSuite(null,name);
            assertFalse("Expected exception", true);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        new ProxyTestSuite(delegate,(String)null);
                
        // ctor(Test delegate,Class testClass,String name): name is _optional_
        try {
            new ProxyTestSuite(null,null,(String)null);
            assertFalse("Expected exception", true);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        catch(NullPointerException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        try {
            new ProxyTestSuite(delegate,null,name);
            assertFalse("Expected exception", true);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        catch(NullPointerException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        try {
            new ProxyTestSuite(null,testClass,name);
            assertFalse("Expected exception", true);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        catch(NullPointerException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        new ProxyTestSuite(delegate,testClass,name);
        new ProxyTestSuite(delegate,testClass,null);
        
    }
    
    /**
     * Verifies expected behavior for both {@link TestSuite} and
     * {@link ProxyTestSuite} when invoking
     * <code>addTestSuite(Class testClass)</code>.
     */
    public void test_addTest() {
        
        /* junit test suite.  Verify expectations when we add a test class
         * containing a single test to a TestSuite.  junit wraps up all tests
         * in that class in a new TestSuite, so the result looks like this:
         *  
         * suite
         *   newSuite
         *      test1 <delegate is not set>
         */
        
        if( true ) {
            
            TestSuite suite = new TestSuite();
            suite.addTestSuite(MyProxyTest.class);
            assertEquals(1, suite.testCount());
            TestSuite newSuite = (TestSuite) suite.testAt(0);
            assertTrue( newSuite.testAt(0) instanceof MyProxyTest );
            assertNull( ((MyProxyTest)newSuite.testAt(0)).getDelegate() );
            
        }
        
        /*
         * proxy test suite. Verify expectations when we add a proxy test class
         * containing a single test into a proxy test suite. The differences are
         * (1) that the newSuite will be a ProxyTestSuite; (2) that the delegate
         * is set on the new ProxyTestSuite, and (3) that the delegate is set
         * the test.
         * 
         * suite
         *   newSuite <ProxyTestSuite, delegate is set>
         *      test1 <delegate is set>
         */
        
        if( true ) {
            
            final Test delegate = new MyDelegateTest();
            ProxyTestSuite proxySuite = new ProxyTestSuite(delegate);
            proxySuite.addTestSuite(MyProxyTest.class);
            assertEquals(1, proxySuite.testCount());
            ProxyTestSuite newSuite = (ProxyTestSuite) proxySuite.testAt(0);
            assertEquals( delegate, newSuite.getDelegate() );
            assertTrue( newSuite.testAt(0) instanceof MyProxyTest );
            assertEquals( delegate, ((MyProxyTest)newSuite.testAt(0)).getDelegate() );
        
        }
        
    }

    /**
     * <p>
     * Creates a nested proxy test suite and runs it. The tests in the test
     * suite are instances of {@link MyProxyTest}. The delegate is an instance
     * of {@link MyDelegateTest}. The proxy test method
     * {@link MyProxyTest#testNothing()}increments a counter on the delegate
     * each time the test method is run and throws a
     * {@link NullPointerException}if the delegate was not set on the test.
     * This is used to verify that the expected number of tests are run and that
     * the delegate was set on each nested test.
     * </p>
     */
    public void test_addSuite() {
        
        int nexpected = 0;
        
        /*
         * Create a nested test structure using multiple instances of a test
         * class that will increment a counter on the delegate each time it is
         * run (and throw an exception if the delegate was not set).
         */
        TestSuite normalSuite = new TestSuite();
        normalSuite.addTestSuite(MyProxyTest.class);
        nexpected++;
        
        TestSuite normalSuite2 = new TestSuite();
        normalSuite2.addTestSuite(MyProxyTest.class);
        normalSuite.addTest(normalSuite2);
        nexpected++;

        /*
         * Create a proxy test suite.
         */
        final MyDelegateTest delegate = new MyDelegateTest();
        ProxyTestSuite proxySuite = new ProxyTestSuite(delegate);
        
        /*
         * Add the normal test suite to the proxy suite and update the #of
         * expected tests that should be run.
         */
        proxySuite.addTest(normalSuite);

        /*
         * Add a test class to the proxy suite and update the #of expected tests
         * that should be run.
         */
        proxySuite.addTestSuite(MyProxyTest.class);
        nexpected++;
        
        /*
         * Run the proxy test suite. If the proxy was not set on any of the
         * nested classes then a NullPointerException will be thrown. We cross
         * check the #of expected tests with the #of tests that are actually
         * run. If there is a discrepency, verify that you have incremented
         * [nexpected] for each test instance in the test suite. If [nexpected]
         * is correct, then the likely explanation is that some tests were
         * dropped or duplicated by the {@link ProxyTestSuite}.
         */
        TestResult result = new TestResult();
        proxySuite.run(result);
        showResults( result );
        assertTrue("not successful: runCount="+result.runCount()+", errorCount="+result.errorCount()+", failureCount="+result.failureCount(),result.wasSuccessful());
        assertEquals(nexpected,result.runCount());
        
        /*
         * Check the counter on the delegate. If the wrong number of tests are
         * run, then the assertion against the counter on the delegate will be
         * wrong.  This verifies the same thing as the last step above, but in
         * a different manner.
         */
        assertEquals(nexpected,delegate.counter);

    }

    public void showResults( TestResult result ) {
        System.err.println("runCount="+result.runCount()+", errorCount="+result.errorCount()+", failureCount="+result.failureCount()+", successful="+result.wasSuccessful());
        Enumeration en = result.errors();
        while( en.hasMoreElements() ) {
            TestFailure f = (TestFailure) en.nextElement();
            log.info(f.failedTest(),f.thrownException());
        }
        en = result.failures();
        while( en.hasMoreElements() ) {
            TestFailure f = (TestFailure) en.nextElement();
            log.info(f.failedTest(),f.thrownException());
        }
    }
    
    public static class MyDelegateTest extends TestCase {
        /**
         * Counter is incremented by {@link MyProxyTest#testNothing()}.
         */
        public int counter = 0;
    }
    
    public static class MyProxyTest extends TestCase implements IProxyTest {

        private MyDelegateTest delegate;

        /**
         * Verifies that the delegate is an instance of {@link MyDelegateTest}.
         */
        public void setDelegate(Test delegate) {
            this.delegate = (MyDelegateTest)delegate;
        }

        public Test getDelegate() {
           return delegate;
        }
        
        /**
         * Throws {@link NullPointerException} if the delegate is not set when
         * the test is run.
         */
        public void testNothing() {
            delegate.counter++;
        }
        
    }
    
}
