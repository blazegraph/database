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

import junit.framework.*;

/**
 * Tests the proxy delegation pattern.
 */

public class DelegateTestCase
    extends TestCase
{

    public static Test suite()
    {

	Test delegate = new DelegateTestCase(); // !!!! THIS CLASS !!!!

	/* Use a proxy test suite and specify the delegate.
	 */

        ProxyTestSuite suite = new ProxyTestSuite
	    ( delegate
	      );

        /*
         * Load 'proxy' tests. This verifies that ProxyTestSuite#addTestSuite()
         * correctly establishes the delegate on the tests in the named class.
         */

	suite.addTestSuite
 	    ( ProxyTestCase.class
 	      );

	/*
	 * Load 'proxy' tests into a normal test suite.
	 */
	TestSuite suite2 = new TestSuite();
	suite2.addTestSuite(ProxyTestCase.class);
	suite.addTest( suite2 );
	
	return suite;

    }
    
    /**
     * This method succeeds on the delegate and it writes a message on
     * {@link System#err}.  The implementation on the proxy throws an
     * exception so if the proxy is not setup correctly, you will see
     * that thrown exception.
     */

    public void doSomething()
    {

	System.err.println
	    ( "The delegate doSomething() method was successfully invoked."
	      );
	
    }

    /**
     * Suppress the warning that no tests were declared.
     */

    public void testNothing()
    {
    }

}
