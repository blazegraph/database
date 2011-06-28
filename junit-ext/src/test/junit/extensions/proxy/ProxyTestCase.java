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
 * Tests the proxy delegation pattern.  This class implements a proxy.
 */

public class ProxyTestCase
    extends TestCase
    implements IProxyTest
{

    //************************************************************
    //*********************** IProxyTest *************************
    //************************************************************

    Test m_delegate = null;

    public void setDelegate( Test delegate )
    {

	m_delegate = delegate;

    }

    public Test getDelegate()
    {

	return m_delegate;

    }

    //************************************************************
    //********************** Delegated Methods *******************
    //************************************************************

    /**
     * This method succeeds on the delegate and it writes a message on
     * {@link System#err}.  The implementation on the proxy throws an
     * exception so if the proxy is not setup correctly, you will see
     * that thrown exception.
     */

    public void doSomething()
    {

	Test delegate = getDelegate();

	if( delegate == null ) {

	    System.err.println
		( "The delegate was not setup."
		  );

	    fail( "The delegate was not setup."
		  );

	}

	(( DelegateTestCase) delegate ) . doSomething() ;

    }

    //************************************************************
    //************************** tests ***************************
    //************************************************************

    /**
     * This test will fail unless the delegate has been correctly
     * configured.<p>
     *
     * Note: If this test fails, then it can also be a symptom of
     * attempting to directly execute the proxy tests rather than
     * gathering them up and executing them in {@link
     * DelegateTestCase}.  This issue can typically be traced back to
     * how you have configured your Maven/ant JUnit task.  It is
     * recommended that the JUnit task exclude all classes matching

       <code> ** / *ProxyTest* </code>

     */

    public void testProxy()
    {

	doSomething();

    }

}
