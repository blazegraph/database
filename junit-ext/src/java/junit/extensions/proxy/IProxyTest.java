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
 * A {@link Test} that holds a reference to a delegate.  Normally the
 * delegate will extend {@link TestCase} to provide, possibly
 * persistent, implementation specific logic for establishing test
 * fixtures.  The delegate is normally an instance of your concrete
 * implementation specific test harness with access to any required
 * configuration data, e.g., a properties file.
 */

public interface IProxyTest
    extends Test
{

    /**
     * Sets the delegate.  {@link ProxyTestSuite} uses this method to
     * set the delegate on each test class instance that it creates
     * that implements the {@link IProxyTest} interface.
     */

    public void setDelegate( Test delegate );

    /**
     * Returns the reference to the delegate or <code>null</code> if
     * the delegate was not established.
     */

    public Test getDelegate();

}
