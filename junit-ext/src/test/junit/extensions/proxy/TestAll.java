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
 * Created on May 23, 2006
 */
package junit.extensions.proxy;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Runs all tests in this package.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */

public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
        super();
    }

    /**
     * @param name
     */
    public TestAll(String name) {
        super(name);
    }

    public static Test suite() {
        
        TestSuite suite = new TestSuite();

        suite.addTestSuite( DelegateTestCase.class );
        suite.addTestSuite( TestProxySetup.class );
        
        return suite;
        
    }
}
