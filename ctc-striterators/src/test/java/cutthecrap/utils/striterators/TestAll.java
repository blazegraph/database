/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package cutthecrap.utils.striterators;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    /**
     * Aggregates the tests in increasing dependency order.
     */
    public static Test suite()
 {

        final TestSuite suite = new TestSuite("striterators");

        suite.addTestSuite(TestFilterBase.class);

        /*
         * @todo test Appender
         * 
         * @todo test Contractor
         * 
         * @todo test ExclusionFilter
         * 
         * @todo test Expander
         * 
         * @todo test Mapper
         * 
         * @todo Test Merger
         * 
         * @todo Test Resolver
         * 
         * @todo Test Sorter
         * 
         * @todo Test UniquenessFilter
         * 
         * @todo Test Visitor
         * 
         * @todo Test XProperty
         * 
         */
        suite.addTestSuite(TestFilter.class);
        suite.addTestSuite(TestCloseable.class);

        // @todo test Striterator

        suite.addTestSuite(TestArrayIterator.class);

        return suite;

    }

}
