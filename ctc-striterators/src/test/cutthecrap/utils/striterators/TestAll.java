/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 27, 2010
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

        // @todo test Striterator
        
        return suite;

    }

}
