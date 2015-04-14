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
package com.bigdata.rdf.graph;

import java.util.LinkedHashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.bigdata.rdf.graph.util.IGraphFixture;
import com.bigdata.rdf.graph.util.IGraphFixtureFactory;

/**
 * Abstract base class for graph mining tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class AbstractGraphTestCase extends TestCase {

    protected static final Logger log = Logger
            .getLogger(AbstractGraphTestCase.class);
    
    public AbstractGraphTestCase() {
    }

    public AbstractGraphTestCase(String name) {
        super(name);
    }

    /**
     * The fixture for the test.
     * 
     * @see #newGraphFixture()
     */
    protected IGraphFixture fixture;

    /**
     * Return the factory object used to obtain an {@link IGraphFixture}.
     */
    abstract protected IGraphFixtureFactory getGraphFixtureFactory();

    /**
     * Return the existing {@link IGraphFixture} for the test.
     */
    protected IGraphFixture getGraphFixture() {

        if (fixture == null)
            throw new IllegalStateException();

        return fixture;
        
    }

    @Override
    protected void setUp() throws Exception {
        
        super.setUp();

        fixture = getGraphFixtureFactory().newGraphFixture();

    }

    @Override
    protected void tearDown() throws Exception {

        if (fixture != null) {

            try {

                fixture.destroy();

            } finally {
            
                fixture = null;
                
            }

        }
        
        super.tearDown();
        
    }

    /**
     * Make a set
     * 
     * @param a
     *            The objects for the set.
     *            
     * @return The set.
     */
    static protected <T> Set<T> set(final T...a) {
        final Set<T> tmp = new LinkedHashSet<T>();
        for(T x : a) {
            tmp.add(x);
        }
        return tmp;
    }

    /**
     * Assert that two sets are the same.
     */
    static protected <T> void assertSameEdges(final Set<? extends T> expected,
            final Set<? extends T> actual) {

        final Set<T> tmp = new LinkedHashSet<T>();

        tmp.addAll(expected);

        for (T t : actual) {

            if (!tmp.remove(t)) {

                fail("Not expected: " + t);

            }

        }

        if (!tmp.isEmpty()) {

            fail("Expected but not found: " + tmp.toString());

        }

    }

}
