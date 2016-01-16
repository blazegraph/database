/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

/**
 * Data driven test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBasicQuery extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestBasicQuery() {
    }

    /**
     * @param name
     */
    public TestBasicQuery(String name) {
        super(name);
    }

    /**
     * A SELECT query consisting of a single statement pattern.
     */
    public void test_select_1() throws Exception {
        
        new TestHelper("select-1").runTest();
        
    }
    
    /**
     * A simple SELECT query.
     */
    public void test_select_2() throws Exception {
        
        new TestHelper("select-2").runTest();
        
    }
    
    /**
     * A simple ASK query.
     */
    public void test_ask() throws Exception {

        new TestHelper("ask").runTest();
        
    }

}
