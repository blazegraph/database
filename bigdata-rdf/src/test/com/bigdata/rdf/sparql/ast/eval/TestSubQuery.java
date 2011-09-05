/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

/**
 * Data driven test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSubQuery extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestSubQuery() {
    }

    /**
     * @param name
     */
    public TestSubQuery(String name) {
        super(name);
    }

    /**
     * Unit test for an query with an EXISTS filter. The EXISTS filter is
     * modeled as an ASK sub-query which projects an anonymous variable and a
     * simple test of the truth state of that anonymous variable.
     */
    public void test_exists() {
        fail("write test");
    }

    public void test_sparql_subselect() throws Exception {

        new TestHelper("sparql-subselect").runTest();
        
    }

    /**
     * This is a version of {@link #test_sparql_subselect()} which uses the same
     * data and has the same results, but which uses a named subquery rather
     * than a SPARQL 1.1 subselect.
     */
    public void test_named_subquery() throws Exception {

        new TestHelper("named-subquery").runTest();

    }

    /**
     * This is a variant {@link #test_named_subquery()} in which the JOIN ON
     * query hint is used to explicitly specify NO join variables.
     */
    public void test_named_subquery_noJoinVars() throws Exception {

        new TestHelper("named-subquery-noJoinVars").runTest();

    }

}
