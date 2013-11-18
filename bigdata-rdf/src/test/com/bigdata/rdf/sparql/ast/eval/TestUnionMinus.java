/**

Copyright (C) SYSTAP, LLC 2006-2013.  All rights reserved.

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
 * Created on Oct 24, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

/**
 * Test suite for UNION and MINUS combined, see 
 * https://sourceforge.net/apps/trac/bigdata/ticket/767
 * 
 */
public class TestUnionMinus extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestUnionMinus() {
    }

    /**
     * @param name
     */
    public TestUnionMinus(String name) {
        super(name);
    }

    /**
SELECT  ?s
WHERE {
   { 
     BIND ( :bob as ?s )
   } UNION {
   }
   MINUS {
      BIND ( :bob as ?s )
   }
} LIMIT 10
     */
    public void test_union_minus_01() throws Exception {

        new TestHelper("union_minus_01").runTest();
        
    }

    /**
SELECT  ?s
WHERE {
   { 
     BIND ( :bob as ?s )
   } UNION {
   }
   FILTER (!BOUND(?s) || ?s != :bob)
}

     */
    public void test_union_minus_02() throws Exception {

        new TestHelper("union_minus_02").runTest();
        
    }
    
}
