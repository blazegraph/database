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
 * Created on Oct 24, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;


/**
 * Test suite for UNION and MINUS combined, see 
 * https://sourceforge.net/apps/trac/bigdata/ticket/767
 * 
 */
public class TestUnionMinus extends AbstractInlineSELECTTestCase {

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

    @Override
    public String trigData() {
    	return "";
    }
    

    /*
    public void test_union_minus_01() throws Exception {
    	// Concerning omitting the test with hash joins, see Trac776 and 
    	// com.bigdata.rdf.internal.encoder.AbstractBindingSetEncoderTestCase.test_solutionWithOneMockIV()
  
        new Execute(
        		"SELECT  ?s                \r\n" + 
        		"WHERE {                   \r\n" + 
        		"   {                      \r\n" + 
        		"     BIND ( :bob as ?s )  \r\n" + 
        		"   } UNION {              \r\n" + 
        		"   }                      \r\n" + 
        		"   MINUS {                \r\n" + 
        		"      BIND ( :bob as ?s ) \r\n" + 
        		"   }                      \r\n" + 
        		"}").expectResultSet("?s","UNDEF");
        
    }
    */

   
    public void test_union_minus_02() throws Exception {

    	new Execute(
        		"SELECT  ?s\r\n" + 
        		"WHERE {\r\n" + 
        		"   { \r\n" + 
        		"     BIND ( :bob as ?s )\r\n" + 
        		"   } UNION {\r\n" + 
        		"   }\r\n" + 
        		"   FILTER (!BOUND(?s) || ?s != :bob)\r\n" + 
        		"}").expectResultSet("?s","UNDEF");
        
    }
    
    /*
    public void test_union_minus_03() throws Exception {

        new Execute(
        		"SELECT  ?s                \r\n" + 
        		"WHERE {                   \r\n" + 
        		"   {                      \r\n" + 
        		"     BIND ( 2 as ?s )     \r\n" + 
        		"   } UNION {              \r\n" + 
        		"   }                      \r\n" + 
        		"   MINUS {                \r\n" + 
        		"      BIND ( 2 as ?s )    \r\n" + 
        		"   }                      \r\n" + 
        		"}").expectResultSet("?s","UNDEF");
        
    }
    */
    
    /*
    public void test_union_minus_04() throws Exception {

        new Execute(
        		"SELECT  ?x                \r\n" + 
        		"WHERE {                   \r\n" + 
        		"   BIND ( 3 as ?x )       \r\n" + 
        		"   { BIND ( 4 as ?x )     \r\n" + 
        		"   } UNION {              \r\n" + 
        		"     MINUS {              \r\n" + 
        		"      BIND ( 3 as ?x )    \r\n" + 
        		"     }                    \r\n" + 
        		"     BIND (3 as ?x)       \r\n" + 
        		"   }                      \r\n" + 
        		"}").expectResultSet("?x","3");
        
    }
    */
    
    /*
    public void test_union_minus_05() throws Exception {

        new Execute(
        		"SELECT  ?x                \r\n" + 
        		"WHERE {                   \r\n" + 
        		"   BIND ( 3 as ?x )       \r\n" + 
        		"   { BIND ( 4 as ?x )     \r\n" + 
        		"   } UNION {              \r\n" + 
        		"     MINUS {              \r\n" + 
        		"      BIND ( 3 as ?x )    \r\n" + 
        		"     }                    \r\n" + 
        		"   }                      \r\n" + 
        		"}").expectResultSet("?x","3");
        
    }
    */
    
    /*
    public void test_union_minus_06() throws Exception {

        new Execute(
        		"SELECT  ?x                \r\n" + 
        		"WHERE {                   \r\n" + 
        		"   BIND ( 3 as ?x )       \r\n" + 
        		"   { BIND ( 4 as ?x )     \r\n" + 
        		"   } UNION {              \r\n" + 
        		"     BIND (3 as ?x)       \r\n" + 
        		"     MINUS {              \r\n" + 
        		"      BIND ( 3 as ?x )    \r\n" + 
        		"     }                    \r\n" + 
        		"   }                      \r\n" + 
        		"}").expectResultSet("?x");
        
    }
    */

    /*
    public void test_union_minus_07() throws Exception {

        new Execute(
        		"SELECT  ?x                   \r\n" + 
        		"WHERE {                      \r\n" + 
        		"   BIND ( 3 as ?x )          \r\n" + 
        		"   { BIND ( 4 as ?x )        \r\n" + 
        		"   } UNION {                 \r\n" + 
        		"     BIND ( 3 as ?x )        \r\n" + 
        		"     MINUS {                 \r\n" + 
        		"       {                     \r\n" + 
        		"         BIND ( 3 as ?x )    \r\n" + 
        		"       } UNION {             \r\n" + 
        		"         BIND ( 4 as ?y )    \r\n" + 
        		"       }                     \r\n" + 
        		"     }                       \r\n" + 
        		"   }                         \r\n" + 
        		"}").expectResultSet("?x");
        
    }
    */
    
    /*
    public void test_union_minus_08() throws Exception {

        new Execute(
        		"SELECT  ?x                   \r\n" + 
        		"WHERE {                      \r\n" + 
        		"   BIND ( 3 as ?x )          \r\n" + 
        		"   { BIND ( 4 as ?x )        \r\n" + 
        		"   } UNION {                 \r\n" + 
        		"     BIND ( 3 as ?x )        \r\n" + 
        		"     MINUS {                 \r\n" + 
        		"       {                     \r\n" + 
        		"         BIND ( 3 as ?x )    \r\n" + 
        		"       } UNION {             \r\n" + 
        		"       }                     \r\n" + 
        		"     }                       \r\n" + 
        		"   }                         \r\n" + 
        		"}").expectResultSet("?x");
        
    }
    */
    

    public void test_union_minus_09() throws Exception {

        new Execute(
        		"SELECT  ?x                   \r\n" + 
        		"WHERE {                      \r\n" + 
        		"   BIND ( 3 as ?x )          \r\n" + 
        		"   { BIND ( 4 as ?x )        \r\n" + 
        		"   } UNION {                 \r\n" + 
        		"     BIND ( 3 as ?x )        \r\n" + 
        		"     MINUS {                 \r\n" + 
        		"     }                       \r\n" + 
        		"   }                         \r\n" + 
        		"}").expectResultSet("?x","3");
        
    }

    /*
    public void test_union_minus_10() throws Exception {

        new Execute(
        		"SELECT  ?x                   \r\n" + 
        		"WHERE {                      \r\n" + 
        		"  { BIND ( 3 as ?x ) }       \r\n" + 
        		"  UNION                      \r\n" + 
        		"  { BIND ( 4 as ?y ) }       \r\n" + 
        		"  MINUS {                    \r\n" + 
        		"    { BIND ( 3 as ?x ) }     \r\n" + 
        		"    UNION                    \r\n" + 
        		"    { BIND ( 4 as ?y ) }     \r\n" + 
        		"  }                          \r\n" + 
        		"}").expectResultSet("?x","3");
        
    }
    */
}
