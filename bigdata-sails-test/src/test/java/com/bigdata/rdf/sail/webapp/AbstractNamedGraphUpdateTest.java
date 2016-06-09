/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2014.  All rights reserved.

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

package com.bigdata.rdf.sail.webapp;

import java.io.IOException;

import com.bigdata.rdf.sparql.ast.eval.ASTConstructIterator;

/**
 * This class is concerning the issues raised in trac 804
 * 
 * @see <a href="https://jira.blazegraph.com/browse/BLZG-885" >update bug
 *      deleting quads</a>
 * 
 * @author jeremycarroll
 */
public class AbstractNamedGraphUpdateTest extends AbstractProtocolTest {
	
	private final static String distinctHintFalse = "    hint:Query hint:nativeDistinctSPO false . \n";
	private final static String distinctHintTrue = "    hint:Query hint:nativeDistinctSPO true . \n";
	private final boolean nativeDistinct;
	
	public AbstractNamedGraphUpdateTest(final boolean nativeDistinct, final String name)  {
		super(name);
		this.nativeDistinct = nativeDistinct;
	}
	
	
	private String atomicMoveNamedGraph() {
		// Atomic update of uploaded graph - moving eg:tmp to eg:a (deleting old contents of eg:a)
		return 
		"DELETE {\n" +   
	    "  GRAPH <eg:a> {\n" +
	    "    ?olds ?oldp ?oldo\n" +
	    "  }\n" +       
	    "  GRAPH <eg:tmp> {\n" +
	    "    ?news ?newp ?newo\n" +
	    "  }\n" +       
	    "}\n" +     
	    "INSERT {\n" +
	    "  GRAPH <eg:a> {\n" +
	    "    ?news ?newp ?newo\n" +
	    "  }\n" +       
	    "}\n" +     
	    "WHERE {\n" +
	    distinctHintFalse +
	    "  {\n" +       
	    "    GRAPH <eg:a> {\n" +
	    "      ?olds ?oldp ?oldo\n" +
	    "    }\n" +         
	    "  } UNION {\n" +
	    "    GRAPH <eg:tmp> {\n" +
	    "      ?news ?newp ?newo\n" +
	    "    }\n" +         
	    "  }\n" +       
	    "}";
	}
	
	private String insertData = 
			        "prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
				    "INSERT DATA\n" +
				    "{ \n" +
				    " GRAPH <eg:a> {\n" +
				    "   [ a \"Blank\" ] .\n" +
				    "   <eg:b> rdf:type <eg:c> ; rdf:value [] .\n" +
				    "   [ rdf:value [] ] .\n" +
				    " }\n" +
				    " GRAPH <eg:tmp> {\n" +
				    "   [ a \"Blankx\" ] .\n" +
				    "   <eg:B> rdf:type <eg:C> ; rdf:value [] .\n" +
				    "   [ rdf:value [] ] .\n" +
				    " }\n" +
				    "}\n";
	
	
	private void makeUpdate(String update) throws IOException {
		final boolean hasHint = update.contains(distinctHintFalse);
		if (hasHint) {
			ASTConstructIterator.flagToCheckNativeDistinctQuadsInvocationForJUnitTesting = false;
			if (nativeDistinct) {
				update = update.replace(distinctHintFalse, distinctHintTrue);
			}
		}
		setMethodisPostUrlEncodedData();
		serviceRequest("update", update);
		if (hasHint) {
			assertEquals(nativeDistinct, ASTConstructIterator.flagToCheckNativeDistinctQuadsInvocationForJUnitTesting );
		}
	}
	
	private void assertQuad(String graph, String triple) throws IOException {
		assertQuad("true", graph, triple);
	}

	private void assertNotQuad(String graph, String triple) throws IOException {
		assertQuad("false", graph, triple);
	}

	void assertQuad(String expected, String graph, String triple) throws IOException {
		String result = serviceRequest("query", "ASK { GRAPH " + graph + " { " + triple + "} }" );
		assertTrue(result.contains(expected));
	}
	
	private void updateAFewTimes(int numberOfUpdatesPerTime) throws IOException {
		final int numberOfTimes = 5;
		for (int i=0; i<numberOfTimes; i++) {
			for (int j=0; j<numberOfUpdatesPerTime;j++) {
				makeUpdate(insertData);
			}
			makeUpdate( atomicMoveNamedGraph() );
			assertNotQuad("<eg:tmp>", " ?s ?p ?o ");
		}
	}

	public void test_t_1() throws  IOException {
		updateAFewTimes(1);
	}
	public void test_t_2() throws  IOException {
		updateAFewTimes(2);
	}
	public void test_t_3() throws  IOException {
		updateAFewTimes(3);
	}
	public void test_t_5() throws  IOException {
		updateAFewTimes(5);
	}
	public void test_double_triple_delete() throws  IOException {
		setMethodisPostUrlEncodedData();
		makeUpdate("prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
			    "INSERT DATA\n" +
			    "{ \n" +
			    " GRAPH <eg:a> {\n" +
			    "   <eg:b> rdf:type <eg:c> \n" +
			    " }\n" +
			    " GRAPH <eg:tmp> {\n" +
			    "   <eg:b> rdf:type <eg:c> \n" +
			    " }\n" +
			    "}\n");
		makeUpdate( "DELETE {\n" +   
			    "  GRAPH <eg:a> {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +       
			    "  GRAPH <eg:tmp> {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +       
			    "}\n" +   
			    "WHERE {\n" +
			    distinctHintFalse +
			    "    GRAPH <eg:a> {\n" +
			    "      ?olds ?oldp ?oldo\n" +
			    "    }\n" +     
			    "}");
		assertNotQuad("?g","?s ?p ?o");
		
	}

	public void test_double_triple_insert() throws  IOException {
		makeUpdate( "prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
			    "INSERT DATA\n" +
			    "{ \n" +
			    " GRAPH <eg:tmp> {\n" +
			    "   <eg:b> rdf:type <eg:c> .\n" +
			    "   <eg:x> rdf:type _:foo \n" +
			    " }\n" +
			    "}\n");
		makeUpdate( "INSERT {\n" +   
			    "  GRAPH <eg:A> {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +       
			    "  GRAPH <eg:B> {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +       
			    "}\n" +   
			    "WHERE {\n" +
			    distinctHintFalse +
			    "    GRAPH <eg:tmp> {\n" +
			    "      ?olds ?oldp ?oldo\n" +
			    "    }\n" +     
			    "}");
		assertQuad("<eg:A>","<eg:b> rdf:type <eg:c> ");
		assertQuad("<eg:B>","<eg:b> rdf:type <eg:c> ");
		assertQuad("<eg:A>","<eg:x> rdf:type ?x");
		assertQuad("<eg:B>","<eg:x> rdf:type ?x ");
	}
	
	public void test_double_triple_delete_insert() throws  IOException {
		makeUpdate( "prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
			    "INSERT DATA\n" +
			    "{ \n" +
			    " GRAPH <eg:tmp> {\n" +
			    "   <eg:A> <eg:moveTo> <eg:AA> .\n" +
			    "   <eg:B> <eg:moveTo> <eg:BB> \n" +
			    " }\n" +
			    "}\n");
		makeUpdate( "INSERT {\n" +   
			    "  GRAPH <eg:A> {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +      
			    "}\n" +   
			    "WHERE {\n" +
			    "    GRAPH <eg:tmp> {\n" +
			    "      ?olds ?oldp ?oldo\n" +
			    "    }\n" +     
			    "}");
		makeUpdate( "INSERT {\n" +  
			    "  GRAPH <eg:B> {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +       
			    "}\n" +   
			    "WHERE {\n" +
			    "    GRAPH <eg:tmp> {\n" +
			    "      ?olds ?oldp ?oldo\n" +
			    "    }\n" +     
			    "}");
		assertQuad("<eg:A>","<eg:A> <eg:moveTo> <eg:AA> ");
		assertQuad("<eg:B>","<eg:A> <eg:moveTo> <eg:AA> ");
		assertQuad("<eg:A>","<eg:B> <eg:moveTo> <eg:BB>");
		assertQuad("<eg:B>","<eg:B> <eg:moveTo> <eg:BB> ");
		makeUpdate(
				"DELETE {\n" + 
			    "    GRAPH ?oldg {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +       
			    "}\n" +   
				"INSERT {\n" +  
			    "  GRAPH ?newg {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +       
			    "}\n" +   
			    "WHERE {\n" +
			    distinctHintFalse +
			    "    GRAPH <eg:tmp> {\n" +
			    "      ?oldg <eg:moveTo> ?newg\n" +
			    "    }\n" +     
			    "    GRAPH ?oldg {\n" +
			    "       ?olds ?oldp ?oldo\n" +
			    "    }\n" +     
			    "}");
		assertNotQuad("<eg:A>","<eg:A> <eg:moveTo> <eg:AA> ");
		assertNotQuad("<eg:B>","<eg:A> <eg:moveTo> <eg:AA> ");
		assertNotQuad("<eg:A>","<eg:B> <eg:moveTo> <eg:BB>");
		assertNotQuad("<eg:B>","<eg:B> <eg:moveTo> <eg:BB> ");
		assertQuad("<eg:AA>","<eg:A> <eg:moveTo> <eg:AA> ");
		assertQuad("<eg:BB>","<eg:A> <eg:moveTo> <eg:AA> ");
		assertQuad("<eg:AA>","<eg:B> <eg:moveTo> <eg:BB>");
		assertQuad("<eg:BB>","<eg:B> <eg:moveTo> <eg:BB> ");
	}

    /*
     * TODO FIXME - this test is broken when we force it through the 
     * DELETE+INSERT code path (solution set replay).
     */
//	public void test_triple_template_and_fixed_insert() throws  IOException {
//		makeUpdate( "prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
//			    "INSERT DATA\n" +
//			    "{ \n" +
//			    " GRAPH <eg:tmp> {\n" +
//			    "   <eg:b> rdf:type <eg:c> .\n" +
//			    " }\n" +
//			    "}\n");
//		makeUpdate( "INSERT {\n" +   
//			    "  GRAPH <eg:A> {\n" +
//			    "    ?olds ?oldp ?oldo\n" +
//			    "  }\n" +       
//			    "  GRAPH <eg:B> {\n" +
//			    "   <eg:b> rdf:type <eg:c> .\n" +
//			    "  }\n" +       
//			    "}\n" +   
//			    "WHERE {\n" +
//			    distinctHintFalse +
//			    "    GRAPH <eg:tmp> {\n" +
//			    "      ?olds ?oldp ?oldo\n" +
//			    "    }\n" +     
//			    "}");
//		assertQuad("<eg:A>","<eg:b> rdf:type <eg:c> ");
//		assertQuad("<eg:B>","<eg:b> rdf:type <eg:c> ");
//	}

}
