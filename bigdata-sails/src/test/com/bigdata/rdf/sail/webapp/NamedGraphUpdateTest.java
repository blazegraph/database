/**
Copyright (C) SYSTAP, LLC 2014.  All rights reserved.

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

package com.bigdata.rdf.sail.webapp;

import java.io.IOException;

import junit.framework.Test;


/**
 * This class is concernign the issues raised in trac 804
 * https://sourceforge.net/apps/trac/bigdata/ticket/804
 * @author jeremycarroll
 *
 */
public class NamedGraphUpdateTest extends AbstractProtocolTest {

	public NamedGraphUpdateTest(String name)  {
		super(name);
	}
	
	
	private String atomicMoveNamedGraph(boolean useHint) {
		// Atomic update of uploaded graph
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
	    (useHint?"  hint:Query hint:chunkSize 2 .\n":"") +
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
				    "   [ rdf:value [] ]\n" +
				    " }\n" +
				    " GRAPH <eg:tmp> {\n" +
				    "   [ a \"Blankx\" ] .\n" +
				    "   <eg:B> rdf:type <eg:C> ; rdf:value [] .\n" +
				    "   [ rdf:value [] ]\n" +
				    " }\n" +
				    "}\n";
	
	private String ask = "ASK { GRAPH <eg:tmp> { ?s ?p ?o } }";
	
	private void updateAFewTimes(boolean useHint, int numberOfTimes, int numberOfUpdatesPerTime) throws IOException {
		for (int i=0; i<numberOfTimes; i++) {
			for (int j=0; j<numberOfUpdatesPerTime;j++) {
				setMethodisPostUrlEncodedData();
				serviceRequest("update", insertData);
			}
			setMethodisPostUrlEncodedData();
			serviceRequest("update", atomicMoveNamedGraph(useHint) );
			if (!serviceRequest("query", ask ).contains("false")) {
				fail("On loop "+i+" (number of updates = "+numberOfUpdatesPerTime+")");
			}
		}
	}

	public void test_t_20_1() throws  IOException {
		updateAFewTimes(true, 20, 1);
	}
	public void test_t_20_2() throws  IOException {
		updateAFewTimes(true, 20, 2);
	}
	public void test_t_20_3() throws  IOException {
		updateAFewTimes(true, 20, 3);
	}
	public void test_t_20_5() throws  IOException {
		updateAFewTimes(true, 20, 5);
	}
	public void test_f_20_1() throws  IOException {
		updateAFewTimes(false, 20, 1);
	}
	public void test_f_20_2() throws  IOException {
		updateAFewTimes(false, 20, 2);
	}
	public void test_f_20_3() throws  IOException {
		updateAFewTimes(false, 20, 3);
	}
	public void test_f_20_5() throws  IOException {
		updateAFewTimes(false, 20, 5);
	}
	static public Test suite() {
		return ProxySuiteHelper.suiteWhenStandalone(NamedGraphUpdateTest.class,"test.*", TestMode.quads);
	}

}
