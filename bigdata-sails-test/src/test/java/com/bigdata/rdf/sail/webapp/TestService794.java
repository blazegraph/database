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

import junit.framework.Test;


/**
 * While writing this comment, early January 2014,
 * the status is that the two interesting tests
 * both fail and are disabled
 * {@link #xtestMassageServiceCall()} and {@link #xtestMassageServiceNested1Call()}
 * 
 * Also {@link #xtestServiceSyntaxError()} shows some bug some where in that
 * we take legal SPARQL and make it illegal before the service call ....
 * 
 * Some of the other tests show how to use a subselect as a workaround.
 * @author jeremycarroll
 *
 */
public class TestService794 extends AbstractProtocolTest {

	public TestService794(String name)  {
		super(name);
	}


	static public Test suite() {
		return ProxySuiteHelper.suiteWhenStandalone(TestService794.class,"test.*",
				TestMode.quads,TestMode.sids,
				TestMode.triples);
	}
	
	/**
	 * Execute an ASK query including a SERVICE keyword which gets sent back to this server.
	 * The test succeeeds if the query returns true, and fails otherwise
	 * @param args
	 * @throws IOException
	 */
	private void abstactAskService(final String ... args) throws IOException {

		setMethodisPostUrlEncodedData();
		serviceRequest("update","PREFIX eg: <http://example.com/a#> INSERT { eg:a eg:p \"rs123\" ; eg:q 123, 100 } WHERE {}");
		
		final StringBuilder bld = new StringBuilder();
		// Set the base URI to be our sparql end point, for re-entrant queries,
		// using the idiom SERVICE <>
		bld.append("base <");
		bld.append(m_serviceURL);
		bld.append("/sparql>");
		for (String arg:args) {
			bld.append('\n');
			bld.append(arg);
		}
		
		if(log.isInfoEnabled()) log.info(bld.toString());
		final String result = serviceRequest("query",bld.toString());
		if(log.isInfoEnabled()) log.info(result);
		assertTrue(result.contains("true"));
		
	}

	/**
	 * @throws IOException
	 */
	public void testSimpleServiceCall() throws IOException {
		abstactAskService("PREFIX eg: <http://example.com/a#>",
				 "ASK {",
				 "?x eg:p ?y ",
				 " SERVICE <> {",
				 " FILTER ( true )",
				 "{ SELECT ?x ?y {",
				 "?x eg:p ?y ",
				 "} ORDER BY ?y LIMIT 1 }",
				 "} }");
	}

	/**
	 * This one is currently broken, see trac794
	 * 
	 * Note also there is something unintersting with syntax
	 * going wrong with some expressions like
	 * SERVICE <> {
	 *    { SELECT * {
	 *       ?x eg:q ?y 
	 *      }
	 *    }
	 * }
	 */
	public void xtestMassageServiceCall() throws IOException {
		abstactAskService("PREFIX eg: <http://example.com/a#>",
				"prefix xsd:  <http://www.w3.org/2001/XMLSchema#>",
				 "ASK {",
				 "?x eg:p ?y ",
				 "BIND (xsd:integer(substr(?y,3)) as ?yy )",
				 " SERVICE <> {",
				 " FILTER (true )",
				 "{ SELECT ?x ?yy {",
				 "?x eg:q ?yy ",
				 "} ORDER BY ?yy LIMIT 1 }",
				 "} }");
	}
	/**
	 * @throws IOException
	 */
	public void xtestMassageServiceNested1Call() throws IOException {
		abstactAskService("PREFIX eg: <http://example.com/a#>",
				"prefix xsd:  <http://www.w3.org/2001/XMLSchema#>",
				 "ASK {",
				 "{ ?x eg:p ?y ",
				 "BIND (xsd:integer(substr(?y,3)) as ?yy ) }",
				 " SERVICE <> {",
				 "{ SELECT ?x ?yy {",
				 "?x eg:q ?yy ",
				 "} ORDER BY ?yy LIMIT 1 }",
				 "} }");
	}
	/**
	 * @throws IOException
	 */
	public void testMassageServiceNested2Call() throws IOException {
		abstactAskService("PREFIX eg: <http://example.com/a#>",
				"prefix xsd:  <http://www.w3.org/2001/XMLSchema#>",
				 "ASK {",
				 "{ SELECT ?x ?yy ",
				 "  { ?x eg:p ?y ",
				 "    BIND (xsd:integer(substr(?y,3)) as ?yy ) } }",
				 " SERVICE <> {",
				 "{ SELECT ?x ?yy {",
				 "?x eg:q ?yy ",
				 "} ORDER BY ?yy LIMIT 1 }",
				 "} }");
	}
	public void testMassageServiceNested3Call() throws IOException {
		abstactAskService("PREFIX eg: <http://example.com/a#>",
				"prefix xsd:  <http://www.w3.org/2001/XMLSchema#>",
				 "ASK {",
				 "{ SELECT ?x (xsd:integer(substr(?y,3)) as ?yy ) ",
				 "  { ?x eg:p ?y } }",
				 " SERVICE <> {",
				 "{ SELECT ?x ?yy {",
				 "?x eg:q ?yy ",
				 "} ORDER BY ?yy LIMIT 1 }",
				 "} }");
	}
	public void xtestServiceSyntaxError() throws IOException {
		abstactAskService("PREFIX eg: <http://example.com/a#>",
				"prefix xsd:  <http://www.w3.org/2001/XMLSchema#>",
				 "ASK {",
				 "{ SELECT ?x (xsd:integer(substr(?y,3)) as ?yy ) ",
				 "  { ?x eg:p ?y } }",
				 " SERVICE <> {",
				 "{ SELECT * {",
				 "?x eg:q ?yy ",
				 "} ORDER BY ?yy LIMIT 1 }",
				 "} }");
	}
}
