/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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
package com.bigdata.rdf.rio.json;

import java.nio.charset.Charset;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.turtle.TurtleParser;

/**
 * An {@link RDFParserFactory} for Turtle parsers.
 * 
 * @author Arjohn Kampman
 * @openrdf
 */
public class BigdataSPARQLResultsJSONParserFactory implements RDFParserFactory {

	public static final RDFFormat JSON = new RDFFormat(
			"JSON", // name 
			"application/sparql-results+json", // mime-type 
			Charset.forName("UTF-8"), // charset
			"json", // file extension
			false, // supports namespaces
			true // supports contexts
			);
	
	static {
		
		RDFFormat.register(JSON);
		
	}
	
	/**
	 * Returns {@link RDFFormat#TURTLE}.
	 */
	public RDFFormat getRDFFormat() {
		return JSON;
	}

	/**
	 * Returns a new instance of {@link TurtleParser}.
	 */
	public RDFParser getParser() {
		return new BigdataSPARQLResultsJSONParser();
	}
}
