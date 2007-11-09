/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Aug 23, 2006
 */

package com.bigdata.rdf.metrics;

import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase2;

/**
 * Test case for helper class to parse generated queries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestQueryReader extends TestCase2 {

    /**
     * 
     */
    public TestQueryReader() {
    }

    /**
     * @param arg0
     */
    public TestQueryReader(String arg0) {
        super(arg0);
    }

    public void test_queryParser() throws IOException {
        
        String queryFile = "com/bigdata/rdf/metrics/q1155248500387_0001.apq";

        String content = getTestResource(queryFile);

//        StringBuilder sb = new StringBuilder();
//
//        while (true) {
//
//            int ch = r.read();
//
//            if (ch == -1)
//                break;
//
//            sb.append((char) ch);
//
//            // if( (char)ch == '\n') {
//            // System.err.println(""+ch+" "+(char)ch);
//            // }
//
//        }
//
//        r.close();
//
//        String content = sb.toString();

        Properties properties = new Properties();

        MetricsQueryParser parser = new MetricsQueryParser();
        
        String query = parser.parseQueryFile(properties, content);

        /*
         * State that we expect to recover by parsing the query resource.
         */
        String[][] expectedProperties = new String[][] {
                new String[] { "FileName", "q1155248500387_0001.apq" },
                new String[] { "DateGenerated", "8/10/06 6:21:40 PM EDT" },
                new String[] { "OntologyVersion", "1.9.2" },
                new String[] { "QueryType", "8" },
                new String[] { "NumBoundVars", "3" },
                new String[] { "NumFreeVars", "1" },
                new String[] { "NumWhereClauses", "6" },
                new String[] { "NumAndClauses", "1" },
                new String[] { "ContainsRegExpr", "true" },
                new String[] { "NumUsingClauses", "2" } };

        String expectedQuery = "SELECT ?link ?ent1 ?ent2 \n"
                + "WHERE (?link rdf:type domain:fundsTechnology),\n"
                + "       (?ent1 rdf:type domain:Actor),\n"
                + "       (?ent2 rdf:type domain:Technology),\n"
                + "       (?link system:fromEntity ?ent1),\n"
                + "       (?link system:toEntity ?ent2),\n"
                + "       (?ent2 system:lexicalForm ?inst)\n"
                + "AND ?inst ~~\"*MA\"\n"
                + "USING domain FOR <http://expcollab.net/apstars/domain#>,\n"
                + "      system FOR <http://expcollab.net/apstars/system#> ";

        assertEquals("query", expectedQuery, query);

        assertEquals("#properties", expectedProperties.length, properties
                .size());
        for (int i = 0; i < expectedProperties.length; i++) {
            String[] nvp = expectedProperties[i];
            String key = nvp[0];
            String val = nvp[1];
            assertEquals("key=" + key, val, properties.getProperty(key));
        }

    }
    
}
