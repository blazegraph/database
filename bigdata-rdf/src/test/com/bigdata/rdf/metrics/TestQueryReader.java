/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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
