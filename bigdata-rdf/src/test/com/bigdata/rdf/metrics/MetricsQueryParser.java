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

import java.util.Properties;

/**
 * Helper class parse a query resource containing metadata about a query
 * together with an textual representation of the query itself.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MetricsQueryParser {

    /**
     * Parse a query file.
     * 
     * @param propertySet
     *            The properties in the file are set on this object.
     * @param content
     *            The content of the query file.
     * @return The query as extracted from the file.
     */
    public String parseQueryFile(Properties propertySet, String content) {

        assert propertySet != null;

        assert content != null;

        // System.err.println("["+content+"]");

        // // String[] tmp = content.split("$$");
        //        
        // String[] tmp =
        // Pattern.compile("\n\n",Pattern.MULTILINE).split(content);

        int pos = content.indexOf("\n\n");

        TestQueryReader.assertTrue(pos != -1);

        String query = content.substring(pos + 2);
        // System.err.println("["+query+"]");

        String props = content.substring(0, pos);
        // System.err.println("["+props+"]");

        String[] properties = props.split("\n");

        for (int i = 0; i < properties.length; i++) {

            String[] nvp = properties[i].split("\\s*=\\s*");

            TestQueryReader.assertEquals(2, nvp.length);

            // System.err.println("["+nvp[0]+"]=["+nvp[1]+"]");

            propertySet.setProperty(nvp[0], nvp[1]);

        }

        return query;

    }

}
