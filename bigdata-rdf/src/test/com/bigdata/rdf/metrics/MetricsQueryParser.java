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
