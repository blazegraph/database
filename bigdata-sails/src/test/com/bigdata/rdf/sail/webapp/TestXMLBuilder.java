/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Apr 13, 2011
 */

package com.bigdata.rdf.sail.webapp;

import java.io.IOException;

import junit.framework.TestCase2;

/**
 * Test suite for {@link XMLBuilder}.
 * 
 * @author martyncutcher
 * @version $Id$
 */
public class TestXMLBuilder extends TestCase2 {

    /**
     * 
     */
    public TestXMLBuilder() {

    }

    /**
     * @param name
     */
    public TestXMLBuilder(String name) {

        super(name);
        
    }

    /**
     * @todo This does not actually test anything. You have to inspect the
     *       output.
     */
    public void testXMLBuilder() throws IOException {

        final XMLBuilder xml = new XMLBuilder();
        
        XMLBuilder.Node close = xml.root("data")
            .attr("id", "TheRoot")
            .attr("name", "Test")
            .node("child", "My Child")
            .node("child")
                .attr("name", "My Child")
                .close()
            .node("child")
                .attr("name", "My Child")
                .text("Content")
                .close()
            .close();
        
        assertTrue(close == null);
        
        System.out.println(xml.toString());

    }

}
