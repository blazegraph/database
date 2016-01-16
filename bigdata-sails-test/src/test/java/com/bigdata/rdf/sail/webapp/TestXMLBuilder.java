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
 * Created on Apr 13, 2011
 */

package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.io.StringWriter;

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

    	final StringWriter w = new StringWriter();
    	
        final XMLBuilder xml = new XMLBuilder(w);
        
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
        
        if(log.isInfoEnabled())
            log.info(w.toString());

    }

}
