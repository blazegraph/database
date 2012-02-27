/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Feb 27, 2012
 */

package com.bigdata.rdf.rio.nquads;

import junit.framework.TestCase2;

import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;

import com.bigdata.rdf.rio.nquads.NQuadsParser;
import com.bigdata.rdf.rio.nquads.NQuadsParserFactory;

/**
 * Test suite for the {@link NQuadsParserFactory}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNQuadsParserFactory extends TestCase2 {

    /**
     * 
     */
    public TestNQuadsParserFactory() {
    }

    /**
     * @param name
     */
    public TestNQuadsParserFactory(String name) {
        super(name);
    }

    public void test_serviceRegistry() {

        final RDFParserFactory parserFactory = RDFParserRegistry.getInstance()
                .get(NQuadsParser.nquads);

        assertNotNull(parserFactory);

        if (!(parserFactory instanceof NQuadsParserFactory))
            fail("Expecting " + NQuadsParserFactory.class.getName() + " not "
                    + parserFactory.getClass());

        final RDFParser parser = parserFactory.getParser();

        assertNotNull(parser);

        if (!(parser instanceof NQuadsParser))
            fail("Expecting " + NQuadsParser.class.getName() + " not "
                    + parser.getClass());

    }

}
