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

package com.bigdata.rdf.rio.rdfxml;

import java.io.ByteArrayOutputStream;

import junit.framework.TestCase;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.rio.rdfxml.RDFXMLParserFactory;

import com.bigdata.rdf.ServiceProviderHook;

/**
 * Test suite for the {@link RDFXMLParserFactory}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRDFXMLWriterFactory extends TestCase {

    /**
     * 
     */
    public TestRDFXMLWriterFactory() {
    }

    /**
     * @param name
     */
    public TestRDFXMLWriterFactory(String name) {
        super(name);
    }

    public void test_serviceRegistry() {

        /*
         * Note: You MUST use this method to *replace* the existing RDF/XML
         * parser/writer declarations.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/439">
         * Class loader problems </a>
         */
        ServiceProviderHook.forceLoad();

        final RDFWriterFactory parserFactory = RDFWriterRegistry.getInstance()
                .get(RDFFormat.RDFXML);

        assertNotNull(parserFactory);

        if (!(parserFactory instanceof BigdataRDFXMLWriterFactory))
            fail("Expecting " + BigdataRDFXMLWriterFactory.class.getName() + " not "
                    + parserFactory.getClass());

        final RDFWriter writer = parserFactory
                .getWriter(new ByteArrayOutputStream());

        assertNotNull(writer);

        if (!(writer instanceof BigdataRDFXMLWriter))
            fail("Expecting " + BigdataRDFXMLWriter.class.getName() + " not "
                    + writer.getClass());

    }

}
