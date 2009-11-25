/*
 * Copyright SYSTAP, LLC 2006-2009.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package com.bigdata.sail.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.UUID;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

public class IOUtils {

    /**
     * Suck the character data from the reader into a string.
     * 
     * @param reader
     * @return
     * @throws IOException
     */
    public static String readString(Reader reader) throws IOException {
        StringWriter writer = new StringWriter();
        try {
            int i;
            while ((i = reader.read()) != -1) {
                writer.write(i);
            }
        } finally {
            reader.close();
            writer.close();
        }
        return writer.toString();
    }
    
    /**
     * Read the entire input byte stream, converting to characters using the
     * specified encoding and return the result as a string.
     * 
     * @param is
     * @param encoding
     * @return
     * @throws IOException
     */
    public static String readString(InputStream is, String encoding)
            throws IOException {
        return readString(new InputStreamReader(is, encoding));
    }

    /**
     * Deserialize an rdf/xml graph into a collection of bigdata statements.
     * 
     * @param rdfXml
     *          the rdf/xml to deserialize
     * @return
     *          the collection of statements
     */
    public static Collection<Statement> deserialize(String rdfXml) 
            throws Exception {
        
        final Collection<Statement> stmts = new LinkedHashSet<Statement>();
        final String namespace = UUID.randomUUID().toString();
        final RDFXMLParser rdfParser = new RDFXMLParser(
                BigdataValueFactoryImpl.getInstance(namespace));
        rdfParser.setVerifyData(false);
        rdfParser.setStopAtFirstError(true);
        rdfParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
        rdfParser.setRDFHandler(new StatementCollector(stmts));
        rdfParser.parse(new StringReader(rdfXml), "");
        return stmts;
        
    }

    /**
     * Serialize a collection of bigdata statements into an rdf/xml graph.
     * 
     * @param stmts
     *          the collection of statements
     * @return
     *          the serialized rdf/xml
     */
    public static String serialize(Collection<Statement> stmts) 
            throws Exception {
        
        final StringWriter sw = new StringWriter();
        final RDFXMLWriter writer = new RDFXMLWriter(sw);
        writer.startRDF();
        for (Statement stmt : stmts) {
            writer.handleStatement(stmt);
        }
        writer.endRDF();

        return sw.toString();
        
    }

}
