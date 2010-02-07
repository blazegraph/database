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
package com.bigdata.sail.rest;

import info.aduna.xml.XMLWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collection;
import java.util.UUID;
import org.openrdf.model.Graph;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.query.QueryResultUtil;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLParser;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
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
    public static Graph rdfXmlToStatements(String rdfXml) 
            throws Exception {
        
        //final Collection<Statement> stmts = new LinkedHashSet<Statement>();
        final Graph graph = new GraphImpl();
        final String namespace = UUID.randomUUID().toString();
        final RDFXMLParser rdfParser = new RDFXMLParser(
                BigdataValueFactoryImpl.getInstance(namespace));
        rdfParser.setVerifyData(false);
        rdfParser.setStopAtFirstError(true);
        rdfParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
        rdfParser.setRDFHandler(new StatementCollector(graph));
        rdfParser.parse(new StringReader(rdfXml), "");
        return graph;
        
    }

    /**
     * Serialize a collection of statements into an rdf/xml graph.
     * 
     * @param graph
     *          the collection of statements
     * @return
     *          the serialized rdf/xml
     */
    public static String statementsToRdfXml(Collection<Statement> graph) 
            throws Exception {
        
        final StringWriter sw = new StringWriter();
        final RDFXMLWriter writer = new RDFXMLWriter(sw);
        writer.startRDF();
        for (Statement stmt : graph) {
            writer.handleStatement(stmt);
        }
        writer.endRDF();

        return sw.toString();
        
    }

    /**
     * Deserialize SPARQL results xml into a TupleQueryResult object.
     * 
     * @param xml
     *          the xml to deserialize
     * @return
     *          the TupleQueryResult object
     */
    public static TupleQueryResult xmlToSolutions(String xml) 
            throws Exception {

        TupleQueryResultParser parser = new SPARQLResultsXMLParser();
        TupleQueryResultBuilder qrBuilder = new TupleQueryResultBuilder();
        parser.setTupleQueryResultHandler(qrBuilder);
        parser.parse(new ByteArrayInputStream(xml.getBytes()));
        return qrBuilder.getQueryResult();
        
    }

    /**
     * Serialize a collection of statements into an rdf/xml graph.
     * 
     * @param graph
     *          the collection of statements
     * @return
     *          the serialized rdf/xml
     */
    public static String solutionsToXml(TupleQueryResult solutions) 
            throws Exception {
        
        StringWriter writer = new StringWriter();
        TupleQueryResultHandler handler = 
            new SPARQLResultsXMLWriter(new XMLWriter(writer));
        QueryResultUtil.report(solutions, handler);
        return writer.toString();
        
    }
    
}
