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
 * Created on Jan 29, 2007
 */
package com.bigdata.rdf.sail;

import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;

import junit.framework.TestCase2;

import org.openrdf.model.Graph;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.openrdf.rio.rdfxml.RDFXMLWriter;

/**
 */
public class TestBryansParser extends TestCase2 {
    /**
     * 
     */
    public TestBryansParser() {
    }

    /**
     * @param name
     */
    public TestBryansParser(String name) {
        super(name);
    }

    public void testBryansParser1() throws Exception {
        File tmp = File.createTempFile("bigdata", ".jnl");
        tmp.delete();
        Properties sailProps = new Properties();
        sailProps.setProperty(BigdataSail.Options.FILE, tmp
                .getAbsolutePath());
        BigdataSail sail = new BigdataSail(sailProps);
        Repository repo = new BigdataSailRepository(sail);
        repo.initialize();
        RepositoryConnection cxn = repo.getConnection();
        try {
            cxn.add(
                    TestBryansParser.class
                            .getResourceAsStream("test.rdf"), "",
                    RDFFormat.RDFXML);
            {
                StringWriter sw = new StringWriter();
                cxn.export(new RDFXMLWriter(sw));
                System.out.println(sw.toString());
                Graph graph = new GraphImpl();
                final RDFXMLParser rdfParser =
                        new RDFXMLParser(sail.getValueFactory());
                /*
                 * Setting this verification to true causes an exception to be
                 * thrown.
                 */
                rdfParser.setVerifyData(true);
                rdfParser.setStopAtFirstError(true);
                rdfParser
                        .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
                rdfParser.setRDFHandler(new StatementCollector(graph));
                rdfParser.parse(new StringReader(sw.toString()), "");
            }
        } finally {
            cxn.close();
        }
    }

    public void testBryansParser2() throws Exception {
        File tmp = File.createTempFile("bigdata", ".jnl");
        tmp.delete();
        Properties sailProps = new Properties();
        sailProps.setProperty(BigdataSail.Options.FILE, tmp
                .getAbsolutePath());
        BigdataSail sail = new BigdataSail(sailProps);
        Repository repo = new BigdataSailRepository(sail);
        repo.initialize();
        RepositoryConnection cxn = repo.getConnection();
        try {
            cxn.add(
                    TestBryansParser.class
                            .getResourceAsStream("test.rdf"), "",
                    RDFFormat.RDFXML);
            {
                final URI entity =
                        new URIImpl(
                                "http://expcollab.net/aether/system#Entity");
                StringWriter sw = new StringWriter();
                cxn.exportStatements(
                        entity, null, null, true, new RDFXMLWriter(sw));
                /*
                 * First of all - look at the namespaces that are output by
                 * the writer - they are nonsensical.
                 */
                System.out.println(sw.toString());
                Graph graph = new GraphImpl();
                final RDFXMLParser rdfParser =
                        new RDFXMLParser(sail.getValueFactory());
                rdfParser.setVerifyData(false);
                rdfParser.setStopAtFirstError(true);
                rdfParser
                        .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
                rdfParser.setRDFHandler(new StatementCollector(graph));
                rdfParser.parse(new StringReader(sw.toString()), "");
                /*
                 * Compare the collection of statements that is parsed to the
                 * rdf/xml you just saw output above.  Does not match up at all.
                 */
                for (Statement stmt : graph) {
                    System.out.println(stmt);
                }
            }
        } finally {
            cxn.close();
        }
    }

}
