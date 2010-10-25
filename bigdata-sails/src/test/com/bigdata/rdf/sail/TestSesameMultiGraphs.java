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
 * Created on Sep 16, 2009
 */

package com.bigdata.rdf.sail;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.memory.MemoryStore;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestSesameMultiGraphs {

    public static void main(String[] args) throws Exception {

        final Sail sail;
        final SailRepository repo;
        final SailRepositoryConnection cxn;
        
        sail = new MemoryStore();
        repo = new SailRepository(sail);
        
        repo.initialize();
        cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();
            
            final String ns = "http://namespace/";
            
            URI a = vf.createURI(ns+"a");
            URI b = vf.createURI(ns+"b");
            URI c = vf.createURI(ns+"c");
            URI g1 = vf.createURI(ns+"graph1");
            URI g2 = vf.createURI(ns+"graph2");
/**/
            cxn.setNamespace("ns", ns);
            
            cxn.add(a, b, c, g1, g2);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.commit();//
            
            {
                
                String query = 
                    "PREFIX rdf: <"+RDF.NAMESPACE+"> " +
                    "PREFIX rdfs: <"+RDFS.NAMESPACE+"> " +
                    "PREFIX ns: <"+ns+"> " +
                    
                    "select ?p ?o " +
                    "WHERE { " +
                    "  ns:a ?p ?o . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();
                
                System.err.println("no dataset specified, RDF-MERGE, should produce one solution:");
                while (result.hasNext()) {
                    System.err.println(result.next());
                }
 
            }
            
            {
                
                String query = 
                    "PREFIX rdf: <"+RDF.NAMESPACE+"> " +
                    "PREFIX rdfs: <"+RDFS.NAMESPACE+"> " +
                    "PREFIX ns: <"+ns+"> " +
                    
                    "select ?p ?o " +
                    "from <"+g1+">" +
                    "from <"+g2+">" +
                    "WHERE { " +
                    "  ns:a ?p ?o . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();
                
                System.err.println("default graph query, RDF-MERGE, should produce one solution:");
                while (result.hasNext()) {
                    System.err.println(result.next());
                }
 
            }
            
            {
                
                String query = 
                    "PREFIX rdf: <"+RDF.NAMESPACE+"> " +
                    "PREFIX rdfs: <"+RDFS.NAMESPACE+"> " +
                    "PREFIX ns: <"+ns+"> " +
                    
                    "select ?p ?o " +
                    "from named <"+g1+">" +
                    "from named <"+g2+">" +
                    "WHERE { " +
                    "  graph ?g { ns:a ?p ?o . } " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();
                
                System.err.println("named graph query, no RDF-MERGE, should produce two solutions:");
                while (result.hasNext()) {
                    System.err.println(result.next());
                }
 
            }
            
        } finally {
            cxn.close();
            sail.shutDown();
        }

    }
    
}
