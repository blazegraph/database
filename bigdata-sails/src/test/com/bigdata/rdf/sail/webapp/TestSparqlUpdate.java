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
Portions of this code are:

Copyright Aduna (http://www.aduna-software.com/) � 2001-2007

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of the copyright holder nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package com.bigdata.rdf.sail.webapp;

import java.io.File;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.parser.sparql.DC;
import org.openrdf.query.parser.sparql.FOAF;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;

/**
 * Proxied test suite.
 *
 * @param <S>
 */
public class TestSparqlUpdate<S extends IIndexManager> extends
        AbstractTestNanoSparqlClient<S> {

    public TestSparqlUpdate() {

    }

	public TestSparqlUpdate(final String name) {

		super(name);

	}

    protected static final String EX_NS = "http://example.org/";

    protected ValueFactory f = new ValueFactoryImpl();
    protected URI bob, alice, graph1, graph2;
    protected RemoteRepository repo;

	@Override
	public void setUp() throws Exception {
	    
	    super.setUp();
	    
        repo = new RemoteRepository(m_serviceURL);
        
        /*
         * Only for testing. Clients should use AddOp(File, RDFFormat).
         * 
         * TODO Do this using LOAD or just write tests for LOAD?
         */
        final AddOp add = new AddOp(
                new File(
                        "bigdata-sails/src/test/com/bigdata/rdf/sail/webapp/dataset-update.trig"),
                RDFFormat.TRIG);
        
        repo.add(add);

        bob = f.createURI(EX_NS, "bob");
        alice = f.createURI(EX_NS, "alice");

        graph1 = f.createURI(EX_NS, "graph1");
        graph2 = f.createURI(EX_NS, "graph2");
	}
	
	public void tearDown() throws Exception {
	    
	    bob = alice = graph1 = graph2 = null;
	    
	    f = null;
	    
	    super.tearDown();
	    
	}
	
    /**
     * Get a set of useful namespace prefix declarations.
     * 
     * @return namespace prefix declarations for rdf, rdfs, dc, foaf and ex.
     */
    protected String getNamespaceDeclarations() {
        final StringBuilder declarations = new StringBuilder();
        declarations.append("PREFIX rdf: <" + RDF.NAMESPACE + "> \n");
        declarations.append("PREFIX rdfs: <" + RDFS.NAMESPACE + "> \n");
        declarations.append("PREFIX dc: <" + DC.NAMESPACE + "> \n");
        declarations.append("PREFIX foaf: <" + FOAF.NAMESPACE + "> \n");
        declarations.append("PREFIX ex: <" + EX_NS + "> \n");
        declarations.append("PREFIX xsd: <" +  XMLSchema.NAMESPACE + "> \n");
        declarations.append("\n");

        return declarations.toString();
    }

    protected boolean hasStatement(final Resource subj, final URI pred,
            final Value obj, final boolean includeInferred,
            final Resource... contexts) throws RepositoryException {

        try {

            return !repo.getStatements(subj, pred, obj, includeInferred,
                    contexts).isEmpty();
            
        } catch (Exception e) {
            
            throw new RepositoryException(e);
            
        }

    }
    
    public void testInsertWhere()
            throws Exception
    {
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

        assertFalse(hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertFalse(hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertTrue(hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
    }
    
////    //@Test
    public void testDeleteInsertWhere()
        throws Exception
    {
//        logger.debug("executing test DeleteInsertWhere");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE { ?x foaf:name ?y } INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

        assertFalse(hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertFalse(hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertTrue(hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

        assertFalse(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertFalse(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

    }

    //@Test
    public void testInsertTransformedWhere()
        throws Exception
    {
//        logger.debug("executing test InsertTransformedWhere");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT {?x rdfs:label [] . } WHERE {?y ex:containsPerson ?x.  }");

        assertFalse(hasStatement(bob, RDFS.LABEL, null, true));
        assertFalse(hasStatement(alice, RDFS.LABEL, null, true));

        repo.prepareUpdate(update.toString()).evaluate();

        /**
         * FIXME getStatements() is hitting a problem in the ASTConstruct
         * iterator where a blank node is being reported without a letter (_:18
         * versus _:B18). However, there are a number of DAWG tests which fail
         * if we just wrap any blank node with the canonicalizing mapping (those
         * which deal with reification) so this change needs to be implemented
         * carefully. One of the test failures that can be created this way is
         * in TestTCK. The others show up when you run the full TCK. They are:
         * 
         * <pre>
         * dawg-construct-identity
         * dawg-construct-reification-1
         * dawg-construct-reification-2
         * </pre>
         */
        assertTrue(hasStatement(bob, RDFS.LABEL, null, true));
        assertTrue(hasStatement(alice, RDFS.LABEL, null, true));
    }

    //@Test
    public void testInsertWhereGraph()
        throws Exception
    {
//        logger.debug("executing testInsertWhereGraph");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT {GRAPH ?g {?x rdfs:label ?y . }} WHERE {GRAPH ?g {?x foaf:name ?y }}");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        
        repo.prepareUpdate(update.toString()).evaluate();

        String message = "labels should have been inserted in corresponding named graphs only.";
        assertTrue(message, hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph1));
        assertFalse(message, hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph2));
        assertTrue(message, hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph2));
        assertFalse(message, hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph1));
    }

    //@Test
    public void testInsertWhereUsing()
        throws Exception
    {

//        logger.debug("executing testInsertWhereUsing");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT {?x rdfs:label ?y . } USING ex:graph1 WHERE {?x foaf:name ?y }");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();

        repo.prepareUpdate(update.toString()).evaluate();

        String message = "label should have been inserted in default graph, for ex:bob only";
        assertTrue(message, hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertFalse(message, hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph1));
        assertFalse(message, hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph2));
        /*
         * Note: I added the following line to verify that <alice, rdfs:label,
         * "Alice"> is not asserted into the default graph either.
         */
        assertFalse(message, hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
        assertFalse(message, hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph2));
        assertFalse(message, hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph1));
    }

    //@Test
    public void testInsertWhereWith()
        throws Exception
    {
//        logger.debug("executing testInsertWhereWith");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("WITH ex:graph1 INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        
        repo.prepareUpdate(update.toString()).evaluate();

        String message = "label should have been inserted in graph1 only, for ex:bob only";
        assertTrue(message, hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph1));
        assertFalse(message, hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph2));
        assertFalse(message, hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph2));
        assertFalse(message, hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph1));
    }

    //@Test
    public void testDeleteWhereShortcut()
        throws Exception
    {
//        logger.debug("executing testDeleteWhereShortcut");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE WHERE {?x foaf:name ?y }");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertTrue(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

        repo.prepareUpdate(update.toString()).evaluate();

        String msg = "foaf:name properties should have been deleted";
        assertFalse(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertFalse(msg, hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

        msg = "foaf:knows properties should not have been deleted";
        assertTrue(msg, hasStatement(bob, FOAF.KNOWS, null, true));
        assertTrue(msg, hasStatement(alice, FOAF.KNOWS, null, true));
    }

    //@Test
    public void testDeleteWhere()
        throws Exception
    {
//        logger.debug("executing testDeleteWhere");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE {?x foaf:name ?y } WHERE {?x foaf:name ?y }");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertTrue(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        String msg = "foaf:name properties should have been deleted";
        assertFalse(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertFalse(msg, hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

    }

    //@Test
    public void testDeleteTransformedWhere()
        throws Exception
    {
//        logger.debug("executing testDeleteTransformedWhere");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE {?y foaf:name [] } WHERE {?x ex:containsPerson ?y }");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertTrue(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        String msg = "foaf:name properties should have been deleted";
        assertFalse(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertFalse(msg, hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

        msg = "ex:containsPerson properties should not have been deleted";
        assertTrue(msg, hasStatement(graph1, f.createURI(EX_NS, "containsPerson"), bob, true));
        assertTrue(msg, hasStatement(graph2, f.createURI(EX_NS, "containsPerson"), alice, true));

    }

    //@Test
    public void testInsertData()
        throws Exception
    {
//        logger.debug("executing testInsertData");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT DATA { ex:book1 dc:title \"book 1\" ; dc:creator \"Ringo\" . } ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        URI book1 = f.createURI(EX_NS, "book1");

        assertFalse(hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true));
        assertFalse(hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true));

//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        String msg = "two new statements about ex:book1 should have been inserted";
        assertTrue(msg, hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true));
        assertTrue(msg, hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true));
    }

    //@Test
    public void testInsertDataMultiplePatterns()
        throws Exception
    {
//        logger.debug("executing testInsertData");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT DATA { ex:book1 dc:title \"book 1\". ex:book1 dc:creator \"Ringo\" . ex:book2 dc:creator \"George\". } ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        URI book1 = f.createURI(EX_NS, "book1");
        URI book2 = f.createURI(EX_NS, "book2");

        assertFalse(hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true));
        assertFalse(hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true));
        assertFalse(hasStatement(book2, DC.CREATOR, f.createLiteral("George"), true));

//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        String msg = "newly inserted statement missing";
        assertTrue(msg, hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true));
        assertTrue(msg, hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true));
        assertTrue(msg, hasStatement(book2, DC.CREATOR, f.createLiteral("George"), true));
    }

    //@Test
    public void testInsertDataInGraph()
        throws Exception
    {
//        logger.debug("executing testInsertDataInGraph");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT DATA { GRAPH ex:graph1 { ex:book1 dc:title \"book 1\" ; dc:creator \"Ringo\" . } } ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        URI book1 = f.createURI(EX_NS, "book1");

        assertFalse(hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true, graph1));
        assertFalse(hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true, graph1));

        repo.prepareUpdate(update.toString()).evaluate();
//        operation.execute();

        String msg = "two new statements about ex:book1 should have been inserted in graph1";
        assertTrue(msg, hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true, graph1));
        assertTrue(msg, hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true, graph1));
    }

    //@Test
    public void testDeleteData()
        throws Exception
    {
//        logger.debug("executing testDeleteData");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE DATA { ex:alice foaf:knows ex:bob. } ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(alice, FOAF.KNOWS, bob, true));
        
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        String msg = "statement should have been deleted.";
        assertFalse(msg, hasStatement(alice, FOAF.KNOWS, bob, true));
    }

    //@Test
    public void testDeleteDataMultiplePatterns()
        throws Exception
    {
//        logger.debug("executing testDeleteData");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE DATA { ex:alice foaf:knows ex:bob. ex:alice foaf:mbox \"alice@example.org\" .} ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(alice, FOAF.KNOWS, bob, true));
        assertTrue(hasStatement(alice, FOAF.MBOX, f.createLiteral("alice@example.org"), true));
    
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        String msg = "statement should have been deleted.";
        assertFalse(msg, hasStatement(alice, FOAF.KNOWS, bob, true));
        assertFalse(msg, hasStatement(alice, FOAF.MBOX, f.createLiteral("alice@example.org"), true));
    }

    //@Test
    public void testDeleteDataFromGraph()
        throws Exception
    {
//        logger.debug("executing testDeleteDataFromGraph");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE DATA { GRAPH ex:graph1 {ex:alice foaf:knows ex:bob. } } ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(alice, FOAF.KNOWS, bob, true, graph1));
    
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        String msg = "statement should have been deleted from graph1";
        assertFalse(msg, hasStatement(alice, FOAF.KNOWS, bob, true, graph1));
    }

    //@Test
    public void testDeleteDataFromWrongGraph()
        throws Exception
    {
//        logger.debug("executing testDeleteDataFromWrongGraph");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());

        // statement does not exist in graph2.
        update.append("DELETE DATA { GRAPH ex:graph2 {ex:alice foaf:knows ex:bob. } } ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(alice, FOAF.KNOWS, bob, true, graph1));
        assertFalse(hasStatement(alice, FOAF.KNOWS, bob, true, graph2));
        
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        String msg = "statement should have not have been deleted from graph1";
        assertTrue(msg, hasStatement(alice, FOAF.KNOWS, bob, true, graph1));
    }

    //@Test
    public void testCreateNewGraph()
        throws Exception
    {
//        logger.debug("executing testCreateNewGraph");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());

        URI newGraph = f.createURI(EX_NS, "new-graph");

        update.append("CREATE GRAPH <" + newGraph + "> ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(null, null, null, false, graph1));
        assertTrue(hasStatement(null, null, null, false, graph2));
        assertFalse(hasStatement(null, null, null, false, newGraph));
        assertTrue(hasStatement(null, null, null, false));
    }

    //@Test
    public void testCreateExistingGraph()
        throws Exception
    {
//        logger.debug("executing testCreateExistingGraph");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("CREATE GRAPH <" + graph1 + "> ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        try {
            repo.prepareUpdate(update.toString()).evaluate();
//            operation.execute();

            fail("creation of existing graph should have resulted in error.");
        }
        catch (Exception e) {
            // expected behavior
//            con.rollback();
            log.info("Exception: " + e, e);
        }
    }

    //@Test
    public void testCopyToDefault()
        throws Exception
    {
//        logger.debug("executing testCopyToDefault");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("COPY GRAPH <" + graph1.stringValue() + "> TO DEFAULT");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
      
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();
        
        assertFalse(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertFalse(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, (Resource)null));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph1));
    }

    //@Test
    public void testCopyToExistingNamed()
        throws Exception
    {
//        logger.debug("executing testCopyToExistingNamed");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("COPY GRAPH ex:graph1 TO ex:graph2");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph2));
        assertFalse(hasStatement(alice, FOAF.NAME, null, false, graph2));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph1));
    }

    //@Test
    public void testCopyToNewNamed()
        throws Exception
    {
//        logger.debug("executing testCopyToNewNamed");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("COPY GRAPH ex:graph1 TO ex:graph3");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        
        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, FOAF.NAME, null, false, f.createURI(EX_NS, "graph3")));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph1));
    }

    //@Test
    public void testCopyFromDefault()
        throws Exception
    {
//        logger.debug("executing testCopyFromDefault");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("COPY DEFAULT TO ex:graph3");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
    
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));

    }

    //@Test
    public void testCopyFromDefaultToDefault()
        throws Exception
    {
//        logger.debug("executing testCopyFromDefaultToDefault");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("COPY DEFAULT TO DEFAULT");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
    
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
    }

    //@Test
    public void testAddToDefault()
        throws Exception
    {
//        logger.debug("executing testAddToDefault");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("ADD GRAPH <" + graph1.stringValue() + "> TO DEFAULT");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, (Resource)null));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph1));
    }

    //@Test
    public void testAddToExistingNamed()
        throws Exception
    {
//        logger.debug("executing testAddToExistingNamed");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("ADD GRAPH ex:graph1 TO ex:graph2");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph2));
        assertTrue(hasStatement(alice, FOAF.NAME, null, false, graph2));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph1));
    }

    //@Test
    public void testAddToNewNamed()
        throws Exception
    {
//        logger.debug("executing testAddToNewNamed");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("ADD GRAPH ex:graph1 TO ex:graph3");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        
        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, FOAF.NAME, null, false, f.createURI(EX_NS, "graph3")));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph1));
    }

    //@Test
    public void testAddFromDefault()
        throws Exception
    {
//        logger.debug("executing testAddFromDefault");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("ADD DEFAULT TO ex:graph3");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));

    }

    //@Test
    public void testAddFromDefaultToDefault()
        throws Exception
    {
//        logger.debug("executing testAddFromDefaultToDefault");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("ADD DEFAULT TO DEFAULT");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
    }

    //@Test
    public void testMoveToDefault()
        throws Exception
    {
//        logger.debug("executing testMoveToDefault");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("MOVE GRAPH <" + graph1.stringValue() + "> TO DEFAULT");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertFalse(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, (Resource)null));
        assertFalse(hasStatement(null, null, null, false, graph1));
    }

    //@Test
    public void testMoveToNewNamed()
        throws Exception
    {
//        logger.debug("executing testMoveToNewNamed");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("MOVE GRAPH ex:graph1 TO ex:graph3");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        
        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, FOAF.NAME, null, false, f.createURI(EX_NS, "graph3")));
        assertFalse(hasStatement(null, null, null, false, graph1));
    }

    //@Test
    public void testMoveFromDefault()
        throws Exception
    {
//        logger.debug("executing testMoveFromDefault");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("MOVE DEFAULT TO ex:graph3");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertFalse(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));

    }

    //@Test
    public void testMoveFromDefaultToDefault()
        throws Exception
    {
//        logger.debug("executing testMoveFromDefaultToDefault");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("MOVE DEFAULT TO DEFAULT");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
    }

    //@Test
    public void testClearAll()
        throws Exception
    {
//        logger.debug("executing testClearAll");
        String update = "CLEAR ALL";

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);
//
//        operation.execute();
        
        repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(null, null, null, false));

    }

    //@Test
    public void testClearGraph()
        throws Exception
    {
//        logger.debug("executing testClearGraph");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("CLEAR GRAPH <" + graph1.stringValue() + "> ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        
        repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(null, null, null, false, graph1));
        assertTrue(hasStatement(null, null, null, false, graph2));
        assertTrue(hasStatement(null, null, null, false));
    }

    //@Test
    public void testClearNamed()
        throws Exception
    {
//        logger.debug("executing testClearNamed");
        String update = "CLEAR NAMED";

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);
//
//        operation.execute();
        
        repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(null, null, null, false, graph1));
        assertFalse(hasStatement(null, null, null, false, graph2));
        assertTrue(hasStatement(null, null, null, false));

    }

    //@Test
    public void testClearDefault()
        throws Exception
    {
//        logger.debug("executing testClearDefault");

        String update = "CLEAR DEFAULT";

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

        // Verify statements exist in the named graphs.
        assertTrue(hasStatement(null, null, null, false, graph1));
        assertTrue(hasStatement(null, null, null, false, graph2));
        // Verify statements exist in the default graph.
        assertTrue(hasStatement(null, null, null, false, new Resource[]{null}));

//        System.err.println(dumpStore());
        
//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(null, null, null, false, graph1));
        assertTrue(hasStatement(null, null, null, false, graph2));
        // Verify that no statements remain in the 'default' graph.
        assertFalse(hasStatement(null, null, null, false, new Resource[]{null}));
        
//      System.err.println(dumpStore());
    }

    //@Test
    public void testDropAll()
        throws Exception
    {
//        logger.debug("executing testDropAll");
        String update = "DROP ALL";

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);
//
//        operation.execute();
        
        repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(null, null, null, false));

    }

    //@Test
    public void testDropGraph()
        throws Exception
    {
//        logger.debug("executing testDropGraph");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DROP GRAPH <" + graph1.stringValue() + "> ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        
        repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(null, null, null, false, graph1));
        assertTrue(hasStatement(null, null, null, false, graph2));
        assertTrue(hasStatement(null, null, null, false));
    }

    //@Test
    public void testDropNamed()
        throws Exception
    {
//        logger.debug("executing testDropNamed");

        String update = "DROP NAMED";

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);
//
//        operation.execute();
        
        repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(null, null, null, false, graph1));
        assertFalse(hasStatement(null, null, null, false, graph2));
        assertTrue(hasStatement(null, null, null, false));
    }

    //@Test
    public void testDropDefault()
        throws Exception
    {
//        logger.debug("executing testDropDefault");

        String update = "DROP DEFAULT";

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

        // Verify statements exist in the named graphs.
        assertTrue(hasStatement(null, null, null, false, graph1));
        assertTrue(hasStatement(null, null, null, false, graph2));
        // Verify statements exist in the default graph.
        assertTrue(hasStatement(null, null, null, false, new Resource[]{null}));

//        operation.execute();
        
        repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(null, null, null, false, graph1));
        assertTrue(hasStatement(null, null, null, false, graph2));
        // Verify that no statements remain in the 'default' graph.
        assertFalse(hasStatement(null, null, null, false, new Resource[]{null}));

    }

    //@Test
    public void testUpdateSequenceDeleteInsert()
        throws Exception
    {
//        logger.debug("executing testUpdateSequenceDeleteInsert");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE {?y foaf:name [] } WHERE {?x ex:containsPerson ?y }; ");
        update.append(getNamespaceDeclarations());
        update.append("INSERT {?x foaf:name \"foo\" } WHERE {?y ex:containsPerson ?x} ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertTrue(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

//        operation.execute();

        repo.prepareUpdate(update.toString()).evaluate();
        
        String msg = "foaf:name properties should have been deleted";
        assertFalse(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertFalse(msg, hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

        msg = "foaf:name properties with value 'foo' should have been added";
        assertTrue(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("foo"), true));
        assertTrue(msg, hasStatement(alice, FOAF.NAME, f.createLiteral("foo"), true));
    }

    //@Test
    public void testUpdateSequenceInsertDelete()
        throws Exception
    {
//        logger.debug("executing testUpdateSequenceInsertDelete");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT {?x foaf:name \"foo\" } WHERE {?y ex:containsPerson ?x}; ");
        update.append(getNamespaceDeclarations());
        update.append("DELETE {?y foaf:name [] } WHERE {?x ex:containsPerson ?y } ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertTrue(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        String msg = "foaf:name properties should have been deleted";
        assertFalse(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertFalse(msg, hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

        msg = "foaf:name properties with value 'foo' should not have been added";
        assertFalse(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("foo"), true));
        assertFalse(msg, hasStatement(alice, FOAF.NAME, f.createLiteral("foo"), true));
    }

    //@Test
    public void testUpdateSequenceInsertDelete2()
        throws Exception
    {
//        logger.debug("executing testUpdateSequenceInsertDelete2");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT { GRAPH ex:graph2 { ?s ?p ?o } } WHERE { GRAPH ex:graph1 { ?s ?p ?o . FILTER (?s = ex:bob) } }; ");
        update.append("WITH ex:graph1 DELETE { ?s ?p ?o } WHERE {?s ?p ?o . FILTER (?s = ex:bob) } ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true, graph1));
        assertTrue(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true, graph2));

//        operation.execute();
        repo.prepareUpdate(update.toString()).evaluate();

        String msg = "statements about bob should have been removed from graph1";
        assertFalse(msg, hasStatement(bob, null, null, true, graph1));

        msg = "statements about bob should have been added to graph2";
        assertTrue(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true, graph2));
        assertTrue(msg, hasStatement(bob, FOAF.MBOX, null, true, graph2));
        assertTrue(msg, hasStatement(bob, FOAF.KNOWS, alice, true, graph2));
    }

    public void testLoad()
            throws Exception
        {
        final String update = "LOAD <file:bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf>";
        
        final String ns = "http://bigdata.com/test/data#";
        
        repo.prepareUpdate(update).evaluate();
        
        assertTrue(hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true));

    }

    //@Test
    public void testLoadSilent()
        throws Exception
    {
        final String update = "LOAD SILENT <file:bigdata-rdf/src/test/com/bigdata/rdf/rio/NOT-FOUND.rdf>";
        
        final String ns = "http://bigdata.com/test/data#";
        
        repo.prepareUpdate(update).evaluate();

        assertFalse(hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true));

    }

    //@Test
    public void testLoadIntoGraph()
        throws Exception
    {

        final URI g1 = f.createURI("http://www.bigdata.com/g1");

        final String update = "LOAD <file:bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf> "
                + "INTO GRAPH <" + g1.stringValue() + ">";
        
        final String ns = "http://bigdata.com/test/data#";
        
        repo.prepareUpdate(update).evaluate();

        assertFalse(hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true, (Resource)null));

        assertTrue(hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true, g1));

    }

//    //@Test
//    public void testUpdateSequenceInsertDeleteExample9()
//        throws Exception
//    {
//        logger.debug("executing testUpdateSequenceInsertDeleteExample9");
//
//        // replace the standard dataset with one specific to this case.
//        con.clear();
//        con.commit();
//        loadDataset("/testdata-update/dataset-update-example9.trig");
//
//        URI book1 = f.createURI("http://example/book1");
//        URI book3 = f.createURI("http://example/book3");
//        URI bookStore = f.createURI("http://example/bookStore");
//        URI bookStore2 = f.createURI("http://example/bookStore2");
//        
//        final StringBuilder update = new StringBuilder();
//        update.append("prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ");
//        update.append("prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>  ");
//        update.append("prefix xsd: <http://www.w3.org/2001/XMLSchema#>  ");
//        update.append("prefix dc: <http://purl.org/dc/elements/1.1/>  ");
//        update.append("prefix dcmitype: <http://purl.org/dc/dcmitype/>  ");
//        update.append("INSERT  { GRAPH <http://example/bookStore2> { ?book ?p ?v } } ");
//        update.append(" WHERE ");
//        update.append(" { GRAPH  <http://example/bookStore> ");
//        update.append("   { ?book dc:date ?date . ");
//        update.append("       FILTER ( ?date < \"2000-01-01T00:00:00-02:00\"^^xsd:dateTime ) ");
//        update.append("       ?book ?p ?v ");
//        update.append("      } ");
//        update.append(" } ;");
//        update.append("WITH <http://example/bookStore> ");
//        update.append(" DELETE { ?book ?p ?v } ");
//        update.append(" WHERE ");
//        update.append(" { ?book dc:date ?date ; ");
//        update.append("         a dcmitype:PhysicalObject .");
//        update.append("    FILTER ( ?date < \"2000-01-01T00:00:00-02:00\"^^xsd:dateTime ) ");
//        update.append("   ?book ?p ?v");
//        update.append(" } ");
//
//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
//
//        String msg = "statements about book1 should have been removed from bookStore";
//        assertFalse(msg, hasStatement(book1, null, null, true, bookStore));
//
//        msg = "statements about book1 should have been added to bookStore2";
//        assertTrue(msg, hasStatement(book1, RDF.TYPE, null, true, bookStore2));
//        assertTrue(msg, hasStatement(book1, DC.DATE, null, true, bookStore2));
//        assertTrue(msg, hasStatement(book1, DC.TITLE, null, true, bookStore2));
//    }

}
