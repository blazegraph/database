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
Portions of this code are:

Copyright Aduna (http://www.aduna-software.com/) 2001-2007

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
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedHashSet;

import junit.framework.Test;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.DC;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.parser.sparql.SPARQLUpdateTest;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.IPreparedTupleQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;

/**
 * Proxied test suite.
 * <p>
 * Note: Also see {@link SPARQLUpdateTest}. These two test suites SHOULD be kept
 * synchronized. {@link SPARQLUpdateTest} runs against a local kb instance while
 * this class runs against the NSS. The two test suites are not exactly the same
 * because one uses the {@link RemoteRepository} to communicate with the NSS
 * while the other uses the local API.
 * 
 * @param <S>
 * 
 * @see SPARQLUpdateTest
 */
public class TestSparqlUpdate<S extends IIndexManager> extends
        AbstractTestNanoSparqlClient<S> {
    
    public TestSparqlUpdate() {

    }

	public TestSparqlUpdate(final String name) {

		super(name);

	}

   /**
    * We need to be running this test suite for each of the BufferModes that we
    * want to support. This is because there are subtle interactions between the
    * BufferMode, the AbstractTask, and the execution of mutation operations.
    * One approach might be to pass in a collection of BufferMode values rather
    * than a singleton and then generate the test suite for each BufferMode
    * value in that collection [I've tried this, but I am missing something in
    * the proxy test pattern with the outcome that the tests are not properly
    * distinct.]
    */
	static public Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(TestSparqlUpdate.class,
		      "test.*",
//		      "testMoveFromDefault",
//				"testStressInsertWhereGraph",
//				"testInsertWhereGraph",
				new LinkedHashSet<BufferMode>(Arrays.asList(new BufferMode[]{
				BufferMode.Transient, 
				BufferMode.DiskWORM, 
				BufferMode.MemStore,
				BufferMode.DiskRW, 
				})),
				TestMode.quads
				);
	}

	private static final String EX_NS = "http://example.org/";

    private ValueFactory f = new ValueFactoryImpl();
    private URI bob, alice, graph1, graph2;
//    protected RemoteRepository m_repo;

	@Override
	public void setUp() throws Exception {

		super.setUp();
	    
//        m_repo = new RemoteRepository(m_serviceURL);

		// Load the test data set.
		doLoadFile();

        bob = f.createURI(EX_NS, "bob");
        alice = f.createURI(EX_NS, "alice");

        graph1 = f.createURI(EX_NS, "graph1");
        graph2 = f.createURI(EX_NS, "graph2");
	}
	
	/**
	 * Load the test data set.
	 * 
	 * @throws Exception
	 */
	private void doLoadFile() throws Exception {
        /*
		 * Only for testing. Clients should use AddOp(File, RDFFormat) or SPARQL
		 * UPDATE "LOAD".
		 */
        loadFile(
                "src/test/java/com/bigdata/rdf/sail/webapp/dataset-update.trig",
                RDFFormat.TRIG);
	}
	
	@Override
	public void tearDown() throws Exception {
	    
	    bob = alice = graph1 = graph2 = null;
	    
	    f = null;
	    
	    super.tearDown();
	    
	}

    /**
     * Load a file.
     * 
     * @param file
     *            The file.
     * @param format
     *            The file format.
     * @throws Exception
     */
    protected void loadFile(final String file, final RDFFormat format)
            throws Exception {

        final AddOp add = new AddOp(new File(file), format);

        m_repo.add(add);

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

         return m_repo.hasStatement(subj, pred, obj, includeInferred, contexts);

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

        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertTrue(hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
    }
    
//    /**
//     * TODO Requires BINDINGS support for {@link RemoteRepository}
//     * 
//     * @since openrdf 2.6.3
//     */
////      @Test
//    public void testInsertWhereWithBinding()
//        throws Exception
//    {
//        if (!BINDINGS)
//            return;
//        log.debug("executing test testInsertWhereWithBinding");
//        final StringBuilder update = new StringBuilder();
//        update.append(getNamespaceDeclarations());
//        update.append("INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");
//
//        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL,update.toString());
//        operation.setBinding("x", bob);
//
//        assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
//        assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
//
//        operation.execute();
//
//        assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
//        assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
//    }
//
//    /**
//     * TODO Requires BINDINGS support for {@link RemoteRepository}
//     *
//     * @since openrdf 2.6.6
//     */
//    public void testInsertWhereWithBindings2()
//        throws Exception
//    {
//        if (!BINDINGS)
//            return;
//        log.debug("executing test testInsertWhereWithBindings2");
//        StringBuilder update = new StringBuilder();
//        update.append(getNamespaceDeclarations());
//        update.append("INSERT {?x rdfs:label ?z . } WHERE {?x foaf:name ?y }");
//
//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//        operation.setBinding("z", f.createLiteral("Bobbie"));
//        operation.setBinding("x", bob);
//
//        assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bobbie"), true));
//        assertFalse(con.hasStatement(alice, RDFS.LABEL, null, true));
//
//        operation.execute();
//
//        assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bobbie"), true));
//        assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
//    }

    /**
     * @since openrdf 2.6.3
     */
//      @Test
    public void testInsertEmptyWhere()
        throws Exception
    {
        log.debug("executing test testInsertEmptyWhere");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT { <" + bob + "> rdfs:label \"Bob\" . } WHERE { }");

        assertFalse(hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));

        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
    }

//    /**
//     * TODO Requires BINDINGS support for {@link RemoteRepository}
//     *
//     * @since openrdf 2.6.3
//     */
////      @Test
//    public void testInsertEmptyWhereWithBinding()
//        throws Exception
//    {
//        if (!BINDINGS)
//            return;
//        log.debug("executing test testInsertEmptyWhereWithBinding");
//        final StringBuilder update = new StringBuilder();
//        update.append(getNamespaceDeclarations());
//        update.append("INSERT {?x rdfs:label ?y . } WHERE { }");
//
//        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//        operation.setBinding("x", bob);
//        operation.setBinding("y", f.createLiteral("Bob"));
//
//        assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
//
//        operation.execute();
//
//        assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
//    }

    /**
     * @since openrdf 2.6.3
     */
//      @Test
    public void testInsertNonMatchingWhere()
        throws Exception
    {
        log.debug("executing test testInsertNonMatchingWhere");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT { ?x rdfs:label ?y . } WHERE { ?x rdfs:comment ?y }");

        assertFalse(hasStatement(bob, RDFS.LABEL, null, true));

        m_repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(bob, RDFS.LABEL, null, true));
    }

//    /**
//  * TODO Requires BINDINGS support for {@link RemoteRepository}
//  *
//     * @since openrdf 2.6.3
//     */
////      @Test
//    public void testInsertNonMatchingWhereWithBindings()
//        throws Exception
//    {
//        if (!BINDINGS)
//            return;
//        log.debug("executing test testInsertNonMatchingWhereWithBindings");
//        final StringBuilder update = new StringBuilder();
//        update.append(getNamespaceDeclarations());
//        update.append("INSERT { ?x rdfs:label ?y . } WHERE { ?x rdfs:comment ?y }");
//
//        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//        operation.setBinding("x", bob);
//        operation.setBinding("y", f.createLiteral("Bob"));
//
//        assertFalse(con.hasStatement(bob, RDFS.LABEL, null, true));
//
//        operation.execute();
//
//        assertFalse(con.hasStatement(bob, RDFS.LABEL, null, true));
//    }

//    /**
//  * TODO Requires BINDINGS support for {@link RemoteRepository}
//  *
//     * @since openrdf 2.6.3
//     */
////      @Test
//    public void testInsertWhereWithBindings()
//        throws Exception
//    {
//        if (!BINDINGS)
//            return;
//        log.debug("executing test testInsertWhereWithBindings");
//        final StringBuilder update = new StringBuilder();
//        update.append(getNamespaceDeclarations());
//        update.append("INSERT { ?x rdfs:comment ?z . } WHERE { ?x foaf:name ?y }");
//
//        final Literal comment = f.createLiteral("Bob has a comment");
//
//        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//        operation.setBinding("x", bob);
//        operation.setBinding("z", comment);
//
//        assertFalse(con.hasStatement(null, RDFS.COMMENT, comment, true));
//
//        operation.execute();
//
//        assertTrue(con.hasStatement(bob, RDFS.COMMENT, comment, true));
//        assertFalse(con.hasStatement(alice, RDFS.COMMENT, comment, true));
//
//    }

    /**
     * @since openrdf 2.6.3
     */
//      @Test
    public void testInsertWhereWithOptional()
        throws Exception
    {
        log.debug("executing testInsertWhereWithOptional");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append(" INSERT { ?s ex:age ?incAge } ");
        // update.append(" DELETE { ?s ex:age ?age } ");
        update.append(" WHERE { ?s foaf:name ?name . ");
        update.append(" OPTIONAL {?s ex:age ?age . BIND ((?age + 1) as ?incAge)  } ");
        update.append(" } ");

        final URI age = f.createURI(EX_NS, "age");

        assertFalse(hasStatement(alice, age, null, true));
        assertTrue(hasStatement(bob, age, null, true));

        m_repo.prepareUpdate(update.toString()).evaluate();

//        RepositoryResult<Statement> result = m_repo.getStatements(bob, age, null, true);
//
//        while (result.hasNext()) {
//            final Statement stmt = result.next();
//            if (log.isInfoEnabled())
//                log.info(stmt.toString());
//        }

        assertTrue(hasStatement(bob, age, f.createLiteral("43", XMLSchema.INTEGER), true));

//        result = m_repo.getStatements(alice, age, null, true);
//
//        while (result.hasNext()) {
//            final Statement stmt = result.next();
//            if (log.isInfoEnabled())
//                log.info(stmt.toString());
//        }
        assertFalse(hasStatement(alice, age, null, true));
    }

    ////    //@Test
    public void testDeleteInsertWhere()
        throws Exception
    {
//        log.debug("executing test DeleteInsertWhere");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE { ?x foaf:name ?y } INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

        assertFalse(hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertFalse(hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertTrue(hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

        assertFalse(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertFalse(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

    }

//    /** 
//  * TODO Requires BINDINGS support for {@link RemoteRepository}
//  *
//       * @since OPENRDF 2.6.6. */
////  @Test
//  public void testDeleteInsertWhereWithBindings2()
//      throws Exception
//  {
//      if (!BINDINGS)
//          return;
//      log.debug("executing test testDeleteInsertWhereWithBindings2");
//      final StringBuilder update = new StringBuilder();
//      update.append(getNamespaceDeclarations());
//      update.append("DELETE { ?x foaf:name ?y } INSERT {?x rdfs:label ?z . } WHERE {?x foaf:name ?y }");
//
//      final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//      operation.setBinding("z", f.createLiteral("person"));
//      
//      assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
//      assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
//
//      operation.execute();
//
//      assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("person"), true));
//      assertTrue(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("person"), true));
//
//      assertFalse(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
//      assertFalse(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
//  }
  
  /** @since openrdf 2.6.3 */
  public void testDeleteInsertWhereLoopingBehavior() throws Exception {
      log.debug("executing test testDeleteInsertWhereLoopingBehavior");
      final StringBuilder update = new StringBuilder();
      update.append(getNamespaceDeclarations());
      update.append(" DELETE { ?x ex:age ?y } INSERT {?x ex:age ?z }");
      update.append(" WHERE { ");
      update.append("   ?x ex:age ?y .");
      update.append("   BIND((?y + 1) as ?z) ");
      update.append("   FILTER( ?y < 46 ) ");
      update.append(" } ");

      final URI age = f.createURI(EX_NS, "age");
      final Literal originalAgeValue = f.createLiteral("42", XMLSchema.INTEGER);
      final Literal correctAgeValue = f.createLiteral("43", XMLSchema.INTEGER);
      final Literal inCorrectAgeValue = f.createLiteral("46", XMLSchema.INTEGER);

      assertTrue(hasStatement(bob, age, originalAgeValue, true));

      m_repo.prepareUpdate(update.toString()).evaluate();

      assertFalse(hasStatement(bob, age, originalAgeValue, true));
      assertTrue(hasStatement(bob, age, correctAgeValue, true));
      assertFalse(hasStatement(bob, age, inCorrectAgeValue, true));
  }

  //@Test
    public void testInsertTransformedWhere()
        throws Exception
    {
//        log.debug("executing test InsertTransformedWhere");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT {?x rdfs:label [] . } WHERE {?y ex:containsPerson ?x.  }");

        assertFalse(hasStatement(bob, RDFS.LABEL, null, true));
        assertFalse(hasStatement(alice, RDFS.LABEL, null, true));

        m_repo.prepareUpdate(update.toString()).evaluate();

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
//        log.debug("executing testInsertWhereGraph");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT {GRAPH ?g {?x rdfs:label ?y . }} WHERE {GRAPH ?g {?x foaf:name ?y }}");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        
        m_repo.prepareUpdate(update.toString()).evaluate();

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

//        log.debug("executing testInsertWhereUsing");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT {?x rdfs:label ?y . } USING ex:graph1 WHERE {?x foaf:name ?y }");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();

        m_repo.prepareUpdate(update.toString()).evaluate();

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
//        log.debug("executing testInsertWhereWith");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("WITH ex:graph1 INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        
        m_repo.prepareUpdate(update.toString()).evaluate();

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
//        log.debug("executing testDeleteWhereShortcut");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE WHERE {?x foaf:name ?y }");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertTrue(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

        m_repo.prepareUpdate(update.toString()).evaluate();

        String msg = "foaf:name properties should have been deleted";
        assertFalse(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertFalse(msg, hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

        msg = "foaf:knows properties should not have been deleted";
        assertTrue(msg, hasStatement(bob, FOAF.KNOWS, null, true));
        assertTrue(msg, hasStatement(alice, FOAF.KNOWS, null, true));
    }

    /**
     * <pre>
     * DELETE WHERE {GRAPH ?g {?x foaf:name ?y} }
     * </pre>
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/568 (DELETE WHERE
     *      fails with Java AssertionError)
     */
    //@Test
    public void testDeleteWhereShortcut2()
        throws Exception
    {
        
//        log.debug("executing testDeleteWhereShortcut2");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE WHERE { GRAPH ?g {?x foaf:name ?y } }");

        assertTrue(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertTrue(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

        m_repo.prepareUpdate(update.toString()).evaluate();

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
//        log.debug("executing testDeleteWhere");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE {?x foaf:name ?y } WHERE {?x foaf:name ?y }");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertTrue(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        String msg = "foaf:name properties should have been deleted";
        assertFalse(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertFalse(msg, hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

    }

//    /**
//     * Note: blank nodes are not permitted in the DELETE clause template.
//     * 
//     * <a href="https://sourceforge.net/apps/trac/bigdata/ticket/571">
//     * DELETE/INSERT WHERE handling of blank nodes </a>
//     */
//    //@Test
//    public void testDeleteTransformedWhere()
//        throws Exception
//    {
////        log.debug("executing testDeleteTransformedWhere");
//
//        final StringBuilder update = new StringBuilder();
//        update.append(getNamespaceDeclarations());
//        update.append("DELETE {?y foaf:name [] } WHERE {?x ex:containsPerson ?y }");
//
////        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        assertTrue(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
//        assertTrue(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
//
////        operation.execute();
//        m_repo.prepareUpdate(update.toString()).evaluate();
//
//        String msg = "foaf:name properties should have been deleted";
//        assertFalse(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
//        assertFalse(msg, hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
//
//        msg = "ex:containsPerson properties should not have been deleted";
//        assertTrue(msg, hasStatement(graph1, f.createURI(EX_NS, "containsPerson"), bob, true));
//        assertTrue(msg, hasStatement(graph2, f.createURI(EX_NS, "containsPerson"), alice, true));
//
//    }

    //@Test
    public void testInsertData()
        throws Exception
    {
//        log.debug("executing testInsertData");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT DATA { ex:book1 dc:title \"book 1\" ; dc:creator \"Ringo\" . } ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        URI book1 = f.createURI(EX_NS, "book1");

        assertFalse(hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true));
        assertFalse(hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true));

//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        String msg = "two new statements about ex:book1 should have been inserted";
        assertTrue(msg, hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true));
        assertTrue(msg, hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true));
    }

    //@Test
    public void testInsertDataMultiplePatterns()
        throws Exception
    {
//        log.debug("executing testInsertData");

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
        m_repo.prepareUpdate(update.toString()).evaluate();

        String msg = "newly inserted statement missing";
        assertTrue(msg, hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true));
        assertTrue(msg, hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true));
        assertTrue(msg, hasStatement(book2, DC.CREATOR, f.createLiteral("George"), true));
    }

    //@Test
    public void testInsertDataInGraph()
        throws Exception
    {
//        log.debug("executing testInsertDataInGraph");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT DATA { GRAPH ex:graph1 { ex:book1 dc:title \"book 1\" ; dc:creator \"Ringo\" . } } ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        URI book1 = f.createURI(EX_NS, "book1");

        assertFalse(hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true, graph1));
        assertFalse(hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true, graph1));

        m_repo.prepareUpdate(update.toString()).evaluate();
//        operation.execute();

        String msg = "two new statements about ex:book1 should have been inserted in graph1";
        assertTrue(msg, hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true, graph1));
        assertTrue(msg, hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true, graph1));
    }

    /** @since OPENRDF 2.6.3 */
//  @Test
    public void testInsertDataInGraph2()
      throws Exception
    {
      log.debug("executing testInsertDataInGraph2");

      final StringBuilder update = new StringBuilder();
      update.append(getNamespaceDeclarations());
      update.append("INSERT DATA { GRAPH ex:graph1 { ex:Human rdfs:subClassOf ex:Mammal. ex:Mammal rdfs:subClassOf ex:Animal. ex:george a ex:Human. ex:ringo a ex:Human. } } ");

      final URI human = f.createURI(EX_NS, "Human");
      final URI mammal = f.createURI(EX_NS, "Mammal");
      final URI george = f.createURI(EX_NS, "george");

      m_repo.prepareUpdate(update.toString()).evaluate();

      assertTrue(hasStatement(human, RDFS.SUBCLASSOF, mammal, true, graph1));
      assertTrue(hasStatement(mammal, RDFS.SUBCLASSOF, null, true, graph1));
      assertTrue(hasStatement(george, RDF.TYPE, human, true, graph1));
    }

    //@Test
    public void testDeleteData()
        throws Exception
    {
//        log.debug("executing testDeleteData");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE DATA { ex:alice foaf:knows ex:bob. } ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(alice, FOAF.KNOWS, bob, true));
        
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        String msg = "statement should have been deleted.";
        assertFalse(msg, hasStatement(alice, FOAF.KNOWS, bob, true));
    }

    //@Test
    public void testDeleteDataMultiplePatterns()
        throws Exception
    {
//        log.debug("executing testDeleteData");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE DATA { ex:alice foaf:knows ex:bob. ex:alice foaf:mbox \"alice@example.org\" .} ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(alice, FOAF.KNOWS, bob, true));
        assertTrue(hasStatement(alice, FOAF.MBOX, f.createLiteral("alice@example.org"), true));
    
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        String msg = "statement should have been deleted.";
        assertFalse(msg, hasStatement(alice, FOAF.KNOWS, bob, true));
        assertFalse(msg, hasStatement(alice, FOAF.MBOX, f.createLiteral("alice@example.org"), true));
    }

    //@Test
    public void testDeleteDataFromGraph()
        throws Exception
    {
//        log.debug("executing testDeleteDataFromGraph");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE DATA { GRAPH ex:graph1 {ex:alice foaf:knows ex:bob. } } ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(alice, FOAF.KNOWS, bob, true, graph1));
    
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        String msg = "statement should have been deleted from graph1";
        assertFalse(msg, hasStatement(alice, FOAF.KNOWS, bob, true, graph1));
    }

    //@Test
    public void testDeleteDataFromWrongGraph()
        throws Exception
    {
//        log.debug("executing testDeleteDataFromWrongGraph");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());

        // statement does not exist in graph2.
        update.append("DELETE DATA { GRAPH ex:graph2 {ex:alice foaf:knows ex:bob. } } ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(alice, FOAF.KNOWS, bob, true, graph1));
        assertFalse(hasStatement(alice, FOAF.KNOWS, bob, true, graph2));
        
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        String msg = "statement should have not have been deleted from graph1";
        assertTrue(msg, hasStatement(alice, FOAF.KNOWS, bob, true, graph1));
    }

    //@Test
    public void testCreateNewGraph()
        throws Exception
    {
//        log.debug("executing testCreateNewGraph");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());

        URI newGraph = f.createURI(EX_NS, "new-graph");

        update.append("CREATE GRAPH <" + newGraph + "> ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(null, null, null, false, graph1));
        assertTrue(hasStatement(null, null, null, false, graph2));
        assertFalse(hasStatement(null, null, null, false, newGraph));
        assertTrue(hasStatement(null, null, null, false));
    }

    //@Test
    public void testCreateExistingGraph()
        throws Exception
    {
//        log.debug("executing testCreateExistingGraph");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("CREATE GRAPH <" + graph1 + "> ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        try {
            m_repo.prepareUpdate(update.toString()).evaluate();
//            operation.execute();

            fail("creation of existing graph should have resulted in error.");
        }
        catch (Exception e) {
            // expected behavior
//            con.rollback();
            if(log.isInfoEnabled()) log.info("Expected exception: " + e, e);
        }
    }

    //@Test
    public void testCopyToDefault()
        throws Exception
    {
//        log.debug("executing testCopyToDefault");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("COPY GRAPH <" + graph1.stringValue() + "> TO DEFAULT");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
      
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();
        
        assertFalse(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertFalse(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, (Resource)null));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph1));
    }

    //@Test
    public void testCopyToExistingNamed()
        throws Exception
    {
//        log.debug("executing testCopyToExistingNamed");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("COPY GRAPH ex:graph1 TO ex:graph2");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph2));
        assertFalse(hasStatement(alice, FOAF.NAME, null, false, graph2));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph1));
    }

    //@Test
    public void testCopyToNewNamed()
        throws Exception
    {
//        log.debug("executing testCopyToNewNamed");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("COPY GRAPH ex:graph1 TO ex:graph3");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, FOAF.NAME, null, false, f.createURI(EX_NS, "graph3")));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph1));
    }

    //@Test
    public void testCopyFromDefault()
        throws Exception
    {
//        log.debug("executing testCopyFromDefault");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("COPY DEFAULT TO ex:graph3");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
    
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));

    }

    //@Test
    public void testCopyFromDefaultToDefault()
        throws Exception
    {
//        log.debug("executing testCopyFromDefaultToDefault");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("COPY DEFAULT TO DEFAULT");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
    
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
    }

    //@Test
    public void testAddToDefault()
        throws Exception
    {
//        log.debug("executing testAddToDefault");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("ADD GRAPH <" + graph1.stringValue() + "> TO DEFAULT");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, (Resource)null));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph1));
    }

    //@Test
    public void testAddToExistingNamed()
        throws Exception
    {
//        log.debug("executing testAddToExistingNamed");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("ADD GRAPH ex:graph1 TO ex:graph2");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph2));
        assertTrue(hasStatement(alice, FOAF.NAME, null, false, graph2));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph1));
    }

    //@Test
    public void testAddToNewNamed()
        throws Exception
    {
//        log.debug("executing testAddToNewNamed");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("ADD GRAPH ex:graph1 TO ex:graph3");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, FOAF.NAME, null, false, f.createURI(EX_NS, "graph3")));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, graph1));
    }

    //@Test
    public void testAddFromDefault()
        throws Exception
    {
//        log.debug("executing testAddFromDefault");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("ADD DEFAULT TO ex:graph3");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));

    }

    //@Test
    public void testAddFromDefaultToDefault()
        throws Exception
    {
//        log.debug("executing testAddFromDefaultToDefault");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("ADD DEFAULT TO DEFAULT");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
    }

    //@Test
    public void testMoveToDefault()
        throws Exception
    {
//        log.debug("executing testMoveToDefault");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("MOVE GRAPH <" + graph1.stringValue() + "> TO DEFAULT");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertFalse(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(bob, FOAF.NAME, null, false, (Resource)null));
        assertFalse(hasStatement(null, null, null, false, graph1));
    }

    //@Test
    public void testMoveToNewNamed()
        throws Exception
    {
//        log.debug("executing testMoveToNewNamed");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("MOVE GRAPH ex:graph1 TO ex:graph3");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(bob, FOAF.NAME, null, false, f.createURI(EX_NS, "graph3")));
        assertFalse(hasStatement(null, null, null, false, graph1));
    }

    //@Test
    public void testMoveFromDefault()
        throws Exception
    {
//        log.debug("executing testMoveFromDefault");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("MOVE DEFAULT TO ex:graph3");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertFalse(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));

    }

    //@Test
    public void testMoveFromDefaultToDefault()
        throws Exception
    {
//        log.debug("executing testMoveFromDefaultToDefault");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("MOVE DEFAULT TO DEFAULT");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
    }

    //@Test
    public void testClearAll()
        throws Exception
    {
//        log.debug("executing testClearAll");
        String update = "CLEAR ALL";

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);
//
//        operation.execute();
        
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(null, null, null, false));

    }

    //@Test
    public void testClearGraph()
        throws Exception
    {
//        log.debug("executing testClearGraph");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("CLEAR GRAPH <" + graph1.stringValue() + "> ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(null, null, null, false, graph1));
        assertTrue(hasStatement(null, null, null, false, graph2));
        assertTrue(hasStatement(null, null, null, false));
    }

    //@Test
    public void testClearNamed()
        throws Exception
    {
//        log.debug("executing testClearNamed");
        String update = "CLEAR NAMED";

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);
//
//        operation.execute();
        
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(null, null, null, false, graph1));
        assertFalse(hasStatement(null, null, null, false, graph2));
        assertTrue(hasStatement(null, null, null, false));

    }

    //@Test
    public void testClearDefault()
        throws Exception
    {
//        log.debug("executing testClearDefault");

       String update = "CLEAR DEFAULT";

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

        // Verify statements exist in the named graphs.
        assertTrue(hasStatement(null, null, null, false, graph1));
        assertTrue(hasStatement(null, null, null, false, graph2));
        // Verify statements exist in the default graph.
        assertTrue(hasStatement(null, null, null, false, new Resource[]{null}));

//        System.err.println(dumpStore());
        
//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

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
//        log.debug("executing testDropAll");
        String update = "DROP ALL";

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);
//
//        operation.execute();
        
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(null, null, null, false));

    }

    //@Test
    public void testDropGraph()
        throws Exception
    {
//        log.debug("executing testDropGraph");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DROP GRAPH <" + graph1.stringValue() + "> ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        operation.execute();
        
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(null, null, null, false, graph1));
        assertTrue(hasStatement(null, null, null, false, graph2));
        assertTrue(hasStatement(null, null, null, false));
    }

    //@Test
    public void testDropNamed()
        throws Exception
    {
//        log.debug("executing testDropNamed");

        String update = "DROP NAMED";

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);
//
//        operation.execute();
        
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertFalse(hasStatement(null, null, null, false, graph1));
        assertFalse(hasStatement(null, null, null, false, graph2));
        assertTrue(hasStatement(null, null, null, false));
    }

    //@Test
    public void testDropDefault()
        throws Exception
    {
//        log.debug("executing testDropDefault");

        String update = "DROP DEFAULT";

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

        // Verify statements exist in the named graphs.
        assertTrue(hasStatement(null, null, null, false, graph1));
        assertTrue(hasStatement(null, null, null, false, graph2));
        // Verify statements exist in the default graph.
        assertTrue(hasStatement(null, null, null, false, new Resource[]{null}));

//        operation.execute();
        
        m_repo.prepareUpdate(update.toString()).evaluate();

        assertTrue(hasStatement(null, null, null, false, graph1));
        assertTrue(hasStatement(null, null, null, false, graph2));
        // Verify that no statements remain in the 'default' graph.
        assertFalse(hasStatement(null, null, null, false, new Resource[]{null}));

    }

//  /**
//  * Note: blank nodes are not permitted in the DELETE clause template.
//  * 
//  * <a href="https://sourceforge.net/apps/trac/bigdata/ticket/571">
//  * DELETE/INSERT WHERE handling of blank nodes </a>
//  */
//    //@Test
//    public void testUpdateSequenceDeleteInsert()
//        throws Exception
//    {
////        log.debug("executing testUpdateSequenceDeleteInsert");
//
//        final StringBuilder update = new StringBuilder();
//        update.append(getNamespaceDeclarations());
//        update.append("DELETE {?y foaf:name [] } WHERE {?x ex:containsPerson ?y }; ");
//        update.append(getNamespaceDeclarations());
//        update.append("INSERT {?x foaf:name \"foo\" } WHERE {?y ex:containsPerson ?x} ");
//
////        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        assertTrue(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
//        assertTrue(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
//
////        operation.execute();
//
//        m_repo.prepareUpdate(update.toString()).evaluate();
//        
//        String msg = "foaf:name properties should have been deleted";
//        assertFalse(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
//        assertFalse(msg, hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
//
//        msg = "foaf:name properties with value 'foo' should have been added";
//        assertTrue(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("foo"), true));
//        assertTrue(msg, hasStatement(alice, FOAF.NAME, f.createLiteral("foo"), true));
//    }

//  /**
//  * Note: blank nodes are not permitted in the DELETE clause template.
//  * 
//  * <a href="https://sourceforge.net/apps/trac/bigdata/ticket/571">
//  * DELETE/INSERT WHERE handling of blank nodes </a>
//  */
//    //@Test
//    public void testUpdateSequenceInsertDelete()
//        throws Exception
//    {
////        log.debug("executing testUpdateSequenceInsertDelete");
//
//        final StringBuilder update = new StringBuilder();
//        update.append(getNamespaceDeclarations());
//        update.append("INSERT {?x foaf:name \"foo\" } WHERE {?y ex:containsPerson ?x}; ");
//        update.append(getNamespaceDeclarations());
//        update.append("DELETE {?y foaf:name [] } WHERE {?x ex:containsPerson ?y } ");
//
////        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//        assertTrue(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
//        assertTrue(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
//
////        operation.execute();
//        m_repo.prepareUpdate(update.toString()).evaluate();
//
//        String msg = "foaf:name properties should have been deleted";
//        assertFalse(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
//        assertFalse(msg, hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
//
//        msg = "foaf:name properties with value 'foo' should not have been added";
//        assertFalse(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("foo"), true));
//        assertFalse(msg, hasStatement(alice, FOAF.NAME, f.createLiteral("foo"), true));
//    }

    //@Test
    public void testUpdateSequenceInsertDelete2()
        throws Exception
    {
//        log.debug("executing testUpdateSequenceInsertDelete2");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT { GRAPH ex:graph2 { ?s ?p ?o } } WHERE { GRAPH ex:graph1 { ?s ?p ?o . FILTER (?s = ex:bob) } }; ");
        update.append("WITH ex:graph1 DELETE { ?s ?p ?o } WHERE {?s ?p ?o . FILTER (?s = ex:bob) } ");

//        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertTrue(hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true, graph1));
        assertTrue(hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true, graph2));

//        operation.execute();
        m_repo.prepareUpdate(update.toString()).evaluate();

        String msg = "statements about bob should have been removed from graph1";
        assertFalse(msg, hasStatement(bob, null, null, true, graph1));

        msg = "statements about bob should have been added to graph2";
        assertTrue(msg, hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true, graph2));
        assertTrue(msg, hasStatement(bob, FOAF.MBOX, null, true, graph2));
        assertTrue(msg, hasStatement(bob, FOAF.KNOWS, alice, true, graph2));
    }

    //@Test
    public void testUpdateSequenceInsertDeleteExample9()
        throws Exception
    {
//        log.debug("executing testUpdateSequenceInsertDeleteExample9");

        // replace the standard dataset with one specific to this case.
        m_repo.prepareUpdate("DROP ALL").evaluate();
        // Note: local copy of: /testdata-update/dataset-update-example9.trig
        m_repo.prepareUpdate(
                "LOAD <file:src/test/java/com/bigdata/rdf/sail/webapp/dataset-update-example9.trig>")
                .evaluate();

        final URI book1 = f.createURI("http://example/book1");
//        URI book3 = f.createURI("http://example/book3");
        final URI bookStore = f.createURI("http://example/bookStore");
        final URI bookStore2 = f.createURI("http://example/bookStore2");
        
        final StringBuilder update = new StringBuilder();
        update.append("prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ");
        update.append("prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>  ");
        update.append("prefix xsd: <http://www.w3.org/2001/XMLSchema#>  ");
        update.append("prefix dc: <http://purl.org/dc/elements/1.1/>  ");
        update.append("prefix dcmitype: <http://purl.org/dc/dcmitype/>  ");
        update.append("INSERT  { GRAPH <http://example/bookStore2> { ?book ?p ?v } } ");
        update.append(" WHERE ");
        update.append(" { GRAPH  <http://example/bookStore> ");
        update.append("   { ?book dc:date ?date . ");
        update.append("       FILTER ( ?date < \"2000-01-01T00:00:00-02:00\"^^xsd:dateTime ) ");
        update.append("       ?book ?p ?v ");
        update.append("      } ");
        update.append(" } ;");
        update.append("WITH <http://example/bookStore> ");
        update.append(" DELETE { ?book ?p ?v } ");
        update.append(" WHERE ");
        update.append(" { ?book dc:date ?date ; ");
        update.append("         a dcmitype:PhysicalObject .");
        update.append("    FILTER ( ?date < \"2000-01-01T00:00:00-02:00\"^^xsd:dateTime ) ");
        update.append("   ?book ?p ?v");
        update.append(" } ");

        m_repo.prepareUpdate(update.toString()).evaluate();

        String msg = "statements about book1 should have been removed from bookStore";
        assertFalse(msg, hasStatement(book1, null, null, true, bookStore));

        msg = "statements about book1 should have been added to bookStore2";
        assertTrue(msg, hasStatement(book1, RDF.TYPE, null, true, bookStore2));
        assertTrue(msg, hasStatement(book1, DC.DATE, null, true, bookStore2));
        assertTrue(msg, hasStatement(book1, DC.TITLE, null, true, bookStore2));
    }

    /**
     * Unit test for
     * 
     * <pre>
     * DROP ALL;
     * INSERT DATA {
     * GRAPH <http://example.org/one> {
     * <a> <b> <c> .
     * <d> <e> <f> .
     * }};
     * ADD SILENT GRAPH <http://example.org/one> TO GRAPH <http://example.org/two> ;
     * DROP SILENT GRAPH <http://example.org/one>  ;
     * </pre>
     * 
     * The IV cache was not not being propagated correctly with the result that
     * we were seeing mock IVs for "one" and "two". The UPDATE would work
     * correctly the 2nd time since the URIs had been entered into the
     * dictionary by then.
     * 
     * @throws Exception 
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/567" >
     *      Failure to set cached value on IV results in incorrect behavior for
     *      complex UPDATE operation </a>
     */
    public void testTicket567() throws Exception {

        // replace the standard dataset with one specific to this case.
//        con.clear();
//        con.commit();
        m_repo.prepareUpdate("DROP ALL").evaluate();

        final StringBuilder update = new StringBuilder();
        update.append("DROP ALL;\n");
        update.append("INSERT DATA {\n");
        update.append(" GRAPH <http://example.org/one> {\n");
        update.append("   <http://example.org/a> <http://example.org/b> <http://example.org/c> .\n");
        update.append("   <http://example.org/d> <http://example.org/e> <http://example.org/f> .\n");
        update.append("}};\n");
        update.append("ADD SILENT GRAPH <http://example.org/one> TO GRAPH <http://example.org/two> ;\n");
        update.append("DROP SILENT GRAPH <http://example.org/one>  ;\n");
        
        m_repo.prepareUpdate(update.toString()).evaluate();

        final URI one = f.createURI("http://example.org/one");
        final URI two = f.createURI("http://example.org/two");

        String msg = "Nothing in graph <one>";
        assertFalse(msg, hasStatement(null, null, null, true, one));

        msg = "statements are in graph <two>";
        assertTrue(msg, hasStatement(null, null, null, true, two));
        
    }
    

    /**
     * This test is based on a forum post. This post provided an example of an
     * issue with Unicode case-folding in the REGEX operator and a means to
     * encode the Unicode characters to avoid doubt about which characters were
     * transmitted and receieved.
     * 
     * @throws Exception 
     * 
     * @see <a href=
     *      "https://sourceforge.net/projects/bigdata/forums/forum/676946/topic/7073971"
     *      >Forum post on the REGEX Unicode case-folding issue</a>
     *      
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/655">
     *      SPARQL REGEX operator does not perform case-folding correctly for
     *      Unicode data</a>
     */
    public void testUnicodeCleanAndRegex() throws Exception {

        /*
         * If I work around this problem by switching the unencoded Unicode
         * characters into SPARQL escape sequences like this:
         */

        // Insert statement:
        final String updateStr = "PREFIX ns: <http://example.org/ns#>\n"
                + "INSERT DATA { GRAPH ns:graph { ns:auml ns:label \"\u00C4\", \"\u00E4\" } }\n";

        m_repo.prepareUpdate(updateStr).evaluate();

        // Test query:
        final String queryStr = "PREFIX ns: <http://example.org/ns#>\n"
                + "SELECT * { GRAPH ns:graph { ?s ?p ?o FILTER(regex(?o, \"\u00E4\", \"i\")) } }";

        final IPreparedTupleQuery query = m_repo.prepareTupleQuery(queryStr);
        
        assertEquals(2L, countResults(query.evaluate()));

        /*
         * Then I still get only one result for the query, the triple with ''
         * which is \u00E4. But if I now add the 'u' flag to the regex, I get
         * both triples as result, so this seems to be a viable workaround.
         * Always setting the UNICODE_CASE flag sounds like a good idea, and in
         * fact, Jena ARQ seems to do that when given the 'i' flag:
         */
        
    }

    public void testLoad()
            throws Exception
        {
    	final URL url = this.getClass().getClassLoader().getResource("com/bigdata/rdf/rio/small.rdf");

        final String update = "LOAD <" + url.toExternalForm() + ">";
        
        final String ns = "http://bigdata.com/test/data#";
        
        m_repo.prepareUpdate(update).evaluate();
        
        assertTrue(hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true));

    }

    //@Test
    public void testLoadSilent()
        throws Exception
    {
        final String update = "LOAD SILENT <file:src/test/com/bigdata/rdf/rio/NOT-FOUND.rdf>";
        
        final String ns = "http://bigdata.com/test/data#";
        
        m_repo.prepareUpdate(update).evaluate();

        assertFalse(hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true));

    }

    //@Test
    public void testLoadIntoGraph()
        throws Exception
    {
    	final URL url = this.getClass().getClassLoader().getResource("com/bigdata/rdf/rio/small.rdf");


        final URI g1 = f.createURI("http://www.bigdata.com/g1");
        final URI g2 = f.createURI("http://www.bigdata.com/g2");

        final String update = "LOAD <" + url.toExternalForm() + "> "
                + "INTO GRAPH <" + g1.stringValue() + ">";
        
        final String ns = "http://bigdata.com/test/data#";
        
        m_repo.prepareUpdate(update).evaluate();

        assertFalse(hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true, g2));

        assertTrue(hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true, g1));

    }

    /**
     * Verify ability to load data from a gzip resource.
     */
    public void testLoadGZip()
            throws Exception
        {
    
    	final URL url = this.getClass().getClassLoader().getResource("com/bigdata/rdf/rio/small.rdf.gz");
    	
        final String update = "LOAD <" + url.toExternalForm() + ">";
        
        final String ns = "http://bigdata.com/test/data#";
        
        m_repo.prepareUpdate(update).evaluate();
        
        assertTrue(hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true));

    }

//    /**
//     * Verify ability to load data from a gzip resource.
//     */
//    public void testLoadZip()
//            throws Exception
//        {
//        final String update = "LOAD <file:src/test/com/bigdata/rdf/rio/small.rdf.zip>";
//        
//        final String ns = "http://bigdata.com/test/data#";
//        
//        m_repo.prepareUpdate(update).evaluate();
//        
//        assertTrue(hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
//                f.createLiteral("Michael Personick"), true));
//
//    }

//    //@Test
//    public void testUpdateSequenceInsertDeleteExample9()
//        throws Exception
//    {
//        log.debug("executing testUpdateSequenceInsertDeleteExample9");
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
    
    public void testReallyLongQueryString()
            throws Exception
        {
    		final Literal l = getReallyLongLiteral(1000);
    	
            log.debug("executing test testInsertEmptyWhere");
            final StringBuilder update = new StringBuilder();
            update.append(getNamespaceDeclarations());
            update.append("INSERT { <" + bob + "> rdfs:label " + l + " . } WHERE { }");

            assertFalse(hasStatement(bob, RDFS.LABEL, l, true));

            m_repo.prepareUpdate(update.toString()).evaluate();

            assertTrue(hasStatement(bob, RDFS.LABEL, l, true));
        }

    private Literal getReallyLongLiteral(final int length) {
    	
    	final StringBuilder sb = new StringBuilder();
    	for (int i = 0; i < length; i++) {
    		sb.append('a');
    	}
    	return new LiteralImpl(sb.toString());
    	
    }

    /**
     * Used for reporting of the last operation issued.
     */
    private enum StressTestOpEnum {
		Update,
		DropAll,
		LoadFile
	};
	/**
	 * A stress test written to look for stochastic behaviors in SPARQL UPDATE
	 * for GROUP COMMIT.
	 */
    public void testStressInsertWhereGraph() throws Exception {

		final int LIMIT = 10;
		int i = 0;
		StressTestOpEnum lastOp = null;
		try {
			for (i = 0; i < LIMIT; i++) {

				lastOp = StressTestOpEnum.Update;
				testInsertWhereGraph();
				
				lastOp = StressTestOpEnum.DropAll;
				testDropAll();
				
				lastOp = StressTestOpEnum.LoadFile;
				doLoadFile();
			}
		} catch (Throwable t) {
			throw new RuntimeException("Iteration " + (i + 1) + " of " + LIMIT
					+ ", lastOp=" + lastOp, t);
		}

	}

}
