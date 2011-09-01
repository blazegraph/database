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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.UUID;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataStatementImpl;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Test suite for {@link ASTConstructIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTConstructIterator extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTConstructIterator() {
    }

    /**
     * @param name
     */
    public TestASTConstructIterator(String name) {
        super(name);
    }

    /**
     * Unit test for the iterator which interprets the CONSTUCT clause,
     * generating triples. 
     * 
     * <pre>
     * CONSTRUCT { <http://www.bigdata.com/DC> rdfs:label "DC .
     *             ?x rdf:type foaf:Person .
     *             }
     * where {
     *   ?x rdf:type foaf:Person
     * }
     * </pre>
     * 
     * This test verifies that the correct CONSTRUCT clause is generated and
     * that a WHERE clause is added which will capture the necessary bindings to
     * support that CONSTUCT while preserving the original WHERE clause.
     * 
     * @throws Exception 
     */
    public void test_construct_iterator() throws Exception {

        final BigdataValueFactory f = store.getValueFactory();

        // Add some data.
        {

            final BigdataStatement[] stmts = new BigdataStatement[] {//

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Mike"),
                            f.createURI(RDF.TYPE.toString()),
                            f.createURI(FOAFVocabularyDecl.Person.toString()),
                            null, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Bryan"),
                            f.createURI(RDF.TYPE.toString()),
                            f.createURI(FOAFVocabularyDecl.Person.toString()),
                            null, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Mike"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("Mike"), null, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Bryan"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("Bryan"), null, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/DC"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("DC"), null, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

            };

            final StatementBuffer<BigdataStatement> buf = new StatementBuffer<BigdataStatement>(
                    store, 10/* capacity */);

            for (BigdataStatement stmt : stmts) {

                buf.add(stmt);

            }

            // write on the database.
            buf.flush();

        }

        final BigdataURI rdfType = f.createURI(RDF.TYPE.toString());

        final BigdataURI rdfsLabel = f.createURI(RDFS.LABEL.toString());

        final BigdataURI foafPerson = f.createURI(FOAFVocabularyDecl.Person
                .toString());

        final BigdataURI mikeURI = f.createURI("http://www.bigdata.com/Mike");

        final BigdataURI bryanURI = f.createURI("http://www.bigdata.com/Bryan");

        final BigdataLiteral mikeLabel = f.createLiteral("Mike");

        final BigdataLiteral bryanLabel = f.createLiteral("Bryan");

        final BigdataURI dcURI = f.createURI("http://www.bigdata.com/DC");

        final BigdataValue[] values = new BigdataValue[] { rdfType, rdfsLabel,
                foafPerson, mikeURI, bryanURI, mikeLabel, bryanLabel, dcURI };

        // resolve IVs.
        store.getLexiconRelation()
                .addTerms(values, values.length, true/* readOnly */);

        final IVariable x = Var.var("x");

        final IBindingSet[] expected = new IBindingSet[] {

                new ListBindingSet(new IVariable[] { x },
                        new IConstant[] { new Constant(mikeURI.getIV()) }),

                new ListBindingSet(new IVariable[] { x },
                        new IConstant[] { new Constant(bryanURI.getIV()) }),

        };

        final String queryStr = ""
                + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"
                + "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n"
                + "CONSTRUCT { <http://www.bigdata.com/DC> rdfs:label \"DC\" . \n"
                + "             ?x rdf:type foaf:Person . \n"
                + "             } \n" + " where { \n"
                + "   ?x rdf:type foaf:Person \n" + " } \n";

        // parse query
        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, null/* baseURI */);

        // context for evaluation.
        final AST2BOpContext context = new AST2BOpContext(queryRoot, store);

        final PipelineOp queryPlan = AST2BOpUtility.convert(context);

        // Submit query for evaluation.
        final IBindingSet[][] existingBindings = new IBindingSet[][] { new IBindingSet[] { new ListBindingSet() } };

        final QueryEngine queryEngine = QueryEngineFactory
                .getQueryController(store.getIndexManager());

        final AbstractRunningQuery runningQuery = queryEngine.eval(
                UUID.randomUUID(), queryPlan,
                new ThickAsynchronousIterator<IBindingSet[]>(existingBindings));

        // final Iterator<IBindingSet[]> iter = runningQuery.iterator();

        assertSameSolutionsAnyOrder(expected, runningQuery);

        // new ASTConstructIterator(context, queryRoot, new dechunkitr);

        /*
         * FIXME This sets up and verifies the example. It needs to go one step
         * further and wrap the running query appropriately to have materialized
         * values and then wrap that one more time to do the CONSTRUCT on those
         * values. It should then verify that the same triples are constructed.
         */
        fail("finish test");

    }

}
