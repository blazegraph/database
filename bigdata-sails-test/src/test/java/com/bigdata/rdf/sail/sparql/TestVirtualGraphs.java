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
 * Created on Mar 2, 2012
 */

package com.bigdata.rdf.sail.sparql;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.openrdf.model.vocabulary.DC;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.impl.DatasetImpl;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sail.sparql.ast.ParseException;
import com.bigdata.rdf.sail.sparql.ast.TokenMgrError;
import com.bigdata.rdf.sparql.AbstractBigdataExprBuilderTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

/**
 * Test suite for the <code>VIRTUAL GRAPHS</code> SPARQL extension.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestVirtualGraphs extends AbstractBigdataExprBuilderTestCase {

    public TestVirtualGraphs() {
    }

    public TestVirtualGraphs(String name) {
        super(name);
    }
    
    /**
     * Overridden to setup a quads mode instance.
     */
    @Override
    protected Properties getProperties() {

        final Properties properties = new Properties(super.getProperties());

        properties.setProperty(AbstractTripleStore.Options.QUADS_MODE, "true");
        
        return properties;
    
    }

    /**
     * <pre>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * SELECT ?who ?g ?mbox
     * FROM <http://example.org/dft.ttl>
     * FROM NAMED VIRTUAL GRAPH :vg
     * WHERE
     * {
     *    ?g dc:publisher ?who .
     *    GRAPH ?g { ?x foaf:mbox ?mbox }
     * }
     * </pre>
     * 
     * Where the membership in the KB for :vg is
     * 
     * <pre>
     * <http://example.org/alice>
     * <http://example.org/bob>
     * </pre>
     * 
     * Note: This should be turned into exactly the same AST as the
     * {@link #test_from_and_from_named()} test. The virtual graph definition is
     * expanded with the same effect as the original FROM NAMED clauses.
     * 
     * @throws ParseException
     * @throws TokenMgrError
     * @throws MalformedQueryException
     */
    public void test_virtualGraphs_01() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" + //
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + //
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + //
                "SELECT ?who ?g ?mbox\n" + //
                "FROM <http://example.org/dft.ttl>\n" + //
                "FROM NAMED VIRTUAL GRAPH <http://example.org/vg>\n" + //
//                "FROM NAMED <http://example.org/alice>\n" + //
//                "FROM NAMED <http://example.org/bob>\n" + //
                "WHERE {\n" + //
                "    ?g dc:publisher ?who .\n" + //
                "    GRAPH ?g { ?x foaf:mbox ?mbox } \n" + //
                "}"//
        ;

        /*
         * Setup the virtual graph associations in the data.
         */
        final BigdataURI virtualGraph = valueFactory.asValue(BD.VIRTUAL_GRAPH);

        final BigdataURI vg = valueFactory
                .createURI("http://example.org/vg");

        final BigdataURI context = valueFactory
                .createURI("http://www.bigdata.com/context");

        final BigdataURI uri1 = valueFactory
                .createURI("http://example.org/dft.ttl");

        final BigdataURI alice = valueFactory
                .createURI("http://example.org/alice");

        final BigdataURI bob = valueFactory.createURI("http://example.org/bob");
        
        /*
         * Most URIs wind up declared when we insert the statements, but not
         * [uri1]. Also, none of them wind up with the IVCache set unless we do
         * that explicitly.
         */
        {
            final BigdataValue[] values = new BigdataValue[] { virtualGraph,
                    vg, context, uri1, alice, bob };

            tripleStore.getLexiconRelation().addTerms(values, values.length,
                    false/* readOnly */);
            
//            // Cache the Value on the IV.
//            for(BigdataValue v : values) {
//                v.getIV().setValue(v);
//            }
            
        }

        // Insert the statements.
        {

            final StatementBuffer<BigdataStatement> sb = new StatementBuffer<BigdataStatement>(
                    tripleStore, 10/* capacity */);

            sb.add(valueFactory.createStatement(vg, virtualGraph, alice,
                    context, StatementEnum.Explicit));

            sb.add(valueFactory.createStatement(vg, virtualGraph, bob, context,
                    StatementEnum.Explicit));

            sb.flush();
         
        }

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                prefixDecls.put("foaf", FOAFVocabularyDecl.NAMESPACE);
                prefixDecls.put("dc", DC.NAMESPACE);
                expected.setPrefixDecls(prefixDecls);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);
                projection.addProjectionVar(new VarNode("who"));
                projection.addProjectionVar(new VarNode("g"));
                projection.addProjectionVar(new VarNode("mbox"));
            }

            {
                final DatasetImpl dataset = new DatasetImpl();
                dataset.addDefaultGraph(uri1);
                dataset.addNamedGraph(alice);
                dataset.addNamedGraph(bob);
                final DatasetNode datasetNode = new DatasetNode(dataset, false/* update */);
                expected.setDataset(datasetNode);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);
             
                whereClause.addChild(new StatementPatternNode(new VarNode("g"),
                        new ConstantNode(makeIV(valueFactory.createURI(DC.PUBLISHER
                                .toString()))), new VarNode("who"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
                
                final JoinGroupNode group = new JoinGroupNode();
                whereClause.addChild(group);
                group.setContext(new VarNode("g"));
                group.addChild(new StatementPatternNode(
                        new VarNode("x"),
                        new ConstantNode(makeIV(valueFactory
                                .createURI(FOAFVocabularyDecl.mbox.toString()))),
                        new VarNode("mbox"), new VarNode("g"),
                        Scope.NAMED_CONTEXTS));

            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * SELECT ?who ?g ?mbox
     * FROM <http://example.org/dft.ttl>
     * FROM VIRTUAL GRAPH :vg
     * WHERE
     * {
     *    ?g dc:publisher ?who .
     *    GRAPH ?g { ?x foaf:mbox ?mbox }
     * }
     * </pre>
     * 
     * Where the membership in the KB for :vg is
     * 
     * <pre>
     * <http://example.org/alice>
     * <http://example.org/bob>
     * </pre>
     * 
     * Note: This is similar to the above test, but verifies that the virtual
     * graph declaration is expanded into the default graph membership for the
     * dataset. This test also verifies that the FROM and FROM VIRTUAL GRAPH
     * clauses may be combined freely.
     * 
     * @throws ParseException
     * @throws TokenMgrError
     * @throws MalformedQueryException
     */
    public void test_virtualGraphs_02() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" + //
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + //
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + //
                "SELECT ?who ?g ?mbox\n" + //
                "FROM <http://example.org/dft.ttl>\n" + //
                "FROM VIRTUAL GRAPH <http://example.org/vg>\n" + //
                "WHERE {\n" + //
                "    ?g dc:publisher ?who .\n" + //
                "    GRAPH ?g { ?x foaf:mbox ?mbox } \n" + //
                "}"//
        ;

        /*
         * Setup the virtual graph associations in the data.
         */
        final BigdataURI virtualGraph = valueFactory.asValue(BD.VIRTUAL_GRAPH);

        final BigdataURI vg = valueFactory
                .createURI("http://example.org/vg");

        final BigdataURI context = valueFactory
                .createURI("http://www.bigdata.com/context");

        final BigdataURI uri1 = valueFactory
                .createURI("http://example.org/dft.ttl");

        final BigdataURI alice = valueFactory
                .createURI("http://example.org/alice");

        final BigdataURI bob = valueFactory.createURI("http://example.org/bob");
        
        /*
         * Most URIs wind up declared when we insert the statements, but not
         * [uri1]. Also, none of them wind up with the IVCache set unless we do
         * that explicitly.
         */
        {
            final BigdataValue[] values = new BigdataValue[] { virtualGraph,
                    vg, context, uri1, alice, bob };

            tripleStore.getLexiconRelation().addTerms(values, values.length,
                    false/* readOnly */);
            
//            // Cache the Value on the IV.
//            for(BigdataValue v : values) {
//                v.getIV().setValue(v);
//            }
            
        }

        // Insert the statements.
        {

            final StatementBuffer<BigdataStatement> sb = new StatementBuffer<BigdataStatement>(
                    tripleStore, 10/* capacity */);

            sb.add(valueFactory.createStatement(vg, virtualGraph, alice,
                    context, StatementEnum.Explicit));

            sb.add(valueFactory.createStatement(vg, virtualGraph, bob, context,
                    StatementEnum.Explicit));

            sb.flush();
         
        }

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                prefixDecls.put("foaf", FOAFVocabularyDecl.NAMESPACE);
                prefixDecls.put("dc", DC.NAMESPACE);
                expected.setPrefixDecls(prefixDecls);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);
                projection.addProjectionVar(new VarNode("who"));
                projection.addProjectionVar(new VarNode("g"));
                projection.addProjectionVar(new VarNode("mbox"));
            }

            {
                /*
                 * Note: There is some sensitivity to the distinction between an
                 * empty named graph collection or an empty default graph
                 * collection and a [null] reference. I am not yet convinced
                 * that the code is laid out correctly here, in the
                 * DataSetDeclProcessor, or in the DatasetNode class.
                 * 
                 * Note: The IVCache on alice and bob must be cleared since the
                 * Value is not resolved when we resolve the virtual graph IV to
                 * the IVs of the virtual graph membership.
                 */
//                final BigdataURI alice2 = valueFactory.createURI(alice
//                        .stringValue());
//                alice2.setIV(alice.getIV().clone(true/* clearCache */));
//                final BigdataURI bob2 = valueFactory.createURI(bob
//                        .stringValue());
//                bob2.setIV(bob.getIV().clone(true/* clearCache */));
                @SuppressWarnings("rawtypes")
                final Set<IV> defaultGraphs = new LinkedHashSet<IV>();
                defaultGraphs.add(uri1.getIV());
                defaultGraphs.add(alice.getIV().clone(true/*clearCache*/));
                defaultGraphs.add(bob.getIV().clone(true/*clearCache*/));
                final DatasetNode datasetNode = new DatasetNode(defaultGraphs,
                        null/* namedGraphs */, false/* update */);
                expected.setDataset(datasetNode);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);
             
                whereClause.addChild(new StatementPatternNode(new VarNode("g"),
                        new ConstantNode(makeIV(valueFactory.createURI(DC.PUBLISHER
                                .toString()))), new VarNode("who"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
                
                final JoinGroupNode group = new JoinGroupNode();
                whereClause.addChild(group);
                group.setContext(new VarNode("g"));
                group.addChild(new StatementPatternNode(
                        new VarNode("x"),
                        new ConstantNode(makeIV(valueFactory
                                .createURI(FOAFVocabularyDecl.mbox.toString()))),
                        new VarNode("mbox"), new VarNode("g"),
                        Scope.NAMED_CONTEXTS));

            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

}
