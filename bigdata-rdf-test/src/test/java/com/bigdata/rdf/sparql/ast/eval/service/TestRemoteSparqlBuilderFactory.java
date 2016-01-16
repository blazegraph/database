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
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval.service;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.BNode;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.DC;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.AbstractBigdataExprBuilderTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.service.IRemoteSparqlQueryBuilder;
import com.bigdata.rdf.sparql.ast.service.RemoteServiceOptions;
import com.bigdata.rdf.sparql.ast.service.RemoteSparql10QueryBuilder;
import com.bigdata.rdf.sparql.ast.service.RemoteSparql11DraftQueryBuilder;
import com.bigdata.rdf.sparql.ast.service.RemoteSparql11QueryBuilder;
import com.bigdata.rdf.sparql.ast.service.RemoteSparqlBuilderFactory;
import com.bigdata.rdf.sparql.ast.service.SPARQLVersion;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * Test suite the {@link RemoteSparqlBuilderFactory}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestRemoteServiceCallImpl.java 6060 2012-03-02 16:07:38Z
 *          thompsonbry $
 */
public class TestRemoteSparqlBuilderFactory extends
        AbstractBigdataExprBuilderTestCase {

//    private static final Logger log = Logger
//            .getLogger(TestRemoteSparqlQueryBuilder.class);
    
    /**
     * 
     */
    public TestRemoteSparqlBuilderFactory() {
    }

    /**
     * @param name
     */
    public TestRemoteSparqlBuilderFactory(String name) {
        super(name);
    }
    
    @SuppressWarnings("unchecked")
    private void addResolveIVs(final BigdataValue... values) {

        tripleStore.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        /*
         * Cache value on IVs to align with behavior of the SPARQL parser.
         * 
         * Note: BatchRDFValueResolver does this, so we have to do it to in
         * order to have an exact structural match when we parse the generated
         * SPARQL query and then verify the AST model.
         */
        for (BigdataValue v : values) {

            v.getIV().setValue(v);
            
        }

    }

//    /**
//     * Wrap as an {@link IConstant}.
//     * 
//     * @param iv
//     *            The {@link IV}.
//     *            
//     * @return The {@link IConstant}.
//     */
//    private IConstant<?> asConstant(final IV<?,?> iv) {
//        
//        return new Constant<IV<?,?>>(iv);
//        
//    }

    /**
     * A simple test with nothing bound and NO source solution. This can be
     * handled by either {@link IRemoteSparqlQueryBuilder}.
     */
    public void test_service_001() throws Exception {
        
        final BigdataURI serviceURI = valueFactory
                .createURI("http://www.bigdata.com/myService");

        final GraphPatternGroup<IGroupMemberNode> groupNode = new JoinGroupNode();
        {
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
        }
        
        final String exprImage = "SERVICE <" + serviceURI + "> { ?s ?p ?o }";
        
        final Map<String,String> prefixDecls = new LinkedHashMap<String, String>();
        {
            prefixDecls.put("foo", "http://www.bigdata.com/foo");
        }

        final ServiceNode serviceNode = new ServiceNode(new ConstantNode(
                makeIV(serviceURI)), groupNode);
        {
            final Set<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
            {
                projectedVars.add(Var.var("s"));
                projectedVars.add(Var.var("p"));
                projectedVars.add(Var.var("o"));
            }

            serviceNode.setExprImage(exprImage);
            serviceNode.setPrefixDecls(prefixDecls);
            serviceNode.setProjectedVars(projectedVars);
        }
        
        final List<BindingSet> bindingSets = new LinkedList<BindingSet>();
        
        final BindingSet[] a = bindingSets.toArray(new BindingSet[bindingSets
                .size()]);

        final RemoteServiceOptions options = new RemoteServiceOptions();

        options.setSPARQLVersion(SPARQLVersion.SPARQL_10);

        assertEquals(RemoteSparql10QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

        options.setSPARQLVersion(SPARQLVersion.SPARQL_11);

        assertEquals(RemoteSparql11QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());
        
        options.setSPARQLVersion(SPARQLVersion.SPARQL_11_DRAFT_BINDINGS);

        assertEquals(RemoteSparql11DraftQueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

    }

    /**
     * A simple test with nothing bound and a single <em>empty</em> source
     * solution. This can be handled by either {@link IRemoteSparqlQueryBuilder}
     * .
     */
    public void test_service_001b() throws Exception {
        
        final BigdataURI serviceURI = valueFactory
                .createURI("http://www.bigdata.com/myService");

        final GraphPatternGroup<IGroupMemberNode> groupNode = new JoinGroupNode();
        {
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
        }
        
        final String exprImage = "SERVICE <" + serviceURI + "> { ?s ?p ?o }";
        
        final Map<String,String> prefixDecls = new LinkedHashMap<String, String>();
        {
            prefixDecls.put("foo", "http://www.bigdata.com/foo");
        }

        final ServiceNode serviceNode = new ServiceNode(new ConstantNode(
                makeIV(serviceURI)), groupNode);
        {
            final Set<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
            {
                projectedVars.add(Var.var("s"));
                projectedVars.add(Var.var("p"));
                projectedVars.add(Var.var("o"));
            }

            serviceNode.setExprImage(exprImage);
            serviceNode.setPrefixDecls(prefixDecls);
            serviceNode.setProjectedVars(projectedVars);
        }
        
        final List<BindingSet> bindingSets = new LinkedList<BindingSet>();
        {
            bindingSets.add(new MapBindingSet());
        }
        
        final BindingSet[] a = bindingSets.toArray(new BindingSet[bindingSets
                .size()]);

        final RemoteServiceOptions options = new RemoteServiceOptions();

        options.setSPARQLVersion(SPARQLVersion.SPARQL_10);

        assertEquals(RemoteSparql10QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

        options.setSPARQLVersion(SPARQLVersion.SPARQL_11);

        assertEquals(RemoteSparql11QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());
        
        options.setSPARQLVersion(SPARQLVersion.SPARQL_11_DRAFT_BINDINGS);

        assertEquals(RemoteSparql11DraftQueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());
        
    }
        
    /**
     * A test where a single fully bound triple pattern is presented. This can
     * be handled by either {@link IRemoteSparqlQueryBuilder}.
     */
    public void test_service_002() throws Exception {

        /*
         * Resolve IVs that we will use below.
         */
        final BigdataURI dcCreator = valueFactory.asValue(DC.CREATOR);
        final BigdataURI book1 = valueFactory.createURI("http://example.org/book/book1");
        final BigdataURI book2 = valueFactory.createURI("http://example.org/book/book2");
        final BigdataURI author1 = valueFactory.createURI("http://example.org/author/author1");
        final BigdataURI author2 = valueFactory.createURI("http://example.org/author/author2");

        addResolveIVs(dcCreator, book1, book2, author1, author2);

        final BigdataURI serviceURI = valueFactory
                .createURI("http://www.bigdata.com/myService");

        final GraphPatternGroup<IGroupMemberNode> groupNode = new JoinGroupNode();
        {
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
        }
        
        final String exprImage = "SERVICE <" + serviceURI + "> { ?s ?p ?o }";
        
        final Map<String,String> prefixDecls = new LinkedHashMap<String, String>();
        {
            prefixDecls.put("foo", "http://www.bigdata.com/foo");
        }

        final ServiceNode serviceNode = new ServiceNode(new ConstantNode(
                makeIV(serviceURI)), groupNode);
        {
            final Set<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
            {
                projectedVars.add(Var.var("s"));
                projectedVars.add(Var.var("p"));
                projectedVars.add(Var.var("o"));
            }

            serviceNode.setExprImage(exprImage);
            serviceNode.setPrefixDecls(prefixDecls);
            serviceNode.setProjectedVars(projectedVars);
        }

        final List<BindingSet> bindingSets = new LinkedList<BindingSet>();
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("s", book1);
            bset.addBinding("p", DC.CREATOR);
            bset.addBinding("o", author1);
            bindingSets.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("s", book2);
            bset.addBinding("p", DC.CREATOR);
            bset.addBinding("o", author2);
            bindingSets.add(bset);
        }

        final BindingSet[] a = bindingSets.toArray(new BindingSet[bindingSets
                .size()]);

        final RemoteServiceOptions options = new RemoteServiceOptions();

        options.setSPARQLVersion(SPARQLVersion.SPARQL_10);

        assertEquals(RemoteSparql10QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

        options.setSPARQLVersion(SPARQLVersion.SPARQL_11);

        assertEquals(RemoteSparql11QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());
        
        options.setSPARQLVersion(SPARQLVersion.SPARQL_11_DRAFT_BINDINGS);

        assertEquals(RemoteSparql11DraftQueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

    }
        
    /**
     * A variant test in which there are some BINDINGS to be passed through. The
     * set of bindings covers the different types of RDF {@link Value} and also
     * exercises the prefix declarations. This test does NOT use blank nodes in
     * the BINDINGS. This can be handled by either
     * {@link IRemoteSparqlQueryBuilder}.
     */
    public void test_service_003() throws Exception {
        
        /*
         * Resolve IVs that we will use below.
         */
        final BigdataURI dcCreator = valueFactory.asValue(DC.CREATOR);
        final BigdataURI book1 = valueFactory.createURI("http://example.org/book/book1");
        final BigdataURI book2 = valueFactory.createURI("http://example.org/book/book2");
        final BigdataLiteral book3 = valueFactory.createLiteral("Semantic Web Primer");
        final BigdataLiteral book4 = valueFactory.createLiteral("Semantic Web Primer", "DE");
        final BigdataLiteral book5 = valueFactory.createLiteral("12", XSD.INT);
        final BigdataLiteral book6 = valueFactory.createLiteral("true", XSD.BOOLEAN);

        addResolveIVs(dcCreator, book1, book2, book3, book4, book5, book6);

        final BigdataURI serviceURI = valueFactory
                .createURI("http://www.bigdata.com/myService");

        final GraphPatternGroup<IGroupMemberNode> groupNode = new JoinGroupNode();
        {
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("book")));
        }
        
        final String exprImage = "SERVICE <" + serviceURI + "> { ?book ?p ?o}";
        
        final Map<String,String> prefixDecls = new LinkedHashMap<String, String>();
        {
            prefixDecls.put("", "http://example.org/book/");
        }

        final ServiceNode serviceNode = new ServiceNode(new ConstantNode(
                makeIV(serviceURI)), groupNode);
        {
            final Set<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
            {
                projectedVars.add(Var.var("book"));
                projectedVars.add(Var.var("p"));
                projectedVars.add(Var.var("o"));
            }

            serviceNode.setExprImage(exprImage);
            serviceNode.setPrefixDecls(prefixDecls);
            serviceNode.setProjectedVars(projectedVars);
        }

        final List<BindingSet> bindingSets = new LinkedList<BindingSet>();
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book", book1);
            bindingSets.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book", book2);
            bindingSets.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book", book3);
            bindingSets.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book", book4);
            bindingSets.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book", book5);
            bindingSets.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book", book6);
            bindingSets.add(bset);
        }

        final BindingSet[] a = bindingSets.toArray(new BindingSet[bindingSets
                .size()]);

        final RemoteServiceOptions options = new RemoteServiceOptions();

        options.setSPARQLVersion(SPARQLVersion.SPARQL_10);

        assertEquals(RemoteSparql10QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

        options.setSPARQLVersion(SPARQLVersion.SPARQL_11);

        assertEquals(RemoteSparql11QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());
        
        options.setSPARQLVersion(SPARQLVersion.SPARQL_11_DRAFT_BINDINGS);

        assertEquals(RemoteSparql11DraftQueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

    }

    /**
     * A variant test in there is a blank node in the BINDINGS to be flowed
     * through to the remote SERVICE.  In this test the blank nodes are not
     * correlated so we do not need to impose a FILTER on the remote service.
     * This can be handled by either {@link IRemoteSparqlQueryBuilder}.
     */
    public void test_service_004() throws Exception {
        
        final BigdataURI serviceURI = valueFactory
                .createURI("http://www.bigdata.com/myService");

        final GraphPatternGroup<IGroupMemberNode> groupNode = new JoinGroupNode();
        {
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
        }
        
        final String exprImage = "SERVICE <" + serviceURI + "> { ?s ?p ?o }";
        
        final Map<String,String> prefixDecls = new LinkedHashMap<String, String>();

        final ServiceNode serviceNode = new ServiceNode(new ConstantNode(
                makeIV(serviceURI)), groupNode);
        {
            final Set<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
            {
                projectedVars.add(Var.var("s"));
                projectedVars.add(Var.var("p"));
                projectedVars.add(Var.var("o"));
            }

            serviceNode.setExprImage(exprImage);
            serviceNode.setPrefixDecls(prefixDecls);
            serviceNode.setProjectedVars(projectedVars);
        }
        
        final List<BindingSet> bindingSets = new LinkedList<BindingSet>();
        /*
         * Note: Blank nodes are not permitting in the BINDINGS clause (per the
         * SPARQL 1.1 grammar). However, a blank node MAY be turned into an
         * unbound variable as long as we impose the constraint that all vars
         * having that blank node for a solution are EQ (same term).
         */
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("s", new BNodeImpl("abc"));
            bindingSets.add(bset);
        }

        final BindingSet[] a = bindingSets.toArray(new BindingSet[bindingSets
                .size()]);

        final RemoteServiceOptions options = new RemoteServiceOptions();

        options.setSPARQLVersion(SPARQLVersion.SPARQL_10);

        assertEquals(RemoteSparql10QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

        options.setSPARQLVersion(SPARQLVersion.SPARQL_11);

        assertEquals(RemoteSparql11QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());
        
        options.setSPARQLVersion(SPARQLVersion.SPARQL_11_DRAFT_BINDINGS);

        assertEquals(RemoteSparql11DraftQueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

    }

    /**
     * A variant test in there is a blank node in the BINDINGS to be flowed
     * through to the remote SERVICE. In this test the blank nodes are
     * correlated but there is only one solution to be vectored. This can be
     * handled by either {@link IRemoteSparqlQueryBuilder}.
     */
    public void test_service_005() throws Exception {
        
        final BigdataURI serviceURI = valueFactory
                .createURI("http://www.bigdata.com/myService");

        final GraphPatternGroup<IGroupMemberNode> groupNode = new JoinGroupNode();
        {
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
        }
        
        final String exprImage = "SERVICE <" + serviceURI + "> { ?s ?p ?o }";
        
        final Map<String,String> prefixDecls = new LinkedHashMap<String, String>();

        final ServiceNode serviceNode = new ServiceNode(new ConstantNode(
                makeIV(serviceURI)), groupNode);
        {
            final Set<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
            {
                projectedVars.add(Var.var("s"));
                projectedVars.add(Var.var("p"));
                projectedVars.add(Var.var("o"));
            }

            serviceNode.setExprImage(exprImage);
            serviceNode.setPrefixDecls(prefixDecls);
            serviceNode.setProjectedVars(projectedVars);
        }

        final List<BindingSet> bindingSets = new LinkedList<BindingSet>();
        /*
         * Note: Blank nodes are not permitting in the BINDINGS clause (per the
         * SPARQL 1.1 grammar). However, a blank node MAY be turned into an
         * unbound variable as long as we impose the constraint that all vars
         * having that blank node for a solution are EQ (same term).
         * 
         * Note: For this query, the *same* blank node is used for ?s and ?book.
         * That needs to be turned into a FILTER which is attached to the remote
         * SPARQL query in order to maintain the correlation between those
         * variables (FILTER ?s = ?book).
         */
        {
            final MapBindingSet bset = new MapBindingSet();
            final BNode tmp = new BNodeImpl("abc");
            bset.addBinding("s", tmp);
            bset.addBinding("o", tmp);
            bindingSets.add(bset);
        }

        final BindingSet[] a = bindingSets.toArray(new BindingSet[bindingSets
                .size()]);

        final RemoteServiceOptions options = new RemoteServiceOptions();

        options.setSPARQLVersion(SPARQLVersion.SPARQL_10);

        assertEquals(RemoteSparql10QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

        options.setSPARQLVersion(SPARQLVersion.SPARQL_11);

        assertEquals(RemoteSparql11QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());
        
        options.setSPARQLVersion(SPARQLVersion.SPARQL_11_DRAFT_BINDINGS);

        assertEquals(RemoteSparql11DraftQueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

    }

    /**
     * A variant test in there is a blank node in the BINDINGS to be flowed
     * through to the remote SERVICE. In this test the blank nodes are
     * correlated but there is only one solution to be vectored so we will
     * impose a FILTER on the remote service to enforce that correlation. This
     * test differs from the previous test by making more than two variables in
     * the SERVICE clause correlated through shared blank nodes. We need to use
     * a more complex SERVICE graph pattern to accomplish this since the
     * predicate can not be a blank node. This can be handled by either
     * {@link IRemoteSparqlQueryBuilder}.
     */
    public void test_service_006() throws Exception {
        
        final BigdataURI serviceURI = valueFactory
                .createURI("http://www.bigdata.com/myService");

        final GraphPatternGroup<IGroupMemberNode> groupNode = new JoinGroupNode();
        {

            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
            
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o1")));
        }
        
        final String exprImage = "SERVICE <" + serviceURI + "> { ?s ?p ?o . ?s ?p ?o1 }";
        
        final Map<String,String> prefixDecls = new LinkedHashMap<String, String>();

        final ServiceNode serviceNode = new ServiceNode(new ConstantNode(
                makeIV(serviceURI)), groupNode);
        {
            final Set<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
            {
                projectedVars.add(Var.var("s"));
                projectedVars.add(Var.var("p"));
                projectedVars.add(Var.var("o"));
                projectedVars.add(Var.var("o1"));
            }

            serviceNode.setExprImage(exprImage);
            serviceNode.setPrefixDecls(prefixDecls);
            serviceNode.setProjectedVars(projectedVars);
        }

        final List<BindingSet> bindingSets = new LinkedList<BindingSet>();
        /*
         * Note: Blank nodes are not permitting in the BINDINGS clause (per the
         * SPARQL 1.1 grammar). However, a blank node MAY be turned into an
         * unbound variable as long as we impose the constraint that all vars
         * having that blank node for a solution are EQ (same term).
         * 
         * Note: For this query, the *same* blank node is used for ?s and ?book.
         * That needs to be turned into a FILTER which is attached to the remote
         * SPARQL query in order to maintain the correlation between those
         * variables (FILTER ?s = ?book).
         */
        {
            final MapBindingSet bset = new MapBindingSet();
            final BNode tmp = new BNodeImpl("abc");
            bset.addBinding("s", tmp);
            bset.addBinding("o", tmp);
            bset.addBinding("o1", tmp);
            bindingSets.add(bset);
        }

        final BindingSet[] a = bindingSets.toArray(new BindingSet[bindingSets
                .size()]);

        final RemoteServiceOptions options = new RemoteServiceOptions();

        options.setSPARQLVersion(SPARQLVersion.SPARQL_10);

        assertEquals(RemoteSparql10QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

        options.setSPARQLVersion(SPARQLVersion.SPARQL_11);

        assertEquals(RemoteSparql11QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());
        
        options.setSPARQLVersion(SPARQLVersion.SPARQL_11_DRAFT_BINDINGS);

        assertEquals(RemoteSparql11DraftQueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

    }

    /**
     * A variant test in there is a blank node in the BINDINGS to be flowed
     * through to the remote SERVICE. In this test the blank nodes are
     * correlated and there is more than one solution to be vectored.
     * <p>
     * This case is only handled by the {@link RemoteSparql10QueryBuilder}.
     */
    public void test_service_007() throws Exception {

        final BigdataURI serviceURI = valueFactory
                .createURI("http://www.bigdata.com/myService");

        final GraphPatternGroup<IGroupMemberNode> groupNode = new JoinGroupNode();
        {
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
        }

        final String exprImage = "SERVICE <" + serviceURI + "> { ?s ?p ?o }";

        final Map<String, String> prefixDecls = new LinkedHashMap<String, String>();

        final ServiceNode serviceNode = new ServiceNode(new ConstantNode(
                makeIV(serviceURI)), groupNode);
        {
            final Set<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
            {
                projectedVars.add(Var.var("s"));
                projectedVars.add(Var.var("p"));
                projectedVars.add(Var.var("o"));
            }

            serviceNode.setExprImage(exprImage);
            serviceNode.setPrefixDecls(prefixDecls);
            serviceNode.setProjectedVars(projectedVars);
        }

        final List<BindingSet> bindingSets = new LinkedList<BindingSet>();
        /*
         * Note: Blank nodes are not permitting in the BINDINGS clause (per the
         * SPARQL 1.1 grammar). However, a blank node MAY be turned into an
         * unbound variable as long as we impose the constraint that all vars
         * having that blank node for a solution are EQ (same term).
         * 
         * Note: For this query, the *same* blank node is used for ?s and ?book.
         * That needs to be turned into a FILTER which is attached to the remote
         * SPARQL query in order to maintain the correlation between those
         * variables (FILTER ?s = ?book).
         */
        {
            final MapBindingSet bset = new MapBindingSet();
            final BNode tmp = new BNodeImpl("abc");
            bset.addBinding("s", tmp);
            bset.addBinding("o", tmp);
            bindingSets.add(bset);
        }
        { // A 2nd solution is required to force the correlated var/bnode issue.
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("s", new LiteralImpl("Semantic Web Primer"));
            bindingSets.add(bset);
        }

        final BindingSet[] a = bindingSets.toArray(new BindingSet[bindingSets
                .size()]);


        final RemoteServiceOptions options = new RemoteServiceOptions();

        options.setSPARQLVersion(SPARQLVersion.SPARQL_10);

        assertEquals(RemoteSparql10QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

        options.setSPARQLVersion(SPARQLVersion.SPARQL_11);

        /*
         * Still uses the SPARQL 1.0 query builder since it will not rely on the
         * BINDINGS clause.
         */
        assertEquals(RemoteSparql10QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());
        
        options.setSPARQLVersion(SPARQLVersion.SPARQL_11_DRAFT_BINDINGS);

        /*
         * Still uses the SPARQL 1.0 query builder since it will not rely on the
         * BINDINGS clause.
         */
        assertEquals(RemoteSparql10QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

    }

    /**
     * A variant test in there is a blank node in the BINDINGS to be flowed
     * through to the remote SERVICE. In this test the blank nodes are
     * correlated so we MUST impose a constraint on the remote service to
     * enforce that correlation. However, there is another solution in which the
     * two variables are NOT correlated so that FILTER MUST NOT be imposed
     * across all such solutions. Therefore the SERVICE class will be vectored
     * by rewriting it into a UNION with different variable names in each
     * variant of the UNION.
     * <p>
     * This case is only handled by the {@link RemoteSparql10QueryBuilder}.
     */
    public void test_service_008() throws Exception {
        
        final BigdataURI serviceURI = valueFactory
                .createURI("http://www.bigdata.com/myService");

        final GraphPatternGroup<IGroupMemberNode> groupNode = new JoinGroupNode();
        {
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
        }
        
        final String exprImage = "SERVICE <" + serviceURI + "> { ?s ?p ?o }";
        
        final Map<String,String> prefixDecls = new LinkedHashMap<String, String>();

        final ServiceNode serviceNode = new ServiceNode(new ConstantNode(
                makeIV(serviceURI)), groupNode);
        {
            final Set<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
            {
                projectedVars.add(Var.var("s"));
                projectedVars.add(Var.var("p"));
                projectedVars.add(Var.var("o"));
            }

            serviceNode.setExprImage(exprImage);
            serviceNode.setPrefixDecls(prefixDecls);
            serviceNode.setProjectedVars(projectedVars);
        }

        final List<BindingSet> bindingSets = new LinkedList<BindingSet>();
        /*
         * Note: Blank nodes are not permitting in the BINDINGS clause (per the
         * SPARQL 1.1 grammar). However, a blank node MAY be turned into an
         * unbound variable as long as we impose the constraint that all vars
         * having that blank node for a solution are EQ (same term).
         * 
         * Note: For this query, the *same* blank node is used for ?s and ?book.
         * That needs to be turned into a FILTER which is attached to the remote
         * SPARQL query in order to maintain the correlation between those
         * variables (FILTER ?s = ?book).
         */
        { // Note: Blank nodes ARE correlated for this solution.
            final MapBindingSet bset = new MapBindingSet();
            final BNode tmp = new BNodeImpl("abc");
            bset.addBinding("s", tmp);
            bset.addBinding("o", tmp);
            bindingSets.add(bset);
        }
        { // Note: Blank nodes are NOT correlated for this solution.
            final MapBindingSet bset = new MapBindingSet();
            final BNode tmp1 = new BNodeImpl("foo");
            final BNode tmp2 = new BNodeImpl("bar");
            bset.addBinding("s", tmp1);
            bset.addBinding("o", tmp2);
            bindingSets.add(bset);
        }

        final BindingSet[] a = bindingSets.toArray(new BindingSet[bindingSets
                .size()]);

        final RemoteServiceOptions options = new RemoteServiceOptions();

        options.setSPARQLVersion(SPARQLVersion.SPARQL_10);

        assertEquals(RemoteSparql10QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

        options.setSPARQLVersion(SPARQLVersion.SPARQL_11);

        /*
         * Still uses the SPARQL 1.0 query builder since it will not rely on the
         * BINDINGS clause.
         */
        assertEquals(RemoteSparql10QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());
        
        options.setSPARQLVersion(SPARQLVersion.SPARQL_11_DRAFT_BINDINGS);

        /*
         * Still uses the SPARQL 1.0 query builder since it will not rely on the
         * BINDINGS clause.
         */
        assertEquals(RemoteSparql10QueryBuilder.class,
                RemoteSparqlBuilderFactory.get(options, serviceNode, a)
                        .getClass());

    }
    
    public void test_service_009() throws Exception {
        
        final BigdataURI serviceURI = valueFactory
                .createURI("http://www.bigdata.com/myService");
        
        final String exprImage = "SERVICE <" + serviceURI + "> { ?s ?p ?o }";
        
        final Map<String,String> prefixDecls = new LinkedHashMap<String, String>();
        
        final GraphPatternGroup<IGroupMemberNode> groupNode = new JoinGroupNode();
        {
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
        }
        

        final ServiceNode serviceNode = new ServiceNode(new ConstantNode(
                makeIV(serviceURI)), groupNode);
        
        {
            final Set<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
            {
                projectedVars.add(Var.var("s"));
 
            }

            serviceNode.setExprImage(exprImage);
            serviceNode.setPrefixDecls(prefixDecls);
            serviceNode.setProjectedVars(projectedVars);
        }

        final List<BindingSet> bindingSets = new LinkedList<BindingSet>();
       
        { 
        	final MapBindingSet bset = new MapBindingSet();
            final BigdataURI tmp1 = valueFactory.createURI("p:p1");
            final BigdataLiteral tmp2 = valueFactory.createLiteral("lit1");
            bset.addBinding("p", tmp1);
            bset.addBinding("o", tmp2);
            bindingSets.add(bset);
        }
        
        final BindingSet[] a = bindingSets.toArray(new BindingSet[bindingSets
                .size()]);

        final RemoteServiceOptions options = new RemoteServiceOptions();        

        options.setSPARQLVersion(SPARQLVersion.SPARQL_10);

        IRemoteSparqlQueryBuilder queryBuilder = RemoteSparqlBuilderFactory.get(options, serviceNode, a);
        
        final String actualQueryStrVersion_10 = queryBuilder.getSparqlQuery(a).replaceAll("\\s+", " ");
        
        final String expectedSparqlVersion_10 = new String("SELECT  ?s " + 
											        		"WHERE { " + //
											        		"FILTER ( sameTerm( ?p, <p:p1>) ). " + //
											        		"FILTER ( sameTerm( ?o, \"lit1\") ). " + //
											        		" ?s ?p ?o " + // 
											        		"} ").replaceAll("\\s+", " ");
        
        assertEquals(expectedSparqlVersion_10, actualQueryStrVersion_10);

        options.setSPARQLVersion(SPARQLVersion.SPARQL_11);

        queryBuilder = RemoteSparqlBuilderFactory.get(options, serviceNode, a);
        
        final String actualQueryStrVersion_11 = queryBuilder.getSparqlQuery(a).replaceAll("\\s+", " ");
        
        final String expectedSparqlVersion_11 = new String("SELECT  ?s " + //
											        		"WHERE {" + //
											        		" ?s ?p ?o " + // 
											        		"} " + //
											        		"VALUES ( ?p ?o) { " + //
											        		"( <p:p1> \"lit1\" ) " + //
											        		"} ").replaceAll("\\s+", " ");
        
        assertEquals(expectedSparqlVersion_11, actualQueryStrVersion_11);
    
        options.setSPARQLVersion(SPARQLVersion.SPARQL_11_DRAFT_BINDINGS);
        
        queryBuilder = RemoteSparqlBuilderFactory.get(options, serviceNode, a);
        
        final String actualQueryStrVersion_11_DRAFT_BINDINGS = queryBuilder.getSparqlQuery(a).replaceAll("\\s+", " ");
        
        final String expectedSparqlVersion_11_DRAFT_BINDINGS = new String("SELECT  ?s " + //
														        		"WHERE {" + //
														        		" ?s ?p ?o " + // 
														        		"} " + //
														        		"BINDINGS ?p ?o { " + //
														        		"( <p:p1> \"lit1\" ) " + //
														        		"} ").replaceAll("\\s+", " ");
        
        assertEquals(actualQueryStrVersion_11_DRAFT_BINDINGS, expectedSparqlVersion_11_DRAFT_BINDINGS);
 
    }

}
