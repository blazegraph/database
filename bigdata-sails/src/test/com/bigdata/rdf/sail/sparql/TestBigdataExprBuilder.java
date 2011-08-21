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
 * Created on Aug 20, 2011
 */
package com.bigdata.rdf.sail.sparql;

import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.SliceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Test suite for {@link BigdataExprBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * TODO Test each query type (SELECT, ASK, DESCRIBE, CONSTRUCT).
 * 
 * TODO Test SLICE.
 * 
 * TODO Test conversion of value expressions.
 * 
 * TODO Test SUBQUERY.
 * 
 * TODO Modify grammar and test named subquery (WITH AS INCLUDE).
 * 
 * TODO Test SELECT DISTINCT, SELECT REDUCED, SELECT *.
 */
public class TestBigdataExprBuilder extends TestCase {

    private static final Logger log = Logger
            .getLogger(TestBigdataExprBuilder.class);
    
    public TestBigdataExprBuilder() {
    }

    public TestBigdataExprBuilder(String name) {
        super(name);
    }

    protected String baseURI = null;
    protected AbstractTripleStore tripleStore = null;
   
    protected void setUp() throws Exception {
        
        tripleStore = getStore(getProperties());
        
    }

    protected void tearDown() throws Exception {
        
        if(tripleStore != null) {
            
            tripleStore.__tearDownUnitTest();
            
            tripleStore = null;
            
        }
        
    }
    
    /*
     * Mock up the tripleStore.
     */
    
    protected Properties getProperties() {

        final Properties properties = new Properties();

//        // turn on quads.
//        properties.setProperty(AbstractTripleStore.Options.QUADS, "true");

        // override the default vocabulary.
        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
                NoVocabulary.class.getName());

        // turn off axioms.
        properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        // Note: No persistence.
        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());
        
        return properties;

    }

    protected AbstractTripleStore getStore(final Properties properties) {

        final String namespace = "kb";

        // create/re-open journal.
        final Journal journal = new Journal(properties);

        final LocalTripleStore lts = new LocalTripleStore(journal, namespace,
                ITx.UNISOLATED, properties);

        lts.create();

        return lts;

    }

    /*
     * Mock up the parser and jjtree AST => bigdata AST translator.
     */
    
    protected QueryRoot parse(final String queryStr, final String baseURI)
            throws MalformedQueryException, TokenMgrError, ParseException {

        final ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(queryStr);
        StringEscapesProcessor.process(qc);
        BaseDeclProcessor.process(qc, baseURI);
        final Map<String, String> prefixes = PrefixDeclProcessor.process(qc);
//        WildcardProjectionProcessor.process(qc); // No. We use "*" as a variable name.
        BlankNodeVarProcessor.process(qc);
        final IQueryNode queryRoot = buildQueryModel(qc, tripleStore);

        return (QueryRoot) queryRoot;

    }

    private IQueryNode buildQueryModel(final ASTQueryContainer qc,
            final AbstractTripleStore tripleStore)
            throws MalformedQueryException {

        final BigdataExprBuilder exprBuilder = new BigdataExprBuilder(
                tripleStore.getValueFactory());
        try {
        
            return (IQueryNode) qc.jjtAccept(exprBuilder, null);
            
        } catch (VisitorException e) {
            
            throw new MalformedQueryException(e.getMessage(), e);
        
        }
        
    }

    /**
     * Unit test for simple SELECT query
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o}
     * </pre>
     */
    public void test_select_s_where_s_p_o() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot();
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
            expected.setRoot(whereClause);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);
//        final QueryRoot actual = new QueryRoot();
//        {
//
//            final ProjectionNode projection2 = new ProjectionNode();
//            projection2.addProjectionVar(new VarNode("s"));
//            actual.setProjection(projection2);
//
//            final JoinGroupNode whereClause2 = new JoinGroupNode();
//            whereClause2.addChild(new StatementPatternNode(new VarNode("s"),
//                    new VarNode("p"), new VarNode("o")));
//            actual.setRoot(whereClause2);
//        }

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for SELECT DISTINCT
     * 
     * <pre>
     * SELECT DISTINCT ?s where {?s ?p ?o}
     * </pre>
     */
    public void test_select_distinct() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select DISTINCT ?s where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot();
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            projection.setDistinct(true);
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
            expected.setRoot(whereClause);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for SELECT REDUCED
     * 
     * <pre>
     * SELECT REDUCED ?s where {?s ?p ?o}
     * </pre>
     */
    public void test_select_reduced() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select REDUCED ?s where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot();
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            projection.setReduced(true);
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
            expected.setRoot(whereClause);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for SELECT query with a wildcard (<code>*</code>).
     * 
     * <pre>
     * SELECT * where {?s ?p ?o}
     * </pre>
     */
    public void test_select_star() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select * where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot();
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
            expected.setRoot(whereClause);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for SLICE in SELECT query.
     * <pre>
     * SELECT ?s where {?s ?p ?o} LIMIT 10 OFFSET 5
     * </pre>
     * 
     * @throws MalformedQueryException
     * @throws TokenMgrError
     * @throws ParseException
     */
    public void test_slice() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o} limit 10 offset 5";

        final QueryRoot expected = new QueryRoot();
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
            expected.setRoot(whereClause);
            
            final SliceNode slice = new SliceNode();
            expected.setSlice(slice);
            slice.setLimit(10);
            slice.setOffset(5);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for join group
     * <pre>
     * select ?s where {?s ?p ?o. ?o ?p2 ?s}
     * </pre>
     * @throws MalformedQueryException
     * @throws TokenMgrError
     * @throws ParseException
     */
    public void test_select_joins() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o. ?o ?p2 ?s}";

        final QueryRoot expected = new QueryRoot();
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setRoot(whereClause);
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
            whereClause.addChild(new StatementPatternNode(new VarNode("o"),
                    new VarNode("p2"), new VarNode("s")));
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    protected static void assertSameAST(final String queryStr,
            final QueryRoot expected, final QueryRoot actual) {

        log.error("queryStr:\n" + queryStr);
        log.error("expected: " + expected);
        log.error("actual  :" + actual);

        assertEquals("parent", expected.getParent(), actual.getParent());

        assertEquals("dataset", expected.getDataset(), actual.getDataset());

        assertEquals("namedSubqueries", expected.getNamedSubqueries(),
                actual.getNamedSubqueries());

        assertEquals("projection", expected.getProjection(),
                actual.getProjection());

        assertEquals("whereClause", expected.getWhereClause(),
                actual.getWhereClause());

        assertEquals("groupBy", expected.getGroupBy(), actual.getGroupBy());

        assertEquals("having", expected.getHaving(), actual.getHaving());

        assertEquals("slice", expected.getSlice(), actual.getSlice());

    }

}
