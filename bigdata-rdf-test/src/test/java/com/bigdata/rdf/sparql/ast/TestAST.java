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
 * Created on March 11, 2008
 */

package com.bigdata.rdf.sparql.ast;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.Compare.CompareOp;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.internal.constraints.XSDBooleanIVValueExpression;
import com.bigdata.rdf.internal.impl.TermId;

/**
 * TODO This does not really "test" much, just exercises some basic aspects of
 * the API. You need to verify the "pretty print" of the AST by hand. It is just
 * written onto the log.
 * 
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class TestAST extends TestCase {

	private static final Logger log = Logger.getLogger(TestAST.class);
	
    /**
     * 
     */
    public TestAST() {
        super();
    }

    /**
     * @param name
     */
    public TestAST(String name) {
        super(name);
    }
    
    /**
     * select *
     * where {
     *   predicate1 .
     *   predicate2 .
     *   filter3 .
     * }
     */
    public void testSimpleJoin() {
    	
    	final GraphPatternGroup<IGroupMemberNode> root = new JoinGroupNode();
    	
    	root.addChild(sp(1));
    	root.addChild(sp(2));
    	root.addChild(filter(3));
    	
        final QueryRoot query = new QueryRoot(QueryType.SELECT);
    	
    	query.setWhereClause(root);

    	if (log.isInfoEnabled())
    		log.info("\n"+query.toString());
    	
    }
    
    /**
     * select *
     * where {
     *   predicate1 .
     *   optional { predicate2 . }
     * }
     */
    public void testSimpleOptional() {
    	
        final GraphPatternGroup<IGroupMemberNode> root = new JoinGroupNode();
    	root.addChild(sp(1));
    	
    	final IGroupNode optional = new JoinGroupNode(true);
    	optional.addChild(sp(2));
    	
    	root.addChild(optional);
    	
        final QueryRoot query = new QueryRoot(QueryType.SELECT);
        
        query.setWhereClause(root);

    	if (log.isInfoEnabled())
    		log.info("\n"+query.toString());
    	
    }
    
    /**
     * select *
     * where {
     *   predicate1 .
     *   optional { 
     *     predicate2 .
     *     predicate3 . 
     *   }
     * }
     */
    public void testOptionalJoinGroup() {
    	
        final GraphPatternGroup<IGroupMemberNode> root = new JoinGroupNode();
    	root.addChild(sp(1));
    	
    	final IGroupNode optional = new JoinGroupNode(true);
    	optional.addChild(sp(2));
    	optional.addChild(sp(3));
    	
    	root.addChild(optional);
    	
        final QueryRoot query = new QueryRoot(QueryType.SELECT);
        
        query.setWhereClause(root);

    	if (log.isInfoEnabled())
    		log.info("\n"+query.toString());
    	
    }
    
    /**
     * select *
     * where {
     *   { 
     *     predicate1 .
     *     predicate2 .
     *   } union {
     *     predicate3 .
     *     predicate4 . 
     *   }
     * }
     */
    public void testUnion() {
    	
    	final IGroupNode g1 = new JoinGroupNode();
    	g1.addChild(sp(1));
    	g1.addChild(sp(2));
    	
    	final IGroupNode g2 = new JoinGroupNode();
    	g2.addChild(sp(3));
    	g2.addChild(sp(4));
    	
    	final GraphPatternGroup union = new UnionNode();
    	union.addChild(g1);
    	union.addChild(g2);
    	
        final QueryRoot query = new QueryRoot(QueryType.SELECT);

        query.setWhereClause(union);

    	if (log.isInfoEnabled())
    		log.info("\n"+query.toString());
    	
    }
    
    /**
     * select *
     * where {
     *   predicate1 .
     *   filter2 .
     *   { 
     *     predicate3 .
     *     predicate4 .
     *   } union {
     *     predicate5 .
     *     predicate6 . 
     *   }
     * }
     */
    public void testNestedUnion() {
    	
    	final IGroupNode g1 = new JoinGroupNode();
    	g1.addChild(sp(3));
    	g1.addChild(sp(4));
    	
    	final IGroupNode g2 = new JoinGroupNode();
    	g2.addChild(sp(5));
    	g2.addChild(sp(6));
    	
    	final IGroupNode union = new UnionNode();
    	union.addChild(g1);
    	union.addChild(g2);
    	
    	final GraphPatternGroup root = new JoinGroupNode();
    	root.addChild(sp(1));
    	root.addChild(filter(2));
    	root.addChild(union);
    	
        final QueryRoot query = new QueryRoot(QueryType.SELECT);
        
        query.setWhereClause(root);

    	if (log.isInfoEnabled())
    		log.info("\n"+query.toString());
    	
    }
    
    public void testEverything() {
    	
    	final GraphPatternGroup<IGroupMemberNode> root = new JoinGroupNode();
    	
    	root.addChild(sp(1)).addChild(sp(2)).addChild(filter(3));
    	
    	final JoinGroupNode j1 = new JoinGroupNode(false);
    	j1.addChild(sp(4)).addChild(sp(5)).addChild(filter(6));
    	
    	final JoinGroupNode j2 = new JoinGroupNode(false);
    	j2.addChild(sp(7)).addChild(sp(8)).addChild(filter(9));
    	
    	final UnionNode u1 = new UnionNode();
    	u1.addChild(j1).addChild(j2);
    	root.addChild(u1);
    	
    	final JoinGroupNode j3 = new JoinGroupNode(true);
    	j3.addChild(sp(10)).addChild(sp(11)).addChild(filter(12));
    	root.addChild(j3);
    	
        final QueryRoot query = new QueryRoot(QueryType.SELECT);
        
        query.setWhereClause(root);

    	final ProjectionNode project = new ProjectionNode();
        project.setDistinct(true);
        project.addProjectionVar(new VarNode("s"));
        project.addProjectionVar(new VarNode("p"));
    	query.setProjection(project);
    	
    	final GroupByNode groupBy = new GroupByNode();
        groupBy.addExpr(new AssignmentNode(new VarNode("s"), new VarNode("s")));

        final HavingNode havingBy = new HavingNode();
        havingBy.addExpr(new LegacyTestValueExpressionNode(new CompareBOp(Var.var("x"),
                Var.var("y"), CompareOp.GT)));

    	final OrderByNode orderBy = new OrderByNode();
    	orderBy.addExpr(new OrderByExpr(new VarNode("s"), true));
    	orderBy.addExpr(new OrderByExpr(new VarNode("p"), false));
    	query.setOrderBy(orderBy);
    	
        query.setSlice(new SliceNode(10, 100));

    	if (log.isInfoEnabled())
    		log.info("\n"+query.toString());

	    /**
         * Now build up a subquery and add it to the query.
         * 
         * <pre>
         * SELECT ?y ?minName
         * WHERE {
         *   :alice :knows ?y .
         *   {
         *     SELECT ?y (MIN(?name) AS ?minName)
         *     WHERE {
         *       ?y :name ?name .
         *     } GROUP BY ?y
         *   }
         * }
         * </pre>
         */
    	final NamedSubqueriesNode subqueries = new NamedSubqueriesNode();
    	{
            
            final NamedSubqueryRoot subquery = new NamedSubqueryRoot(
                    QueryType.SELECT, "foo");
            
            final ProjectionNode projection1 = new ProjectionNode();
            projection1.addProjectionVar(new VarNode("y"));
            subquery.setProjection(projection1);

            final TermNode nameConstant = new ConstantNode(
                    TermId.mockIV(VTE.URI));
            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("y"),
                    nameConstant, new VarNode("name")));
            subquery.setWhereClause(whereClause);
            
            final GroupByNode groupBy1 = new GroupByNode();
            groupBy1.addGroupByVar(new VarNode("y"));
            subquery.setGroupBy(groupBy1);
    	    
            subqueries.add(subquery);
            
    	}

    	query.setNamedSubqueries(subqueries);

        // TODO Add INCLUDE operator paired to a named subquery result. 

        if (log.isInfoEnabled())
            log.info("\n"+query.toString());

    }
    
    public StatementPatternNode sp(final int id) {
        return new StatementPatternNode(new VarNode("s"), new VarNode("p"),
                new VarNode("o"));
    }
    
    public FilterNode filter(final int id) {
        return new FilterNode(new LegacyTestValueExpressionNode(new Filter(id)));
    }
    
    public Predicate pred(final int id) {
    	return new Predicate(BOp.NOARGS, 
			NV.asMap(new NV(BOp.Annotations.BOP_ID, id))
    	);
    }
    
    public IValueExpression ve(final int id) {
    	
    	return new Filter(id);
		
    }
    /**
     * @deprecated This was just for compatibility with SOp2ASTUtility. It is
     *             only used by the test suite now. 
     */
    @Deprecated
    private static final class LegacyTestValueExpressionNode extends ValueExpressionNode {
		private LegacyTestValueExpressionNode(IValueExpression<? extends IV> ve) {
			super(ve);
		}

		@Override
		public String toString(int i) {
			return toShortString();
		}
	}

	private static final class Filter extends XSDBooleanIVValueExpression {
    	
    	/**
         * 
         */
        private static final long serialVersionUID = 1L;

        public Filter(final int id) {
    		super(BOp.NOARGS, 
        			NV.asMap(
                            new NV(BOp.Annotations.BOP_ID, id)//
        			        ));
    	}
    	
		protected boolean accept(IBindingSet bs) {
			return true;
		}
		
    }
    
}
