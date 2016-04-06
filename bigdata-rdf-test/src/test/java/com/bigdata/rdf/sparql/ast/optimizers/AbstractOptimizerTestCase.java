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
package com.bigdata.rdf.sparql.ast.optimizers;


import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ModifiableBOpBase;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.IsBoundBOp;
import com.bigdata.rdf.internal.constraints.OrBOp;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.ExistsNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.GroupMemberNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.NotExistsNode;
import com.bigdata.rdf.sparql.ast.PathNode;
import com.bigdata.rdf.sparql.ast.PathNode.PathAlternative;
import com.bigdata.rdf.sparql.ast.PathNode.PathElt;
import com.bigdata.rdf.sparql.ast.PathNode.PathMod;
import com.bigdata.rdf.sparql.ast.PathNode.PathSequence;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.PropertyPathNode;
import com.bigdata.rdf.sparql.ast.PropertyPathUnionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.SliceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.explainhints.ExplainHints;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * A helper class that can make it easier to write {@link IASTOptimizer} tests.
 * 
 * @author jeremycarroll
 */
public abstract class AbstractOptimizerTestCase extends
		AbstractASTEvaluationTestCase {

	public interface Annotations extends
			com.bigdata.rdf.sparql.ast.GraphPatternGroup.Annotations,
			com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode.Annotations,
			com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.Annotations,
			com.bigdata.rdf.sparql.ast.StatementPatternNode.Annotations
			{
	}
	
	public interface ApplyAnnotation {
		public void apply(ASTBase target);
	}

	enum HelperFlag {
		ZERO_OR_ONE {
			@Override
			public void apply(ASTBase sp) {
				setPathMod(((ArbitraryLengthPathNode) sp), PathMod.ZERO_OR_ONE);
			}
		},
		
		ZERO_OR_MORE {
			@Override
			public void apply(ASTBase sp) {
				setPathMod(((ArbitraryLengthPathNode) sp), PathMod.ZERO_OR_MORE);
			}
			
		},
		
		ONE_OR_MORE {
			@Override
			public void apply(ASTBase sp) {
				setPathMod(((ArbitraryLengthPathNode) sp), PathMod.ONE_OR_MORE);
			}
			
		},
		DEFAULT_CONTEXTS {
			@Override
			public void apply(ASTBase sp) {
				((StatementPatternNode) sp).setScope(Scope.DEFAULT_CONTEXTS);
			}
		},
		NAMED_CONTEXTS {
			@Override
			public void apply(ASTBase sp) {
				((StatementPatternNode) sp).setScope(Scope.DEFAULT_CONTEXTS);
			}
		},
		OPTIONAL {
			@Override
			public void apply(ASTBase sp) {
				((ModifiableBOpBase) sp)
						.setProperty(Annotations.OPTIONAL, true);
			}
		},
		DISTINCT {
			@Override
			public void apply(ASTBase rslt) {
				((QueryBase) rslt).getProjection().setDistinct(true);
			}
		},
		REDUCED {
			@Override
			public void apply(ASTBase rslt) {
				((QueryBase) rslt).getProjection().setReduced(true);
			}
		},
		NOT_DISTINCT {
			@Override
			public void apply(ASTBase rslt) {
				((QueryBase) rslt).getProjection().setDistinct(false);
			}
		},
		NOT_REDUCED {
			@Override
			public void apply(ASTBase rslt) {
				((QueryBase) rslt).getProjection().setReduced(false);
			}
		},
        
        SUBGROUP_OF_ALP {
         @Override
         public void apply(ASTBase sp) {
            ((JoinGroupNode) sp).setSubgroupOfALPNode(true);
         }
      };

		/**
		 * 
		 * @param target
		 * @throws ClassCastException
		 *             If there is a mismatch between the flag and its usage.
		 */
		abstract public void apply(ASTBase target);

		private static void setPathMod(ArbitraryLengthPathNode alp, PathMod mod ) {
			alp.setProperty(Annotations.LOWER_BOUND, mod == PathMod.ONE_OR_MORE ? 1L : 0L);
			alp.setProperty(Annotations.UPPER_BOUND,mod == PathMod.ZERO_OR_ONE ? 1L : Long.MAX_VALUE);
			
		}
	};

	protected AbstractOptimizerTestCase(final String name) {
		super(name);
	}

	protected AbstractOptimizerTestCase() {
		super();
	}

	/**
	 * Return the {@link IASTOptimizer} to be evaluated by the {@link Helper}.
	 * This may be an {@link ASTOptimizerList} if you need to chain multiple
	 * optimizers together in order to evaluate their behavior.
	 */
    abstract IASTOptimizer newOptimizer() ;

	/**
	 * The purpose of this class is to make the tests look like the old
	 * comments. The first example
	 * {@link TestASTStaticJoinOptimizer#test_simpleOptional01A()} is based on
	 * the comments of
	 * {@link TestASTStaticJoinOptimizer#test_simpleOptional01()} and
	 * demonstrates that the comment is out of date.
	 * <p>
	 * NB: Given this goal, several Java naming conventions are ignored. e.g.
	 * methods whose names are ALLCAPS or the same as ClassNames
	 * <p>
	 * Also, note that the intent is that this class be used in
	 * anonymous subclasses with a single invocation of the {@link #test()} method,
	 * and the two fields {@link #given} and {@link #expected} initialized
	 * in the subclasses constructor (i.e. inside a second pair of braces).
	 * <p> 
	 * All of the protected members are wrappers around constructors,
	 * to allow the initialization of these two fields, to have a style
	 * much more like Prolog than Java.
	 * 
	 * @author jeremycarroll
	 */
	@SuppressWarnings("rawtypes")
	public abstract class Helper {

		/**
		 * Wrapper for the annotation property name-value.
		 */
		protected class StatementPatternProperty {
			private String annotation;
			private Object value;
			
			public StatementPatternProperty(String annotation, Object value) {
				this.annotation = annotation;
				this.value = value;
			}

			public String getAnnotation() {
				return annotation;
			}

			public Object getValue() {
				return value;
			}
		}
		
		/**
		 * The given AST is the input to the {@link IASTOptimizer}.
		 */
		protected QueryRoot given;
		
      /**
		 * The expected AST output from the {@link IASTOptimizer}.
		 */
		protected QueryRoot expected;
		
		/**
		 * Field for construction of arbitrary AST nodes.
		 */
		protected ASTBase tmp;
		
		/**
		 * Variables
		 */
		protected final String w = "w", x = "x", y = "y", z = "z", s = "s",
				p = "p", o = "o";
		
		/**
		 * Constants ...
		 */
		protected final IV a = iv("a"), b = iv("b"), c = iv("c"), d = iv("d"),
				e = iv("e"), f = iv("f"), g = iv("g"), h = iv("h");
		
		private VarNode rightVar;
		private VarNode leftVar;
		int varCount = 0;
		
		private GlobalAnnotations globals = new GlobalAnnotations(getName(), ITx.READ_COMMITTED);

		/**
		 * Execute the test comparing the rewrite of the {@link #given} AST by
		 * the {@link IASTOptimizer} with the {@link #expected} AST.
		 */
		public void test() {

			final IASTOptimizer rewriter = newOptimizer();

			final AST2BOpContext context = getAST2BOpContext(given);

			final IQueryNode actual = rewriter.optimize(context,
			      new QueryNodeWithBindingSet(given, new IBindingSet[] {})).
			      getQueryNode();

			assertSameAST(expected, actual);
			
		}

		AST2BOpContext getAST2BOpContext(QueryRoot given) {
			return new AST2BOpContext(new ASTContainer(given), store);
		}
		
		/**
		 * Execute the test comparing the rewrite of the {@link #given} AST by
		 * the {@link IASTOptimizer} with the {@link #expected} AST. Thereby,
		 * explain hints attached to the computed AST are ignored when comparing
		 * the ASTs.
		 */
		public void testWhileIgnoringExplainHints() {

			final IASTOptimizer rewriter = newOptimizer();

			final AST2BOpContext context = getAST2BOpContext(given);

			final IQueryNode actual = rewriter.optimize(context,
			      new QueryNodeWithBindingSet(given, new IBindingSet[] {})).
			      getQueryNode();

			// remove explain hints, to ease comparison
			ExplainHints.removeExplainHintAnnotationsFromBOp(actual);
			
			assertSameAST(expected, actual);
		}
		

		private IV iv(final String id) {
			
			return makeIV(new URIImpl("http://example/" + id));

		}
		
		protected VarNode wildcard() {
			return varNode("*");
		}

		/**
		 * Create a top-level SELECT query.
		 * 
		 * @param varNodes
		 *            The projected variables.
		 * @param namedSubQuery
		 *            A named subquery that is declared to that top-level query.
		 * @param where
		 *            The WHERE clause of the top-level query.
		 * @param flags
		 *            The flags to be applied to the resulting AST.
		 */
		protected QueryRoot select(final VarNode[] varNodes,
				final NamedSubqueryRoot namedSubQuery,
				final JoinGroupNode where, final HelperFlag... flags) {

			final QueryRoot rslt = select(varNodes, where, flags);

			rslt.getNamedSubqueriesNotNull().add(namedSubQuery);

			return rslt;
			
		}

		/**
		 * 
		 * @param varNodes
		 *            The variables that will appear in the projection.
		 * @param where
		 *            The top-level WHERE clause.
		 * @param flags
		 *            Zero or more flags that are applied to the operations.
		 *            
		 * @return The {@link QueryRoot}.
		 */
		protected QueryRoot select(final VarNode[] varNodes,
				final JoinGroupNode where, final HelperFlag... flags) {

//			// Create QueryRoot.
//			final QueryRoot select = new QueryRoot(QueryType.SELECT);

//			// Setup projection.
//			final ProjectionNode projection = new ProjectionNode();
//			for (VarNode varNode : varNodes)
//				projection.addProjectionVar(varNode);

			return select(projection(varNodes), where, flags);
		
//			// Set Projection and Where clause on QueryRoot.
//			select.setProjection(projection);
//			select.setWhereClause(where);
//			
//			// Apply helper flags.
//			for (HelperFlag flag : flags)
//				flag.apply(select);
//			
//			return select;
			
		}

		/**
		 * Return a PROJECTION node. 
		 * 
		 * @param varNodes
		 *            The variables that will appear in the projection.
		 */
		protected ProjectionNode projection(final VarNode... varNodes) {

			// Setup projection.
			final ProjectionNode projection = new ProjectionNode();
			for (VarNode varNode : varNodes)
				projection.addProjectionVar(varNode);

			return projection;
			
		}
		
		/**
		 * Return a PROJECTION node. 
		 * 
		 * @param assignmentNodes
		 *            The BIND()s that will appear in the projection.
		 */
		protected ProjectionNode projection(final AssignmentNode... assignmentNodes) {

			// Setup projection.
			final ProjectionNode projection = new ProjectionNode();
			for (AssignmentNode varNode : assignmentNodes)
				projection.addProjectionExpression(varNode);

			return projection;
			
		}
		
		/**
		 * 
		 * @param projection
		 *            The projection.
		 * @param where
		 *            The top-level WHERE clause.
		 * @param flags
		 *            Zero or more flags that are applied to the operations.
		 *            
		 * @return The {@link QueryRoot}.
		 */
		protected QueryRoot select(final ProjectionNode projection,
				final JoinGroupNode where, final HelperFlag... flags) {
			final QueryRoot select = new QueryRoot(QueryType.SELECT);
			return select(select, projection, where, flags);
		}

		protected QueryRoot select(final VarNode varNode,
				final NamedSubqueryRoot namedSubQuery,
				final JoinGroupNode where, final HelperFlag... flags) {
		
			return select(new VarNode[] { varNode }, namedSubQuery, where,
					flags);

		}

		protected QueryRoot select(final VarNode varNode,
				final JoinGroupNode where, final HelperFlag... flags) {

			return select(new VarNode[] { varNode }, where, flags);
			
		}
		
		protected SubqueryRoot selectSubQuery(final ProjectionNode projection,
				final JoinGroupNode where, final HelperFlag... flags) {
			final SubqueryRoot rslt = new SubqueryRoot(QueryType.SELECT);
			return select(rslt, projection, where, flags);
		}
		
		private <T extends QueryBase> T select(T select, final ProjectionNode projection,
				final JoinGroupNode where, final HelperFlag... flags) {
			assert projection != null;
			
			// Set Projection and Where clause on QueryRoot.
			select.setProjection(projection);
			select.setWhereClause(where);
			
			// Apply helper flags.
			for (HelperFlag flag : flags)
				flag.apply(select);
			
			return select;
		}

		/**
		 * Return an ASK subquery.
		 * 
		 * @param varNode
		 *            The "ASK" variable. See
		 *            {@link SubqueryRoot.Annotations#ASK_VAR}.
		 * @param where
		 *            The WHERE clause.
		 *            
		 * @return the ASK subquery.
		 */
		protected SubqueryRoot ask(final VarNode varNode,
				final JoinGroupNode where) {

			final SubqueryRoot rslt = new SubqueryRoot(QueryType.ASK);
            
			final ProjectionNode projection = new ProjectionNode();
            varNode.setAnonymous(true);
            rslt.setProjection(projection);
            projection.addProjectionExpression(new AssignmentNode(varNode, varNode));
            
            rslt.setWhereClause(where);
            
            rslt.setAskVar(toValueExpression(varNode));
            
			return rslt;
			
		}

		/**
		 * Return a named subquery.
		 * 
		 * @param name
		 *            The name associated with the named subquery result.
		 * @param varNode
		 *            The projected variable.
		 * @param where
		 *            The where clause.
		 *            
		 * @return The named subquyery.
		 */
		protected NamedSubqueryRoot namedSubQuery(final String name,
				final VarNode varNode, final JoinGroupNode where,
				ApplyAnnotation ... annotations) {
			
			final NamedSubqueryRoot namedSubquery = new NamedSubqueryRoot(
					QueryType.SELECT, name);

			final ProjectionNode projection = new ProjectionNode();
			namedSubquery.setProjection(projection);
			projection.addProjectionExpression(new AssignmentNode(varNode,
					new VarNode(varNode)));
			
			namedSubquery.setWhereClause(where);
			
			return applyAnnotations(namedSubquery, annotations);
			
		}

		protected GroupMemberNodeBase namedSubQueryInclude(
				final String name, ApplyAnnotation ... annotations) {
			
			return applyAnnotations(new NamedSubqueryInclude(name), annotations);
			
		}
		
		protected <T extends ASTBase> T applyAnnotations(T target, ApplyAnnotation ... annotations) {
			for (ApplyAnnotation annotation: annotations) {
				annotation.apply(target);
			}
			return target;
		}
		
		protected VarNode leftVar() {
			leftVar = newAlppVar("-tVarLeft-",leftVar);
			return leftVar;
		}
		protected VarNode rightVar() {
			rightVar = newAlppVar("-tVarRight-",rightVar);
			return rightVar;
		}

		private VarNode newAlppVar(final String prefix, VarNode v) {
			if (v != null) {
				return v;
			}
			v = varNode(prefix+varCount++);
			v.setAnonymous(true);
			return v ;
				
		}

		protected ArbitraryLengthPathNode arbitartyLengthPropertyPath(TermNode left, TermNode right,
				HelperFlag card, JoinGroupNode joinGroupNode) {
			assert leftVar != null;
			assert rightVar != null;
			ArbitraryLengthPathNode rslt = new ArbitraryLengthPathNode(left, right, leftVar, rightVar, PathMod.ONE_OR_MORE);
			card.apply(rslt);
			rslt.setArg(0, joinGroupNode);
			leftVar = null;
			rightVar = null;
			return rslt;
		}


		protected VarNode[] varNodes(String... names) {
			VarNode rslt[] = new VarNode[names.length];
			for (int i = 0; i < names.length; i++)
				rslt[i] = varNode(names[i]);
			return rslt;
		}

		protected VarNode varNode(String varName) {
			return new VarNode(varName);
		}

		protected TermNode constantNode(IV iv) {
			return new ConstantNode(iv);
		}
		protected TermNode constantNode(String c) {
			return new ConstantNode(iv(c));
		}

		protected PropertyPathNode propertyPathNode(final TermNode s, String pattern, final TermNode o) {
			return new PropertyPathNode(s, pathNode(pattern), o);
		}
		
		/**
		 * This method is only implemented in part. The issue is what patterns are supported.
		 * The desired support as of the intended contract of this method is that pattern
		 * can be any sparql property path expression without any whitespace, and where
		 * every property in the path is reduced to a single letter a-z.
		 * e.g. "(a/b?/c)+|a|c*"
		 * 
		 * The current support is that parenthesis, alternatives and negated properties  are not supported.
		 * 
		 * @param pattern
		 * @return
		 */
		protected PathNode pathNode(String pattern) {
			final String seq[] = pattern.split("/");
			final PathElt elements[] = new PathElt[seq.length];
			PathMod mod = null;
			for (int i =0;i<seq.length;i++) {
				final String s = seq[i];
				boolean inverse = s.charAt(0)=='^';
				switch (s.charAt(s.length()-1)) {
				case '*':
					mod = PathMod.ZERO_OR_MORE;
					break;
				case '+':
					mod = PathMod.ONE_OR_MORE;
					break;
				case '?':
					mod = PathMod.ZERO_OR_ONE;
					break;
				}
				String c = s.substring(inverse?1:0,s.length() - (mod!=null?1:0));
				elements[i] = new PathElt(constantNode(c),inverse,mod);
				
			}
			return new PathNode(new PathAlternative(new PathSequence(elements)));
		}

		protected StatementPatternProperty property(String name, Object value) {
			return new StatementPatternProperty(name, value);
		}

		/**
		 * Create a statement pattern node. The additional arguments after the s, p, o,
		 * are:
		 * <ol>
		 * <li>A context node: must be first (i.e. fourth)</li>
		 * <li>integer cardinality</li>
		 * <li>HelperFlag's</li>
		 * </ol>
		 * @param s
		 * @param p
		 * @param o
		 * @param more
		 * @return
		 */
		protected StatementPatternNode statementPatternNode(//
				final TermNode s,
				final TermNode p, 
				final TermNode o, 
				final Object ... more) {
			
			final StatementPatternNode rslt = newStatementPatternNode(s, p, o);

			if (more.length > 0) {
				int i = 0;
				if (more[0] instanceof TermNode) {
					rslt.setC((TermNode) more[0]);
					i = 1;
				}
				for (; i < more.length; i++) {
					if (more[i] instanceof Integer) {
						rslt.setProperty(Annotations.ESTIMATED_CARDINALITY,
								Long.valueOf((Integer) more[i]));
					} else if(more[i] instanceof StatementPatternProperty) {
						StatementPatternProperty prop = (StatementPatternProperty) more[i];
						rslt.setProperty(prop.annotation, prop.value);
					} else {
						final HelperFlag flag = (HelperFlag) more[i];
						flag.apply(rslt);
					}
				}
			}
			return rslt;
		}

		@SuppressWarnings("unchecked")
		private <E extends IGroupMemberNode, T extends GraphPatternGroup<E>> T initGraphPatternGroup(
				T rslt, Object... statements) {
			for (Object mem : statements) {
				if (mem instanceof IGroupMemberNode) {
					rslt.addChild((E) mem);
				} else {
					((HelperFlag) mem).apply(rslt);
				}
			}
			return rslt;
		}

		/**
		 * Return a graph join group.
		 * 
		 * @param context
		 *            The graph (named graph variable or IRI).
		 * @param statements
		 *            The children (including any optional {@link HelperFlag}s).
		 *            
		 * @return The group join group node.
		 */
		protected JoinGroupNode joinGroupNode(final TermNode context,
				final Object... statements) {

			final JoinGroupNode rslt = joinGroupNode(statements);

			rslt.setContext(context);

			return rslt;

		}

		/**
		 * Wrap the arguments in a {@link JoinGroupNode} and apply any
		 * inter-mixed {@link HelperFlag}s.
		 * 
		 * @param statements
		 *            The arguments (group group members or {@link HelperFlag}s)
		 * 
		 * @return The {@link JoinGroupNode}.
		 */
		protected JoinGroupNode joinGroupNode(final Object... statements) {
		
			return initGraphPatternGroup(new JoinGroupNode(), statements);
			
		}

		/**
		 * Wrap the triple patterns in a WHERE clause (aka join group).
		 * 
		 * @param statements
		 *            The triple patterns.
		 *            
		 * @return The {@link JoinGroupNode}
		 */
		protected JoinGroupNode where(final IGroupMemberNode... statements) {

			return joinGroupNode((Object[]) statements);
			
		}

		protected PropertyPathUnionNode propertyPathUnionNode(Object... statements) {
			return initGraphPatternGroup(new PropertyPathUnionNode(),
					statements);
		}

		protected UnionNode unionNode(Object... statements) {
			
			return initGraphPatternGroup(new UnionNode(), statements);

		}

		protected FunctionNode bound(final VarNode varNode) {
			
			final FunctionNode rslt = new FunctionNode(FunctionRegistry.BOUND,
            		null,
                    new ValueExpressionNode[] {
                     varNode
                    } );

			rslt.setValueExpression(new IsBoundBOp(varNode.getValueExpression()));
			
			return rslt;
			
		}

		protected FunctionNode knownUnbound(final VarNode varNode) {
            
			final VarNode vv = varNode("-unbound-var-"+varNode.getValueExpression().getName()+"-0");
			
			final FunctionNode rslt = new FunctionNode(FunctionRegistry.BOUND, null, vv);
			
			rslt.setValueExpression(new IsBoundBOp(vv.getValueExpression()));

			return rslt;
			
		}

		protected FilterNode filter(final IValueExpressionNode f) {

			return new FilterNode(f);

		}

		/**
		 * Return a {@link FunctionNode}
		 * 
		 * @param uri
		 *            the function URI. see {@link FunctionRegistry}
		 * @param args
		 *            the arguments to the function.
		 * @return
		 */
		// * @param scalarValues
		// * One or more scalar values that are passed to the function
		protected IValueExpressionNode functionNode(//
				final String uri,//
				final ValueExpressionNode... args//
				) {

			return functionNode(new URIImpl(uri), args);

		}
		
		/**
		 * Return a {@link FunctionNode}
		 * 
		 * @param uri
		 *            the function URI. see {@link FunctionRegistry}
		 * @param args
		 *            the arguments to the function.
		 * @return
		 */
		// * @param scalarValues
		// * One or more scalar values that are passed to the function
		protected IValueExpressionNode functionNode(//
				final URI uri,//
				final ValueExpressionNode... args//
				) {

			return new FunctionNode(uri, null, args);

		}

		protected ServiceNode service(//
				final TermNode serviceRef,
				final GraphPatternGroup<IGroupMemberNode> groupNode//
				) {

			return new ServiceNode(serviceRef, groupNode);

		}

		/**
		 * <code>BIND(expression AS variable)</code>
		 * 
		 * @param valueNode
		 *            The expression
		 * @param varNode
		 *            The variable.
		 */
		protected AssignmentNode bind(final IValueExpressionNode valueNode,
				final VarNode varNode) {
			
			return new AssignmentNode(varNode, valueNode);
			
		}

		/**
		 * Logical OR of two value expressions.
		 */
		protected FunctionNode or(final ValueExpressionNode v1,
				final ValueExpressionNode v2) {

			final FunctionNode rslt = FunctionNode.OR(v1, v2);

			rslt.setValueExpression(new OrBOp(//
					v1.getValueExpression(), //
					v2.getValueExpression()));

			return rslt;
		
		}
				
		protected ExistsNode exists(VarNode v, GraphPatternGroup<IGroupMemberNode> jg) {
			v.setAnonymous(true);
			ExistsNode existsNode = new ExistsNode(v, jg);
			existsNode.setValueExpression(toValueExpression(v));
			return existsNode;
		}

		protected IVariable<? extends IV> toValueExpression(VarNode v) {
			return (IVariable<? extends IV>) AST2BOpUtility.toVE(getBOpContext(), globals, v);
		}

		protected IConstant<? extends IV> toValueExpression(ConstantNode v) {
			return (IConstant<? extends IV>) AST2BOpUtility.toVE(getBOpContext(), globals, v);
		}
		
		private IValueExpression<? extends IV> toValueExpression(FunctionNode n) {
			return AST2BOpUtility.toVE(getBOpContext(), globals, n);
		}
		
		protected NotExistsNode notExists(VarNode v, GraphPatternGroup<IGroupMemberNode> jg) {
			return new NotExistsNode(v, jg);
		}

		protected ASTBase getTmp() {
         return tmp;
        }
		protected ApplyAnnotation joinOn(final VarNode ... joinVars) {
			return new ApplyAnnotation() {
				@Override
				public void apply(ASTBase target) {
			        target.setProperty(Annotations.JOIN_VARS, joinVars);
				}};
			
		}
		protected ApplyAnnotation dependsOn(final String ... dependsOn) {
			return new ApplyAnnotation() {
				@Override
				public void apply(ASTBase target) {
					((NamedSubqueryRoot)target).setDependsOn(dependsOn);
				}};
			
		}
		protected ApplyAnnotation slice(final long offset, final long limit) {
			return new ApplyAnnotation() {
				@Override
				public void apply(ASTBase target) {
					((QueryBase)target).setSlice(new SliceNode(offset, limit));
				}};
			
		}
		
	}

	protected static final class ASTPropertyPathOptimizerInTest extends ASTPropertyPathOptimizer {
			private int counter = 0;
	
			@Override
			protected VarNode anonVar(final String anon) {
			    VarNode v = new VarNode(anon+counter++);
			    v.setAnonymous(true);
			    return v;
			}
		}

	protected StatementPatternNode newStatementPatternNode(final TermNode s,
			final TermNode p, final TermNode o) {

		return newStatementPatternNode(s, p, o, -1, false);

	}

	protected StatementPatternNode newStatementPatternNode(final TermNode s,
			final TermNode p, final TermNode o, final long cardinality) {

		return newStatementPatternNode(s, p, o, cardinality, false);

	}

	/**
	 * Return a new triple pattern.
	 * 
	 * @param s
	 *            The subject.
	 * @param p
	 *            The predicate.
	 * @param o
	 *            The object.
	 * @param cardinality
	 *            The estimated cardinality -or- <code>-1</code> to NOT
	 *            associate cardinality with the triple pattern.
	 * @param optional
	 *            <code>true</code> iff the triple pattern should be marked as a
	 *            simple OPTIONAL.
	 */
    protected StatementPatternNode newStatementPatternNode(
            final TermNode s, final TermNode p, final TermNode o, 
            final long cardinality, final boolean optional) {
        
        final StatementPatternNode sp = new StatementPatternNode(s, p, o);
        
        if (cardinality != -1) {

        	sp.setProperty(Annotations.ESTIMATED_CARDINALITY, cardinality);
        
        }
        
        if (optional) {
            
            sp.setOptional(true);
            
        }
        
        return sp;
        
    }

}
