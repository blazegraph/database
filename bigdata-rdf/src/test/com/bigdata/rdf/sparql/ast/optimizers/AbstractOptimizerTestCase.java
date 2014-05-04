/**

Copyright (C) SYSTAP, LLC 2006-2013.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.optimizers;


import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
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
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.PathNode.*;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.PropertyPathNode;
import com.bigdata.rdf.sparql.ast.PropertyPathUnionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.spo.SPOKeyOrder;

public abstract class AbstractOptimizerTestCase extends AbstractASTEvaluationTestCase {

	public interface Annotations extends
			com.bigdata.rdf.sparql.ast.GraphPatternGroup.Annotations,
			com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode.Annotations,
			com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.Annotations
			{
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

	/**
	 * The purpose of this class is to make the tests look like the old
	 * comments. The first example
	 * {@link TestASTStaticJoinOptimizer#test_simpleOptional01A()} is based on
	 * the comments of
	 * {@link TestASTStaticJoinOptimizer#test_simpleOptional01()} and
	 * demonstrates that the comment is out of date.
	 * 
	 * NB: Given this goal, several Java naming conventions are ignored. e.g.
	 * methods whose names are ALLCAPS or the same as ClassNames
	 * 
	 * Also, note that the intent is that this class be used in
	 * anonymous subclasses with a single invocation of the {@link #test()} method,
	 * and the two fields {@link #given} and {@link #expected} initialized
	 * in the subclasses constructor (i.e. inside a second pair of braces).
	 * 
	 * All of the protected members are wrappers around constructors,
	 * to allow the initialization of these two fields, to have a style
	 * much more like Prolog than Java.
	 * 
	 * @author jeremycarroll
	 * 
	 */
	@SuppressWarnings("rawtypes")
	public abstract class Helper {
		protected QueryRoot given, expected;
		/**
		 * Variables
		 */
		protected final String w = "w", x = "x", y = "y", z = "z";
		/**
		 * Constants ...
		 */
		protected final IV a = iv("a"), b = iv("b"), c = iv("c"), d = iv("d"),
				e = iv("e"), f = iv("f"), g = iv("g"), h = iv("h");
		private VarNode rightVar;
		private VarNode leftVar;
		int varCount = 0;
		private GlobalAnnotations globals = new GlobalAnnotations(getName(), ITx.READ_COMMITTED);

		private IV iv(String id) {
			return makeIV(new URIImpl("http://example/" + id));
		}

		protected QueryRoot select(VarNode[] varNodes,
				NamedSubqueryRoot namedSubQuery, JoinGroupNode where,
				HelperFlag... flags) {
			QueryRoot rslt = select(varNodes, where, flags);
			rslt.getNamedSubqueriesNotNull().add(namedSubQuery);
			return rslt;
		}

		protected QueryRoot select(VarNode[] varNodes, JoinGroupNode where,
				HelperFlag... flags) {

			QueryRoot select = new QueryRoot(QueryType.SELECT);
			final ProjectionNode projection = new ProjectionNode();
			for (VarNode varNode : varNodes)
				projection.addProjectionVar(varNode);

			select.setProjection(projection);
			select.setWhereClause(where);
			for (HelperFlag flag : flags)
				flag.apply(select);
			return select;
		}

		protected QueryRoot select(VarNode varNode,
				NamedSubqueryRoot namedSubQuery, JoinGroupNode where,
				HelperFlag... flags) {
			return select(new VarNode[] { varNode }, namedSubQuery, where,
					flags);
		}

		protected QueryRoot select(VarNode varNode, JoinGroupNode where,
				HelperFlag... flags) {
			return select(new VarNode[] { varNode }, where, flags);
		}
		
		protected SubqueryRoot ask(VarNode varNode, JoinGroupNode where) {

			final SubqueryRoot rslt = new SubqueryRoot(QueryType.ASK);
            final ProjectionNode projection = new ProjectionNode();
            varNode.setAnonymous(true);
            rslt.setProjection(projection);
            projection.addProjectionExpression(new AssignmentNode(varNode, varNode));
            rslt.setWhereClause(where);
            rslt.setAskVar(toValueExpression(varNode));
            
			return rslt;
		}

		protected NamedSubqueryRoot namedSubQuery(String name, VarNode varNode,
				JoinGroupNode where) {
			final NamedSubqueryRoot namedSubquery = new NamedSubqueryRoot(
					QueryType.SELECT, name);
			final ProjectionNode projection = new ProjectionNode();
			namedSubquery.setProjection(projection);
			projection.addProjectionExpression(new AssignmentNode(varNode,
					new VarNode(varNode)));
			namedSubquery.setWhereClause(where);
			return namedSubquery;
		}

		protected GroupMemberNodeBase namedSubQueryInclude(String name) {
			return new NamedSubqueryInclude(name);
		}
		
		protected VarNode leftVar() {
			leftVar = newAlppVar("-tVarLeft-",leftVar);
			return leftVar;
		}
		protected VarNode rightVar() {
			rightVar = newAlppVar("-tVarRight-",rightVar);
			return rightVar;
		}

		private VarNode newAlppVar(String prefix,VarNode v) {
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
		protected StatementPatternNode statementPatternNode(TermNode s,
				TermNode p, TermNode o, Object ... more) {
			StatementPatternNode rslt = newStatementPatternNode(s, p, o);
			if (more.length>0) {
				int i = 0;
				if (more[0] instanceof TermNode) {
					rslt.setC((TermNode)more[0]);
					i = 1;
				}
				for (;i<more.length;i++) {
					if (more[i] instanceof Integer) {
			            rslt.setProperty(Annotations.ESTIMATED_CARDINALITY, Long.valueOf((Integer)more[i]));
					} else {
						HelperFlag flag = (HelperFlag)more[i];
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


		protected JoinGroupNode joinGroupNode(TermNode context,Object... statements) {
			JoinGroupNode rslt = joinGroupNode(statements);
			rslt.setContext(context);
			return rslt;
		}
		protected JoinGroupNode joinGroupNode(Object... statements) {
			return initGraphPatternGroup(new JoinGroupNode(), statements);
		}

		protected PropertyPathUnionNode propertyPathUnionNode(Object... statements) {
			return initGraphPatternGroup(new PropertyPathUnionNode(),
					statements);
		}

		protected UnionNode unionNode(Object... statements) {
			return initGraphPatternGroup(new UnionNode(), statements);

		}

		protected JoinGroupNode where(GroupMemberNodeBase... statements) {
			return joinGroupNode((Object[]) statements);
		}

		public void test() {
			final IASTOptimizer rewriter = newOptimizer();

			final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
					given), store);

			final IQueryNode actual = rewriter.optimize(context, given,
					new IBindingSet[] {});

			assertSameAST(expected, actual);
		}

		protected FunctionNode bound(VarNode varNode) {
            FunctionNode rslt = new FunctionNode(FunctionRegistry.BOUND,
            		null,
                    new ValueExpressionNode[] {
                     varNode
                    } );
			rslt.setValueExpression(new IsBoundBOp(varNode.getValueExpression()));
			return rslt;
		}
		protected FunctionNode knownUnbound(VarNode varNode) {
            final VarNode vv = varNode("-unbound-var-"+varNode.getValueExpression().getName()+"-0");
			FunctionNode rslt = new FunctionNode(FunctionRegistry.BOUND, null, vv);
			rslt.setValueExpression(new IsBoundBOp(vv.getValueExpression()));
			return rslt;
		}

		protected FilterNode filter(IValueExpressionNode f) {
			return new FilterNode(f);
		}

		protected IValueExpressionNode functionNode(String uri, ValueExpressionNode ... args) {
			return new FunctionNode(new URIImpl(uri), null, args);
		}

		protected ServiceNode service(TermNode serviceRef, GraphPatternGroup<IGroupMemberNode> groupNode) {
			return new ServiceNode(serviceRef, groupNode);
		}

		protected AssignmentNode bind(IValueExpressionNode valueNode, VarNode varNode) {
			return new AssignmentNode(varNode, valueNode);
		}
		
		protected FunctionNode or(ValueExpressionNode v1, ValueExpressionNode v2) {
			
			FunctionNode rslt = FunctionNode.OR(v1, v2);
			rslt.setValueExpression(new OrBOp(v1.getValueExpression(),v2.getValueExpression()));
			return rslt;
		}
		
		protected ExistsNode exists(VarNode v, GraphPatternGroup<IGroupMemberNode> jg) {
			v.setAnonymous(true);
			ExistsNode existsNode = new ExistsNode(v, jg);
			existsNode.setValueExpression(toValueExpression(v));
			return existsNode;
		}

		private IVariable<? extends IV> toValueExpression(VarNode v) {
			return (IVariable<? extends IV>) AST2BOpUtility.toVE(globals, v);
		}

		private IValueExpression<? extends IV> toValueExpression(FunctionNode n) {
			return AST2BOpUtility.toVE(globals, n);
		}
		protected NotExistsNode notExists(VarNode v, GraphPatternGroup<IGroupMemberNode> jg) {
			return new NotExistsNode(v, jg);
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

	public AbstractOptimizerTestCase(String name) {
		super(name);
	}

	public AbstractOptimizerTestCase() {
		super();
	}

    protected StatementPatternNode newStatementPatternNode(final TermNode s, final TermNode p, final TermNode o) {
        return newStatementPatternNode(s, p, o, -1, false);
    }
    protected StatementPatternNode newStatementPatternNode(
            final TermNode s, final TermNode p, final TermNode o, 
            final long cardinality) {
        return newStatementPatternNode(s, p, o, cardinality, false);
    }

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

	abstract IASTOptimizer newOptimizer() ;
}
