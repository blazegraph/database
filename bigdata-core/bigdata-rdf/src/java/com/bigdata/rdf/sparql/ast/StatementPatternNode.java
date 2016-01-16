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
package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.internal.constraints.RangeBOp;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpBase;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpJoins;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.optimizers.ASTGraphGroupOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTRangeConstraintOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSimpleOptionalOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.StaticOptimizer;
import com.bigdata.rdf.spo.DistinctTermAdvancer;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOAccessPath;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.IKeyOrder;

/**
 * A node in the AST representing a statement pattern.
 * <p>
 * Note: The annotations on the class are mostly interpreted by the
 * toPredicate() method in {@link AST2BOpUtility} and by the logic in
 * {@link AST2BOpJoins} which handles the default and named graph access
 * patterns.
 * <p>
 * Note: If a variable is bound, then we bind that slot of the predicate. If a
 * variable can take some enumerated set of values, then we use an
 * {@link #INLINE} access path to model that "IN" constraint. If the value for a
 * variable must lie within some key range, then we handle that case using
 * {@link RangeBOp}. If we have no information about a variable, then we just
 * leave the variable unbound.
 */
public class StatementPatternNode extends
        GroupMemberNodeBase<StatementPatternNode> implements
        IJoinNode, IStatementContainer, IReorderableNode {

    private static final long serialVersionUID = 1L;

    public interface Annotations extends GroupMemberNodeBase.Annotations,
            IJoinNode.Annotations {

        /**
         * The {@link Scope} (required).
         * 
         * @see ASTGraphGroupOptimizer
         */
        String SCOPE = "scope";
        
        /**
         * Boolean flag indicates that the distinct solutions for the statement
         * pattern are required ({@value #DEFAULT_DISTINCT}).
         * <p>
         * Note: This is a hint that the {@link DistinctTermAdvancer} should be
         * used to visit the distinct {@link ISPO}s having a common prefix. This
         * is used for <code>GRAPH ?g {}</code>, which evaluates to all of the
         * named graphs in the database (if the named graphs were not explicitly
         * specified).
         * <p>
         * Note: For only partly historical reasons, this is not used to mark
         * default graph access. A default graph access path strips the context
         * and then applies a DISTINCT filter to the resulting triples.
         */
        String DISTINCT = "distinct";
        
        boolean DEFAULT_DISTINCT = false;
        
        /**
         * The existence of at least one solution will be verified otherwise the
         * solution will be failed. This turns into an iterator with a limit of
         * ONE (1) on the {@link SPOAccessPath}.
         * <p>
         * Note: This is used in combination with a join against an inline
         * access path for the named graphs. The "exists" statement pattern MUST
         * run <em>after</em> the access path which produces the variety since
         * it will be used to constrain that as-bound variety. This the join
         * order in query plan must look like:
         * 
         * <pre>
         * (_,_,_,?g)[@INLINE,@IN(g,namedGraphs)] x (_,_,_,?g)[@EXISTS]
         * </pre>
         * 
         * rather than
         * 
         * <pre>
         * (_,_,_,?g)[@EXISTS] x (_,_,_,?g)[@INLINE,@IN(g,namedGraphs)]
         * </pre>
         * 
         * as the latter will find only one solution for <code>?g</code>.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/429
         *      (Optimization for GRAPH uri {} and GRAPH ?foo {})
         */
        String EXISTS = "exists";
        
        /**
         * The data for this access path is inline. The value of the attribute
         * is the column projection / solution set reference.
         * 
         * TODO A column projection is more efficient when we are handling
         * things like the named graphs or constraining the subquery for an
         * optional with multiple predicates. That column projection can be
         * modeled as <code>IN(var,values)</code>. The IN filter could be
         * attached to {@link #FILTERS} or it could be the value of this
         * attribute.
         * <p>
         * We also have use cases for inline solution set access paths for use
         * with the samples materialized by the RTO. Those should be an
         * {@link HTree} and the data should be modeled as {@link ISolution}s.
         * (Note that some vertices may correspond to "bop fragment" joins, in
         * which case the can not be modeled as {@link ISPO}s.)
         * <p>
         * Both the column projection (IN) and the inline solution set (HTree)
         * are simpler access paths. They only support element visitation, a
         * full scan of the access path (this is the same as saying that there
         * are no join variables), or probing to find all solutions which join
         * on some join variable(s). This is in contrast to the
         * {@link SPOAccessPath}, which supports key-range constraints (prefix)
         * and range constraints (prefix with key range on a data type value).
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/429
         *      (Optimization for GRAPH uri {} and GRAPH ?foo {})
         */
        String INLINE = "inline";
        
        /**
         * An optional attribute whose value is an {@link RangeBOp} which models
         * the key-range constraint on the access path. The {@link RangeBOp} is
         * used when there are filters which impose a GT/GTE and/or LT/LTE
         * restriction on the values which a variable may take on for that
         * access path.
         * 
         * TODO We should also handle datatype constraints on a variable here.
         * For example, if a variable is known to be numeric, or known to be
         * xsd:int, then we can immediately reject any bindings which would
         * violate that type constraint. To do this right, we need to notice
         * those type constraints and propagate them backwards in the plan so we
         * can reject bindings as early as possible. (In fact, we can also do a
         * range constraint which spans each of the datatypes which the variable
         * could take on. Datatype constraints and value range constraints are
         * very much related. The datatype constraint is effectively a value
         * range constraint allowing the entire value space for that datatype.
         * Likewise, a value range constraint must be applied across the UNION
         * of the allowable ground datatypes for the variable.)
         * 
         * @see ASTRangeConstraintOptimizer
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/238 (lift range
         *      constraints onto AP)
         */
        String RANGE = "range";
        
        /**
		 * An optional annotation whose value is the variable which will be
		 * bound to the statement identifier for the matched statement patterns.
		 * The statement identifier is always formed from the subject, predicate
		 * and object (the triple). The context is NOT represented in the
		 * statement identifier. This keeps the semantics consistent with RDF
		 * reification.
		 * 
		 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/526">
		 *      Reification Done Right</a>
		 */
        String SID = "sid";

		/**
		 * An optional annotation whose value is a variable which will become
		 * bound to the fast range count of the associated triple pattern.
		 * 
		 * @see <a href="http://trac.blazegraph.com/ticket/1037" > SELECT
		 *      COUNT(...) (DISTINCT|REDUCED) {single-triple-pattern} is slow.
		 *      </a>
		 */
        String FAST_RANGE_COUNT_VAR = "fastRangeCountVar";
        
		/**
		 * An optional annotation whose value the variable that will be bound by
		 * a {@link DistinctTermAdvancer} layered over the access path.
		 * 
		 * @see <a href="http://trac.blazegraph.com/ticket/1034" > DISTINCT
		 *      PREDICATEs query is slow </a>
		 */
        String DISTINCT_TERM_SCAN_VAR = "distinctTermScanVar";
        
    }
    
    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public StatementPatternNode(final StatementPatternNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public StatementPatternNode(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * A triple pattern. The {@link Scope} will be
     * {@link Scope#DEFAULT_CONTEXTS}, the context will be <code>null</code>.
     * 
     * @param s
     * @param p
     * @param o
     * 
     * @see StatementPatternNode#StatementPatternNode(TermNode, TermNode,
     *      TermNode, TermNode, Scope)
     */
    public StatementPatternNode(final TermNode s, final TermNode p,
            final TermNode o) {

        this(s, p, o, null/* context */, Scope.DEFAULT_CONTEXTS);

    }

    /**
     * A quad pattern.
     * <p>
     * Note: When a {@link StatementPatternNode} appears in a WHERE clause, the
     * {@link Scope} should be marked as {@link Scope#DEFAULT_CONTEXTS} if it is
     * NOT embedded within a GRAPH clause and otherwise as
     * {@link Scope#NAMED_CONTEXTS}.
     * <p>
     * The context position of the statement should be <code>null</code> unless
     * it is embedded within a GRAPH clause, in which case the context is the
     * context specified for the parent GRAPH clause.
     * <p>
     * The SPARQL UPDATE <code>WITH uri</code> is a syntactic sugar for
     * <code>GRAPH uri {...}</code>. Therefore, when present, any
     * {@link StatementPatternNode} outside of an explicit GRAPH group is also
     * marked as {@link Scope#NAMED_CONTEXTS} and the context position will be
     * bound to the <i>uri</i> specified in the <code>WITH</code> clause.
     * <p>
     * A <code>null</code> context in {@link Scope#DEFAULT_CONTEXTS} is
     * interpreted as the RDF merge of the graphs in the defaultGraph (as
     * specified by the {@link DatasetNode}). When non-<code>null</code> (it can
     * be bound by the SPARQL UPDATE <em>WITH</em> clause), the defaultGraph
     * declared by the {@link DatasetNode} is ignored and the context is bound
     * to the constant specified in that <em>WITH</em> clause.
     * <p>
     * Absent any other constraints on the query, an unbound variable context in
     * {@link Scope#NAMED_CONTEXTS} may be bound to any named graph specified by
     * the {@link DatasetNode}.
     * 
     * @param s
     *            The subject (variable or constant; required).
     * @param p
     *            The subject (variable or constant; required).
     * @param o
     *            The subject (variable or constant; required).
     * @param c
     *            The context (variable or constant; optional).
     * @param scope
     *            Either {@link Scope#DEFAULT_CONTEXTS} or
     *            {@link Scope#NAMED_CONTEXTS} (required).
     * 
     * @throws IllegalArgumentException
     *             if <i>s</i>, <i>p</i>, or <i>o</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>scope</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>scope</i> is {@link Scope#NAMED_CONTEXTS} and <i>c</i>
     *             is <code>null</code>.
     */
    public StatementPatternNode(final TermNode s, final TermNode p,
            final TermNode o, final TermNode c, final Scope scope) {

        super(new BOp[] { s, p, o, c }, scope == null ? null/* anns */: NV
                .asMap(new NV(Annotations.SCOPE, scope)));

        if (scope == null)
            throw new IllegalArgumentException();
        
		if (s == null || p == null || o == null)
		    throw new IllegalArgumentException();

        if (scope == Scope.NAMED_CONTEXTS && c == null)
            throw new IllegalArgumentException();
		
	}

	/**
	 * The variable or constant for the subject position (required).
	 */
    final public TermNode s() {

        return (TermNode) get(0);

    }

	/**
	 * The variable or constant for the predicate position (required).
	 */
    final public TermNode p() {

        return (TermNode) get(1);

    }

	/**
	 * The variable or constant for the object position (required).
	 */
    final public TermNode o() {

        return (TermNode) get(2);

    }

	/**
	 * The variable or constant for the context position (required iff in quads
	 * mode).
	 */
    final public TermNode c() {

        return (TermNode) get(3);

    }
    
    /**
     * Strengthen return type.
     */
    @Override
    public TermNode get(final int i) {
    	
    	return (TermNode) super.get(i);
    	
    }

	final public void setC(final TermNode c) {

		this.setArg(3, c);
		
    }
    
	/**
	 * The statement identifier variable for triples which match this statement
	 * pattern (optional). The statement identifier is the composition of the
	 * (subject, predicate, and object) positions of the matched statements.
	 * 
	 * @see Annotations#SID
	 */
	final public VarNode sid() {

		return (VarNode) getProperty(Annotations.SID);

    }

	/**
	 * Set the SID variable.
	 */
	final public void setSid(final VarNode sid) {

		setProperty(Annotations.SID, sid);

	}
    
    /**
     * The scope for this statement pattern (either named graphs or default
     * graphs).
     * 
     * @see Annotations#SCOPE
     * @see Scope
     */
    final public Scope getScope() {

        return (Scope) getRequiredProperty(Annotations.SCOPE);
        
    }

	final public void setScope(final Scope scope) {

		if (scope == null)
			throw new IllegalArgumentException();
    	
		setProperty(Annotations.SCOPE, scope);
    	
    }

	/**
	 * Return the {@link VarNode} associated with the optional
	 * {@link Annotations#FAST_RANGE_COUNT_VAR} property.
	 * 
	 * @return The {@link VarNode} -or- <code>null</code> if this triple pattern
	 *         is not associated with that annotation.
	 */
	final public VarNode getFastRangeCountVar() {
		
		return (VarNode) getProperty(Annotations.FAST_RANGE_COUNT_VAR);
		
	}
	
	final public void setFastRangeCount(final VarNode var) {

		if (var == null)
			throw new IllegalArgumentException();

		setProperty(Annotations.FAST_RANGE_COUNT_VAR, var);

	}

	/**
	 * Return the variable that will be bound by the
	 * {@link DistinctTermAdvancer} pattern.
	 * 
	 * @return The distinct term scan variable -or- <code>null</code> if the
	 *         access path will not use a distinct term scan.
	 * 
	 * @see Annotations#DISTINCT_TERM_SCAN_VAR
	 */
	final public VarNode getDistinctTermScanVar() {

		return (VarNode) getProperty(Annotations.DISTINCT_TERM_SCAN_VAR);

	}

	final public void setDistinctTermScanVar(final VarNode var) {

		setProperty(Annotations.DISTINCT_TERM_SCAN_VAR, var);

	}
    
    /**
     * {@inheritDoc}
     * <p>
     * This returns <code>true</code> iff the {@link StatementPatternNode} was
     * lifted out of an optional {@link JoinGroupNode} such that it has OPTIONAL
     * semantics.
     * 
     * @see ASTSimpleOptionalOptimizer
     */
    @Override
    final public boolean isOptional() {

        return getProperty(Annotations.OPTIONAL, Annotations.DEFAULT_OPTIONAL);

    }    

    /**
     * Returns <code>false</code>.
     */
    @Override
    final public boolean isMinus() {
     
        return false;
        
    }
    
    /**
     * Mark this {@link StatementPatternNode} as one which was lifted out of a
     * "simple optional" group and which therefore has "optional" semantics (we
     * will do an optional join for it).
     * <p>
     * Note: The need to maintain the correct semantics for the simple optional
     * group (statement pattern plus filter(s)) is also the reason why the
     * lifted FILTER(s) MUST NOT require the materialization of any variables
     * which would not have been bound before that JOIN. Since variables bound
     * by the JOIN for the optional statement pattern will not be materialized,
     * filters attached to that JOIN can not require materialization of
     * variables bound by the JOIN (though they can depend on variables already
     * bound by the required joins in the parent group).
     * 
     * @see ASTSimpleOptionalOptimizer
     */
    final public void setOptional(final boolean optional) {

        setProperty(Annotations.OPTIONAL, optional);

    }
    
    /**
     * Attach a {@link RangeNode} that describes a range for the statement
     * pattern's O value.
     * 
     * @param range
     */
    final public void setRange(final RangeNode range) {
    	
        setProperty(Annotations.RANGE, range);
    	
    }
    
    final public RangeNode getRange() {
    	
        return (RangeNode) getProperty(Annotations.RANGE);
    	
    }

    @Override
    final public List<FilterNode> getAttachedJoinFilters() {

        @SuppressWarnings("unchecked")
        final List<FilterNode> filters = (List<FilterNode>) getProperty(Annotations.FILTERS);

        if (filters == null) {

            return Collections.emptyList();

        }

        return Collections.unmodifiableList(filters);

    }

    @Override
    final public void setAttachedJoinFilters(final List<FilterNode> filters) {

        setProperty(Annotations.FILTERS, filters);

    }

    /**
     * Return <code>true</code> if none of s, p, o, or c is a variable.
     */
    public boolean isGround() {

        if (s() instanceof VarNode)
            return false;

        if (p() instanceof VarNode)
            return false;
        
        if (o() instanceof VarNode)
            return false;
        
        if (c() instanceof VarNode)
            return false;
        
        return true;
        
    }

    /**
     * Return the variables used by the predicate - i.e. what this node will
     * attempt to bind when run.
     */
    public Set<IVariable<?>> getProducedBindings() {

        final Set<IVariable<?>> producedBindings = new LinkedHashSet<IVariable<?>>();

        final TermNode s = s();
        final TermNode p = p();
        final TermNode o = o();
        final TermNode c = c();

        addProducedBindings(s, producedBindings);
        addProducedBindings(p, producedBindings);
        addProducedBindings(o, producedBindings);
        addProducedBindings(c, producedBindings);
        
        return producedBindings;

    }
    
    /**
     * This handles the special case where we've wrapped a Var with a Constant
     * because we know it's bound, perhaps by the exogenous bindings. If we
     * don't handle this case then we get the join vars wrong.
     * 
     * @see StaticAnalysis._getJoinVars
     */
    private void addProducedBindings(final TermNode t,
            final Set<IVariable<?>> producedBindings) {

        if (t instanceof VarNode) {

            producedBindings.add(((VarNode) t).getValueExpression());

        } else if (t instanceof ConstantNode) {

            final ConstantNode cNode = (ConstantNode) t;
            final Constant<?> c = (Constant<?>) cNode.getValueExpression();
            final IVariable<?> var = c.getVar();
            if (var != null) {
                producedBindings.add(var);
            }

        }

    }

    @Override
	public String toString(final int indent) {
		
	    final StringBuilder sb = new StringBuilder();

        sb.append("\n").append(indent(indent)).append(toShortString());

        final List<FilterNode> filters = getAttachedJoinFilters();
        if(!filters.isEmpty()) {
            for (FilterNode filter : filters) {
                sb.append(filter.toString(indent + 1));
            }
        }

        if (getQueryHints() != null && !getQueryHints().isEmpty()) {
            sb.append("\n");
            sb.append(indent(indent + 1));
            shortenName(sb, Annotations.QUERY_HINTS);
            sb.append("=");
            sb.append(getQueryHints().toString());
        }
        
        final Long rangeCount = (Long) getProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY);

        final IKeyOrder<?> keyOrder = (IKeyOrder<?>) getProperty(AST2BOpBase.Annotations.ORIGINAL_INDEX);

        if (rangeCount != null) {
            sb.append("\n");
            sb.append(indent(indent + 1));
            shortenName(sb, AST2BOpBase.Annotations.ESTIMATED_CARDINALITY);
            sb.append("=");
            sb.append(rangeCount.toString());
        }

        if (keyOrder != null) {
            sb.append("\n");
            sb.append(indent(indent + 1));
            shortenName(sb, AST2BOpBase.Annotations.ORIGINAL_INDEX);
            sb.append("=");
            sb.append(keyOrder.toString());
        }

        return sb.toString();
		
	}

	@Override
    public String toShortString() {
        
	    final StringBuilder sb = new StringBuilder();

	    final Integer id = (Integer)getProperty(BOp.Annotations.BOP_ID);
        sb.append("StatementPatternNode");
        if (id != null) {
            sb.append("[").append(id.toString()).append("]");
        }
        sb.append("(");
        sb.append(s()).append(", ");
        sb.append(p()).append(", ");
        sb.append(o());

        final TermNode c = c();
        if (c != null) {
            sb.append(", ").append(c);
        }

        sb.append(")");
        
		final VarNode sid = sid();
		if (sid != null) {
			sb.append(" [sid=" + sid + "]");
		}

		final Scope scope = getScope();
		if (scope != null) {
			sb.append(" [scope=" + scope + "]");
		}

		final VarNode fastRangeCountVar = getFastRangeCountVar();
		if (fastRangeCountVar != null) {
			sb.append(" [fastRangeCount=" + fastRangeCountVar + "]");
		}

		final VarNode distinctTermScanVar = getDistinctTermScanVar();
		if (distinctTermScanVar != null) {
			sb.append(" [distinctTermScan=" + distinctTermScanVar + "]");
		}

        if(isOptional()) {
            sb.append(" [optional]");
        }

        if (!getAttachedJoinFilters().isEmpty()) {
            sb.append(" [#filters=" + getAttachedJoinFilters().size() + "]");
        }

        return sb.toString();
    }

	/* (non-Javadoc)
	 * @see com.bigdata.rdf.sparql.ast.IReorderableNode#isReorderable()
	 */
	@Override
	public boolean isReorderable() {
		
		return !isOptional();
		
	}

	/* (non-Javadoc)
	 * @see com.bigdata.rdf.sparql.ast.IReorderableNode#getEstimatedCardinality()
	 */
	@Override
	public long getEstimatedCardinality(StaticOptimizer opt) {
        
		return getProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY, -1l);
        
	}
	
   @Override
   public Set<IVariable<?>> getRequiredBound(StaticAnalysis sa) {
      return new HashSet<IVariable<?>>();
   }

   @Override
   public Set<IVariable<?>> getDesiredBound(StaticAnalysis sa) {
      return sa.getSpannedVariables(this, true, new HashSet<IVariable<?>>());
   }
   
   

}
