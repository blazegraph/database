package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.internal.constraints.RangeBOp;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpBase;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.eval.Rule2BOpUtility;
import com.bigdata.rdf.sparql.ast.optimizers.ASTGraphGroupOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSimpleOptionalOptimizer;
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
 * {@link Rule2BOpUtility} which handles the default and named graph access
 * patterns.
 * <p>
 * Note: If a variable is bound, then we bind that slot of the predicate. If a
 * variable can take some enumerated set of values, then we use an
 * {@link #INLINE} access path to model that "IN" constraint. If the value for a
 * variable must lie within some key range, then we handle that case using
 * {@link RangeBOp}. If we have no information about a variable, then we just
 * leave the variable unbound.
 * 
 * TODO We should also handle datatype constraints on a variable here. For
 * example, if a variable is known to be numeric, or known to be xsd:int, then
 * we can immediately reject any bindings which would violate that type
 * constraint. To do this right, we need to notice those type constraints and
 * propagate them backwards in the plan so we can reject bindings as early as
 * possible.
 */
public class StatementPatternNode extends
        GroupMemberNodeBase<StatementPatternNode> implements
        IJoinNode {

    private static final long serialVersionUID = 1L;

    public interface Annotations extends GroupMemberNodeBase.Annotations {

        /**
         * The {@link Scope} (required).
         * 
         * @see ASTGraphGroupOptimizer
         */
        String SCOPE = "scope";
        
        /**
         * Boolean flag indicates that a {@link StatementPatternNode} was lifted
         * out of an optional {@link JoinGroupNode} and has OPTIONAL semantics.
         * 
         * @see ASTSimpleOptionalOptimizer
         */
        String SIMPLE_OPTIONAL = "simpleOptional";
    
        boolean DEFAULT_SIMPLE_OPTIONAL = false;
        
        /**
         * A {@link List} of {@link FilterNode}s for constraints which MUST run
         * <em>with</em> the JOIN for this statement pattern (optional).
         * 
         * <h3>Simple Optionals</h3>
         * 
         * This is specified when a simple optional is identified and the
         * filters are lifted with the optional statement pattern into the
         * parent group. Under these circumstances the FILTER(s) MUST be
         * attached to the JOIN in order to preserve the OPTIONAL semantics of
         * the join (if the filters are run in the pipeline after the join then
         * the join+constraints no longer has the same semantics as the optional
         * group with that statement pattern and filters).
         * <p>
         * Note: The need to maintain the correct semantics for the simple
         * optional group (statement pattern plus filter(s)) is also the reason
         * why the lifted FILTER(s) MUST NOT require the materialization of any
         * variables which would not have been bound before that JOIN. Since
         * variables bound by the JOIN for the optional statement pattern will
         * not be materialized, filters attached to that JOIN can not require
         * materialization of variables bound by the JOIN (though they can
         * depend on variables already bound by the required joins in the parent
         * group).
         * 
         * @see ASTSimpleOptionalOptimizer
         * 
         *      FIXME This annotation was originally developed to support simple
         *      optionals (and is currently only in the code path for simple
         *      optionals), but might also be used to filter an access path. For
         *      example, with IN(var,values). However, this annotation does not
         *      distinguish between local and remote "element" filters and there
         *      is some API tensions between visitation of "elements" versus
         *      "bindingSets" for access paths.
         */
        String FILTERS = "filters";
     
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
         */
        String INLINE = "inline";
        
        /**
         * An optional attribute whose value is an {@link RangeBOp} which models
         * the key-range constraint on the access path. The {@link RangeBOp} is
         * used when there are filters which impose a GT/GTE and/or LT/LTE
         * restriction on the values which a variable may take on for that
         * access path. 
         */
        String RANGE = "range";
        
    }
    
    /**
     * Required deep copy constructor.
     */
    public StatementPatternNode(StatementPatternNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public StatementPatternNode(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * A triple pattern.
     * 
     * @param s
     * @param p
     * @param o
     */
    public StatementPatternNode(final TermNode s, final TermNode p,
            final TermNode o) {

        this(s, p, o, null/* context */, Scope.DEFAULT_CONTEXTS);

    }

    /**
     * A quad pattern.
     * 
     * @param s
     * @param p
     * @param o
     * @param c
     * @param scope
     *            Either {@link Scope#DEFAULT_CONTEXTS} or
     *            {@link Scope#NAMED_CONTEXTS}.
     */
//	@SuppressWarnings("unchecked")
    public StatementPatternNode(
			final TermNode s, final TermNode p, final TermNode o, 
			final TermNode c, final Scope scope) {

        super(new BOp[] { s, p, o, c }, scope == null ? null/* anns */: NV
                .asMap(new NV(Annotations.SCOPE, scope)));

		if (s == null || p == null || o == null) {
		
		    throw new IllegalArgumentException();
		    
		}

	}
	
	public TermNode s() {
		return (TermNode) get(0);
	}

	public TermNode p() {
	    return (TermNode) get(1);
	}

	public TermNode o() {
	    return (TermNode) get(2);
	}

	public TermNode c() {
	    return (TermNode) get(3);
	}
	
    /**
     * The scope for this statement pattern (either named graphs or default
     * graphs).
     * 
     * @see Annotations#SCOPE
     * @see Scope
     */
    public Scope getScope() {

        return (Scope) getProperty(Annotations.SCOPE);
        
    }

    /**
     * Return <code>true</code> the {@link StatementPatternNode} was lifted out
     * of an optional {@link JoinGroupNode} and has OPTIONAL semantics.
     * 
     * @see Annotations#SIMPLE_OPTIONAL
     */
    // TODO Replace with isOptional()
    public boolean isSimpleOptional() {
        
        return isOptional();
        
    }
    
    /**
     * Return <code>true</code> the {@link StatementPatternNode} was lifted out
     * of an optional {@link JoinGroupNode} and has OPTIONAL semantics.
     * 
     * @see Annotations#SIMPLE_OPTIONAL
     */
    public boolean isOptional() {
        
        return getProperty(Annotations.SIMPLE_OPTIONAL,
                Annotations.DEFAULT_SIMPLE_OPTIONAL);

    }
    

    /**
     * Mark this {@link StatementPatternNode} as one which was lifted out of a
     * "simple optional" group and which therefore has "optional" semantics (we
     * will do an optional join for it).
     */
    // TODO Rename as setOptional()
    public void setSimpleOptional(final boolean simpleOptional) {
        
        setProperty(Annotations.SIMPLE_OPTIONAL,simpleOptional);
        
    }

    /**
     * Return the FILTER(s) associated with this statement pattern. Such filters
     * will be run with the JOIN for this statement pattern. As such, they MUST
     * NOT rely on materialization of variables which would not have been bound
     * before that JOIN.
     * 
     * @see Annotations#FILTERS
     * @see ASTSimpleOptionalOptimizer
     */
    public List<FilterNode> getFilters() {

        @SuppressWarnings("unchecked")
        final List<FilterNode> filters = (List<FilterNode>) getProperty(Annotations.FILTERS);

        if (filters == null) {

            return Collections.emptyList();

        }

        return Collections.unmodifiableList(filters);

    }

    public void setFilters(final List<FilterNode> filters) {

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

        if (s instanceof VarNode) {
            producedBindings.add(((VarNode) s).getValueExpression());
        }

        if (p instanceof VarNode) {
            producedBindings.add(((VarNode) p).getValueExpression());
        }

        if (o instanceof VarNode) {
            producedBindings.add(((VarNode) o).getValueExpression());
        }

        if (c != null && c instanceof VarNode) {
            producedBindings.add(((VarNode) c).getValueExpression());
        }

        return producedBindings;

    }

	public String toString(final int indent) {
		
	    final StringBuilder sb = new StringBuilder();

        sb.append("\n").append(indent(indent)).append(toShortString());

        final List<FilterNode> filters = getFilters();
        if(!filters.isEmpty()) {
            for (FilterNode filter : filters) {
                sb.append(filter.toString(indent + 1));
            }
        }

        if (getQueryHints() != null && !getQueryHints().isEmpty()) {
            sb.append("\n");
            sb.append(indent(indent+1));
            sb.append(Annotations.QUERY_HINTS);
            sb.append("=");
            sb.append(getQueryHints().toString());
        }
        
        final Long rangeCount = (Long) getProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY);

        final IKeyOrder<?> keyOrder = (IKeyOrder<?>) getProperty(AST2BOpBase.Annotations.ORIGINAL_INDEX);

        if (rangeCount != null) {
            sb.append("\n");
            sb.append(indent(indent + 1));
            sb.append(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY);
            sb.append("=");
            sb.append(rangeCount.toString());
        }

        if (keyOrder != null) {
            sb.append("\n");
            sb.append(indent(indent + 1));
            sb.append(AST2BOpBase.Annotations.ORIGINAL_INDEX);
            sb.append("=");
            sb.append(keyOrder.toString());
        }

        return sb.toString();
		
	}

	@Override
    public String toShortString() {
        
	    final StringBuilder sb = new StringBuilder();

        sb.append("StatementPatternNode(");
        sb.append(s()).append(", ");
        sb.append(p()).append(", ");
        sb.append(o());

        final TermNode c = c();
        if (c != null) {
            sb.append(", ").append(c);
        }

        final Scope scope = getScope();
        if (scope != null) {
            sb.append(", ").append(scope);
        }

        sb.append(")");
        
        if(isSimpleOptional()) {
            sb.append(" [simpleOptional]");
        }
        if (!getFilters().isEmpty()) {
            sb.append(" [#filters=" + getFilters().size() + "]");
        }

        return sb.toString();
    }

}
