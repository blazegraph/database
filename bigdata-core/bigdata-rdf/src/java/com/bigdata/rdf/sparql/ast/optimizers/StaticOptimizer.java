package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.joinGraph.fast.DefaultEvaluationPlan2;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.IReorderableNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.ASTStaticJoinOptimizer.Annotations;

/**
 * This is the old static optimizer code, taken directly from
 * {@link DefaultEvaluationPlan2}, but lined up with the AST API instead of the
 * Rule and IPredicate API.
 * 
 */
public final class StaticOptimizer {

    private static final transient Logger log = ASTStaticJoinOptimizer.log;
    
	private final StaticAnalysis sa;

	private final IBindingProducerNode[] ancestry;

	private final Set<IVariable<?>> ancestryVars;

	private final List<IReorderableNode> nodes;

	private final int arity;

	/**
	 * This is computed by the optimizer, and is a guess!
	 */
	private final long cardinality;

	private static final long NO_SHARED_VARS = Long.MAX_VALUE - 3;

	/**
	 * The computed evaluation order. The elements in this array are the order
	 * in which each tail predicate will be evaluated. The index into the array
	 * is the index of the tail predicate whose evaluation order you want. So
	 * <code>[2,0,1]</code> says that the predicates will be evaluated in the
	 * order tail[2], then tail[0], then tail[1].
	 */
	private int[/* order */] order;

	public int[] getOrder() {

		if (order == null) {

			/*
			 * This will happen if you try to use toString() during the ctor
			 * before the order has been computed.
			 */

			throw new IllegalStateException();

		}

		return order;

	}

	/**
	 * Cache of the computed range counts for the predicates in the tail. The
	 * elements of this array are initialized to -1L, which indicates that the
	 * range count has NOT been computed. Range counts are computed on demand
	 * and MAY be zero. Only an approximate range count is obtained. Such
	 * approximate range counts are an upper bound on the #of elements that are
	 * spanned by the access pattern. Therefore if the range count reports ZERO
	 * (0L) it is a real zero and the access pattern does not match anything in
	 * the data. The only other caveat is that the range counts are valid as of
	 * the commit point on which the access pattern is reading. If you obtain
	 * them for {@link ITx#READ_COMMITTED} or {@link ITx#UNISOLATED} views then
	 * they could be invalidated by concurrent writers.
	 */
	private long[/* tailIndex */] rangeCount;

	private Tail[/* tailIndex */] tail;

	/**
	 * Keeps track of which tails have been used already and which still need to
	 * be evaluated.
	 */
	private boolean[/* tailIndex */] used;

	/**
	 * See {@link Annotations#OPTIMISTIC}.
	 */
	private final double optimistic;

	public StaticOptimizer(StaticOptimizer parent, List<IReorderableNode> nodes) {
		this(parent.sa, parent.ancestry, nodes, parent.optimistic);
	}

	StaticOptimizer(final QueryRoot queryRoot, final AST2BOpContext context,
			final IBindingProducerNode[] ancestry,
			final List<IReorderableNode> nodes, final double optimistic) {
		this(new StaticAnalysis(queryRoot, context), ancestry, nodes,
				optimistic);
	}

	private StaticOptimizer(final StaticAnalysis sa,
			final IBindingProducerNode[] ancestry,
			final List<IReorderableNode> nodes, final double optimistic) {

		if (ancestry == null)
			throw new IllegalArgumentException();

		if (nodes == null)
			throw new IllegalArgumentException();

		this.sa = sa;

		this.ancestry = ancestry;

		this.ancestryVars = new LinkedHashSet<IVariable<?>>();

		this.nodes = nodes;

		this.arity = nodes.size();

		if (log.isDebugEnabled()) {
			log.debug("arity: " + arity);
			for (int i = 0; i < arity; i++) {
			    final IReorderableNode node = nodes.get(i);
			    log.debug(node.getClass() + 
			            ", reorderable: " + node.isReorderable() + 
			            ", estcard: " + node.getEstimatedCardinality(this) + 
			            ", vars: " + getVars(i));
			    log.debug(node.getEstimatedCardinality(this));
			    log.debug(node.isReorderable());
			}
		}

		this.optimistic = optimistic;

		this.cardinality = calc();

		if (ASTStaticJoinOptimizer.log.isDebugEnabled()) {
			for (int i = 0; i < arity; i++) {
				ASTStaticJoinOptimizer.log.debug(order[i]);
			}
		}

	}

	/**
	 * Computes and sets the evaluation order, and returns an estimated
	 * cardinality.
	 */
	private long calc() {

		if (order != null)
			throw new IllegalStateException(
					"calc should only be called from the constructor");

		order = new int[arity];
		rangeCount = new long[arity];
		used = new boolean[arity];
		tail = new Tail[arity];

		// clear arrays.
		for (int i = 0; i < arity; i++) {
			order[i] = -1; // -1 is used to detect logic errors.
			rangeCount[i] = -1L; // -1L indicates no range count yet.
			used[i] = false; // not yet evaluated
		}

		if (arity == 0) {
			return 1l;
		}

		if (arity == 1) {
			order[0] = 0;
			return cardinality(0);
		}

		/*
		 * Seems like the easiest way to handle the ancestry is the exact same
		 * way we handle the "run first" statement patterns (text search), which
		 * is that we collect up the variables that are bound and then give
		 * preferential treatment to the predicates that can join on those
		 * variables.
		 */
		for (IBindingProducerNode join : ancestry) {
			if (ASTStaticJoinOptimizer.log.isDebugEnabled()) {
				ASTStaticJoinOptimizer.log
						.debug("considering join node from ancestry: " + join);
			}
			sa.getDefinitelyProducedBindings(join, ancestryVars, true/* recursive */);
		}
		if (ASTStaticJoinOptimizer.log.isDebugEnabled()) {
			ASTStaticJoinOptimizer.log.debug("bindings from ancestry: "
					+ Arrays.toString(ancestryVars.toArray()));
		}

		/*
		 * See if there is a best tail to run first. The "run first" query hint
		 * gets priority, then the tails that share variables with the ancestry.
		 */
		int preferredFirstTail = -1;

		for (int i = 0; i < arity; i++) {

			/*
			 * We need the optimizer to play nice with the run first hint.
			 * Choose the first "run first" tail we see.
			 */
			if (nodes.get(i).getProperty(QueryHints.RUN_FIRST, false)) {
				preferredFirstTail = i;
				break;
			}
		}

		/*
		 * Always choose a ZERO cardinality tail first, regardless of ancestry.
		 */
		for (int i = 0; i < arity; i++) {
		    if (cardinality(i) == 0) {
		        preferredFirstTail = i;
		        break;
		    }
		}
		
		/*
		 * If there was no "run first" query hint, then go to the ancestry.
		 */
		if (preferredFirstTail == -1)
			preferredFirstTail = getNextTailThatSharesVarsWithAncestry();

		if (ASTStaticJoinOptimizer.log.isDebugEnabled()) {
			ASTStaticJoinOptimizer.log.debug("preferred first tail: "
					+ preferredFirstTail);
		}

		// special case if there are only two tails
		if (arity == 2) {
			if (ASTStaticJoinOptimizer.log.isDebugEnabled())
				ASTStaticJoinOptimizer.log.debug("two tails left");
			if (preferredFirstTail != -1) {
				order[0] = preferredFirstTail;
				order[1] = preferredFirstTail == 0 ? 1 : 0;
			} else {
				if (cardinality(0) == cardinality(1) && 
						nodes.get(0) instanceof StatementPatternNode &&
						nodes.get(1) instanceof StatementPatternNode) {
					final VarNode sid0 = ((StatementPatternNode) nodes.get(0)).sid();
					final VarNode sid1 = ((StatementPatternNode) nodes.get(1)).sid();
					if (sid0 != null && sid1 == null) {
						order[0] = 1;
						order[1] = 0;
					}
				}
				if (order[0] == -1) {
					order[0] = cardinality(0) <= cardinality(1) ? 0 : 1;
					order[1] = cardinality(0) <= cardinality(1) ? 1 : 0;
				}
			}
			return computeJoinCardinality(getTail(0), getTail(1));
		}

		/*
		 * There will be (tails-1) joins, we just need to figure out what they
		 * should be.
		 */
		StaticOptimizer.Join join;
		if (preferredFirstTail == -1)
			join = getFirstJoin();
		else
			join = getFirstJoin(preferredFirstTail);

		long cardinality = join.cardinality;

		int t1 = ((Tail) join.getD1()).getTailIndex();
		int t2 = ((Tail) join.getD2()).getTailIndex();
		if (preferredFirstTail == -1) {
			order[0] = cardinality(t1) <= cardinality(t2) ? t1 : t2;
			order[1] = cardinality(t1) <= cardinality(t2) ? t2 : t1;
		} else {
			order[0] = t1;
			order[1] = t2;
		}
		used[order[0]] = true;
		used[order[1]] = true;
		for (int i = 2; i < arity; i++) {
			join = getNextJoin(join);
			order[i] = ((Tail) join.getD2()).getTailIndex();
			used[order[i]] = true;
		}
		return cardinality;
	}

	/**
	 * Start by looking at every possible initial join. Take every tail and
	 * match it with every other tail to find the lowest possible cardinality.
	 * See
	 * {@link #computeJoinCardinality(com.bigdata.bop.joinGraph.fast.DefaultEvaluationPlan2.IJoinDimension, com.bigdata.bop.joinGraph.fast.DefaultEvaluationPlan2.IJoinDimension)}
	 * for more on this.
	 */
	private StaticOptimizer.Join getFirstJoin() {
		if (ASTStaticJoinOptimizer.log.isDebugEnabled()) {
			ASTStaticJoinOptimizer.log.debug("evaluating first join");
		}
		long minJoinCardinality = Long.MAX_VALUE;
		long minTailCardinality = Long.MAX_VALUE;
		long minOtherTailCardinality = Long.MAX_VALUE;
		Tail minT1 = null;
		Tail minT2 = null;
		for (int i = 0; i < arity; i++) {
			// only check unused tails
			if (used[i]) {
				continue;
			}
			Tail t1 = getTail(i);
			long t1Cardinality = cardinality(i);
			for (int j = 0; j < arity; j++) {
				// check only non-same and unused tails
				if (i == j || used[j]) {
					continue;
				}
				Tail t2 = getTail(j);
				long t2Cardinality = cardinality(j);
				long joinCardinality = computeJoinCardinality(t1, t2);
				long tailCardinality = Math.min(t1Cardinality, t2Cardinality);
				long otherTailCardinality = Math.max(t1Cardinality,
						t2Cardinality);
				if (ASTStaticJoinOptimizer.log.isDebugEnabled())
					ASTStaticJoinOptimizer.log.debug("evaluating " + i + " X "
							+ j + ": cardinality= " + joinCardinality);
				if (joinCardinality < minJoinCardinality) {
					if (ASTStaticJoinOptimizer.log.isDebugEnabled())
						ASTStaticJoinOptimizer.log.debug("found a new min: "
								+ joinCardinality);
					minJoinCardinality = joinCardinality;
					minTailCardinality = tailCardinality;
					minOtherTailCardinality = otherTailCardinality;
					minT1 = t1;
					minT2 = t2;
				} else if (joinCardinality == minJoinCardinality) {
					if (tailCardinality < minTailCardinality) {
						if (ASTStaticJoinOptimizer.log.isDebugEnabled())
							ASTStaticJoinOptimizer.log
									.debug("found a new min: "
											+ joinCardinality);
						minJoinCardinality = joinCardinality;
						minTailCardinality = tailCardinality;
						minOtherTailCardinality = otherTailCardinality;
						minT1 = t1;
						minT2 = t2;
					} else if (tailCardinality == minTailCardinality) {
						if (otherTailCardinality < minOtherTailCardinality) {
							if (ASTStaticJoinOptimizer.log.isDebugEnabled())
								ASTStaticJoinOptimizer.log
										.debug("found a new min: "
												+ joinCardinality);
							minJoinCardinality = joinCardinality;
							minTailCardinality = tailCardinality;
							minOtherTailCardinality = otherTailCardinality;
							minT1 = t1;
							minT2 = t2;
						}
					}
				}
			}
		}
		// the join variables is the union of the join dimensions' variables
		Set<String> vars = new HashSet<String>();
		vars.addAll(minT1.getVars());
		vars.addAll(minT2.getVars());
		return new Join(minT1, minT2, minJoinCardinality, vars);
	}

	private StaticOptimizer.Join getFirstJoin(final int preferredFirstTail) {
		if (ASTStaticJoinOptimizer.log.isDebugEnabled()) {
			ASTStaticJoinOptimizer.log.debug("evaluating first join");
		}

		long minJoinCardinality = Long.MAX_VALUE;
		long minOtherTailCardinality = Long.MAX_VALUE;
		Tail minT2 = null;
		final int i = preferredFirstTail;
		final Tail t1 = getTail(i);
		for (int j = 0; j < arity; j++) {
			// check only non-same and unused tails
			if (i == j || used[j]) {
				continue;
			}
			Tail t2 = getTail(j);
			long t2Cardinality = cardinality(j);
			long joinCardinality = computeJoinCardinality(t1, t2);
			if (ASTStaticJoinOptimizer.log.isDebugEnabled())
				ASTStaticJoinOptimizer.log.debug("evaluating " + i + " X " + j
						+ ": cardinality= " + joinCardinality);
			if (joinCardinality < minJoinCardinality) {
				if (ASTStaticJoinOptimizer.log.isDebugEnabled())
					ASTStaticJoinOptimizer.log.debug("found a new min: "
							+ joinCardinality);
				minJoinCardinality = joinCardinality;
				minOtherTailCardinality = t2Cardinality;
				minT2 = t2;
			} else if (joinCardinality == minJoinCardinality) {
				if (t2Cardinality < minOtherTailCardinality) {
					if (ASTStaticJoinOptimizer.log.isDebugEnabled())
						ASTStaticJoinOptimizer.log.debug("found a new min: "
								+ joinCardinality);
					minJoinCardinality = joinCardinality;
					minOtherTailCardinality = t2Cardinality;
					minT2 = t2;
				}
			}
		}

		// the join variables is the union of the join dimensions' variables
		Set<String> vars = new HashSet<String>();
		vars.addAll(t1.getVars());
		vars.addAll(minT2.getVars());
		return new Join(t1, minT2, minJoinCardinality, vars);
	}

	private Tail getTail(int tailIndex) {
		if (tail[tailIndex] == null) {
			tail[tailIndex] = new Tail(tailIndex, rangeCount(tailIndex),
					getVars(tailIndex));
		}
		return tail[tailIndex];
	}

	/**
	 * Similar to {@link #getFirstJoin()}, but we have one join dimension
	 * already calculated.
	 * 
	 * @param d1
	 *            the first join dimension
	 * @return the new join with the lowest cardinality from the remaining tails
	 */
	private StaticOptimizer.Join getNextJoin(IJoinDimension d1) {
		if (ASTStaticJoinOptimizer.log.isDebugEnabled()) {
			ASTStaticJoinOptimizer.log.debug("evaluating next join");
		}
		long minJoinCardinality = Long.MAX_VALUE;
		long minTailCardinality = Long.MAX_VALUE;
		Tail minTail = null;
		for (int i = 0; i < arity; i++) {
			// only check unused tails
			if (used[i]) {
				continue;
			}
			Tail tail = getTail(i);
			long tailCardinality = cardinality(i);
			long joinCardinality = computeJoinCardinality(d1, tail);
			if (ASTStaticJoinOptimizer.log.isDebugEnabled())
				ASTStaticJoinOptimizer.log.debug("evaluating "
						+ d1.toJoinString() + " X " + i + ": cardinality= "
						+ joinCardinality);
			if (joinCardinality < minJoinCardinality) {
				if (ASTStaticJoinOptimizer.log.isDebugEnabled())
					ASTStaticJoinOptimizer.log.debug("found a new min: "
							+ joinCardinality);
				minJoinCardinality = joinCardinality;
				minTailCardinality = tailCardinality;
				minTail = tail;
			} else if (joinCardinality == minJoinCardinality) {
				if (tailCardinality < minTailCardinality) {
					if (ASTStaticJoinOptimizer.log.isDebugEnabled())
						ASTStaticJoinOptimizer.log.debug("found a new min: "
								+ joinCardinality);
					minJoinCardinality = joinCardinality;
					minTailCardinality = tailCardinality;
					minTail = tail;
				}
			}
		}

		/*
		 * If we are at the "no shared variables" tails, the first thing we do
		 * is look to the ancestry for the next tail.
		 */
		if (minJoinCardinality == NO_SHARED_VARS) {
			final int i = getNextTailThatSharesVarsWithAncestry();
			if (i >= 0) {
				final long tailCardinality = cardinality(i);
				minJoinCardinality = tailCardinality;
				minTail = getTail(i);
				if (ASTStaticJoinOptimizer.log.isDebugEnabled())
					ASTStaticJoinOptimizer.log.debug("found a new min: "
							+ tailCardinality);
			}
		}

		/*
		 * If we are still at the "no shared variables" state, then simply order
		 * by range count and choose the min.
		 */
		if (minJoinCardinality == NO_SHARED_VARS) {
			minJoinCardinality = Long.MAX_VALUE;
			for (int i = 0; i < arity; i++) {
				// only check unused tails
				if (used[i]) {
					continue;
				}
				Tail tail = getTail(i);
				long tailCardinality = cardinality(i);
				if (tailCardinality < minJoinCardinality) {
					if (ASTStaticJoinOptimizer.log.isDebugEnabled())
						ASTStaticJoinOptimizer.log.debug("found a new min: "
								+ tailCardinality);
					minJoinCardinality = tailCardinality;
					minTail = tail;
				}
			}
		}

		// the join variables is the union of the join dimensions' variables
		Set<String> vars = new HashSet<String>();
		vars.addAll(d1.getVars());
		vars.addAll(minTail.getVars());
		return new Join(d1, minTail, minJoinCardinality, vars);
	}

	/**
	 * Return the range count for the predicate, ignoring any bindings. The
	 * range count for the tail predicate is cached the first time it is
	 * requested and returned from the cache thereafter. The range counts are
	 * requested using the "non-exact" range count query, so the range counts
	 * are actually the upper bound. However, if the upper bound is ZERO (0)
	 * then the range count really is ZERO (0).
	 * 
	 * @param tailIndex
	 *            The index of the predicate in the tail of the rule.
	 * 
	 * @return The range count for that tail predicate.
	 */
	public long rangeCount(final int tailIndex) {

		if (rangeCount[tailIndex] == -1L) {

			final long rangeCount = (long) nodes.get(tailIndex).getEstimatedCardinality(this);

			this.rangeCount[tailIndex] = rangeCount;

		}

		return rangeCount[tailIndex];

	}

	/**
	 * Return the cardinality of a particular tail, which is the range count if
	 * not optional and infinite if optional.
	 */
	public long cardinality(final int tailIndex) {
		return rangeCount(tailIndex);
	}

	public String toString() {
		return Arrays.toString(getOrder());
	}

	/**
	 * This is the secret sauce. There are three possibilities for computing the
	 * join cardinality, which we are defining as the upper-bound for solutions
	 * for a particular join. First, if there are no shared variables then the
	 * cardinality will just be the simple product of the cardinality of each
	 * join dimension. If there are shared variables but no unshared variables,
	 * then the cardinality will be the minimum cardinality from the join
	 * dimensions. If there are shared variables but also some unshared
	 * variables, then the join cardinality will be the maximum cardinality from
	 * each join dimension.
	 * <p>
	 * TODO: Any join involving an optional will have infinite cardinality, so
	 * that optionals get placed at the end.
	 * 
	 * @param d1
	 *            the first join dimension
	 * @param d2
	 *            the second join dimension
	 * @return the join cardinality
	 */
	protected long computeJoinCardinality(IJoinDimension d1, IJoinDimension d2) {
		// // two optionals is worse than one
		// if (d1.isOptional() && d2.isOptional()) {
		// return BOTH_OPTIONAL;
		// }
		// if (d1.isOptional() || d2.isOptional()) {
		// return ONE_OPTIONAL;
		// }
		final boolean sharedVars = hasSharedVars(d1, d2);
		final boolean unsharedVars = hasUnsharedVars(d1, d2);
		final long joinCardinality;
		if (sharedVars == false) {
			// no shared vars - take the sum
			// joinCardinality = d1.getCardinality() + d2.getCardinality();
			// different approach - give preference to shared variables
			joinCardinality = NO_SHARED_VARS;
		} else {
			if (unsharedVars == false) {
				// shared vars and no unshared vars - take the min
				joinCardinality = Math.min(d1.getCardinality(),
						d2.getCardinality());
			} else {
				// shared vars and unshared vars - take the max
				/*
				 * This modification to the join planner results in
				 * significantly faster queries for the bsbm benchmark (3x - 5x
				 * overall). It takes a more optimistic perspective on the
				 * intersection of two statement patterns, predicting that this
				 * will constraint, rather than increase, the multiplicity of
				 * the solutions. However, this COULD lead to pathological cases
				 * where the resulting join plan is WORSE than it would have
				 * been otherwise. For example, this change produces a 3x to 5x
				 * improvement in the BSBM benchmark results. However, it has a
				 * negative effect on LUBM Q2.
				 * 
				 * Update: Ok so just to go into a little detail - yesterday's
				 * change means we choose the join ordering based on an
				 * optimistic view of the cardinality of any particular join. If
				 * you have two triple patterns that share variables but that
				 * also have unshared variables, then technically the maximum
				 * cardinality of the join is the maximum range count of the two
				 * tails. But often the true cardinality of the join is closer
				 * to the minimum range count than the maximum. So yesterday we
				 * started assigning an expected cardinality for the join of the
				 * minimum range count rather than the maximum. What this means
				 * is that a lot of the time when those joins move toward the
				 * front of the line the query will do a lot better, but
				 * occasionally (LUBM 2), the query will do much much worse
				 * (when the true cardinality is closer to the max range count).
				 * 
				 * Today we put in an extra tie-breaker condition. We already
				 * had one tie-breaker - if two joins have the same expected
				 * cardinality we chose the one with the lower minimum range
				 * count. But the new tie-breaker is that if two joins have the
				 * same expected cardinality and minimum range count, we now
				 * chose the one that has the minimum range count on the other
				 * tail (the minimum maximum if that makes sense).
				 * 
				 * 11/14/2011: The static join order optimizer should consider
				 * the swing in stakes when choosing between either the MIN or
				 * the MAX of the cardinality of two join dimensions in order to
				 * decide which join to schedule next. Historically it took the
				 * MAX, but there are counter examples to that decision such as
				 * LUBM Q2. Subsequently it was modified to take the MIN, but
				 * BSBM BI Q1 is a counter example for that.
				 * 
				 * Modify the static optimizer to consider the swing in stakes
				 * between the choice of MAX versus MIN. I believe that this
				 * boils down to something like "If an incorrect guess of MIN
				 * would cause us to suffer a very bad MAX, then choose based on
				 * the MAX to avoid paying that penalty."
				 */
				joinCardinality = (long) ((long) (optimistic * Math.min(
						d1.getCardinality(), d2.getCardinality())) + ((1.0d - optimistic) * Math
						.max(d1.getCardinality(), d2.getCardinality())));
			}
		}
		return joinCardinality;
	}

	/**
	 * Get the named variables for a given tail. Is there a better way to do
	 * this?
	 * 
	 * @param tail
	 *            the tail
	 * @return the named variables
	 */
	protected Set<String> getVars(int tail) {
		final IReorderableNode node = nodes.get(tail);
//		if (ASTStaticJoinOptimizer.log.isDebugEnabled()) {
//			ASTStaticJoinOptimizer.log.debug(node);
//		}

		final Set<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();
		/*
		 * Changed recursive to true so that we get the right produced
		 * bindings out of UnionNodes so that they can be reordered
		 * correctly.
		 */
		sa.getDefinitelyProducedBindings(node, vars, true);

		final Set<String> varNames = new LinkedHashSet<String>();
		for (IVariable<?> v : vars)
			varNames.add(v.getName());

		return varNames;
	}

	/**
	 * Look for shared variables.
	 * 
	 * @param d1
	 *            the first join dimension
	 * @param d2
	 *            the second join dimension
	 * @return true if there are shared variables, false otherwise
	 */
	protected boolean hasSharedVars(IJoinDimension d1, IJoinDimension d2) {
		for (String var : d1.getVars()) {
			if (d2.getVars().contains(var)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Check to see if the specified tail shares any variables with the
	 * ancestry.
	 */
	protected boolean sharesVarsWithAncestry(final int tail) {

		final Set<IVariable<?>> tailVars = sa
				.getDefinitelyProducedBindings(nodes.get(tail),
						new LinkedHashSet<IVariable<?>>(), true/* recursive */);

		return !Collections.disjoint(ancestryVars, tailVars);

	}

	/**
	 * Get the next tail (unused, non-optional) that shares a var with the
	 * ancestry. If there are many, choose the one with the lowest cardinality.
	 * Return -1 if none are found.
	 */
	protected int getNextTailThatSharesVarsWithAncestry() {

		int nextTail = -1;
		long minCardinality = Long.MAX_VALUE;

		// give preferential treatment to a tail that shares variables with the
		// ancestry. collect all of them up and then choose the one
		// that has the lowest cardinality
		for (int i = 0; i < arity; i++) {
			// only check unused tails
			if (used[i]) {
				continue;
			}

			if (ASTStaticJoinOptimizer.log.isDebugEnabled()) {
				ASTStaticJoinOptimizer.log.debug("considering tail: "
						+ nodes.get(i));
			}

			if (ASTStaticJoinOptimizer.log.isDebugEnabled()) {
				ASTStaticJoinOptimizer.log.debug("vars: "
						+ Arrays.toString(getVars(i).toArray()));
			}

			if (sharesVarsWithAncestry(i)) {
				/*
				 * We have a shared var with the ancestry.
				 */
				final long tailCardinality = cardinality(i);
				if (tailCardinality < minCardinality) {
					nextTail = i;
					minCardinality = tailCardinality;
				}
			}

		}

		return nextTail;

	}

	/**
	 * Look for unshared variables.
	 * 
	 * @param d1
	 *            the first join dimension
	 * @param d2
	 *            the second join dimension
	 * @return true if there are unshared variables, false otherwise
	 */
	protected boolean hasUnsharedVars(IJoinDimension d1, IJoinDimension d2) {
		for (String var : d1.getVars()) {
			if (d2.getVars().contains(var) == false) {
				return true;
			}
		}
		for (String var : d2.getVars()) {
			if (d1.getVars().contains(var) == false) {
				return true;
			}
		}
		return false;
	}

	/**
	 * A join dimension can be either a tail, or a previous join. Either way we
	 * need to know its cardinality, its variables, and its tails.
	 */
	private interface IJoinDimension {
		long getCardinality();

		Set<String> getVars();

		String toJoinString();
		// boolean isOptional();
	}

	/**
	 * A join implementation of a join dimension. The join can consist of two
	 * tails, or one tail and another join. Theoretically it could be two joins
	 * as well, which might be a future optimization worth thinking about.
	 */
	private static class Join implements IJoinDimension {

		private final IJoinDimension d1, d2;
		private final long cardinality;
		private final Set<String> vars;

		public Join(IJoinDimension d1, IJoinDimension d2, long cardinality,
				Set<String> vars) {
			this.d1 = d1;
			this.d2 = d2;
			this.cardinality = cardinality;
			this.vars = vars;
		}

		public IJoinDimension getD1() {
			return d1;
		}

		public IJoinDimension getD2() {
			return d2;
		}

		public Set<String> getVars() {
			return vars;
		}

		public long getCardinality() {
			return cardinality;
		}

		public String toJoinString() {
			return d1.toJoinString() + " X " + d2.toJoinString();
		}

	}

	/**
	 * A tail implementation of a join dimension.
	 */
	private class Tail implements IJoinDimension {

		private final int tailIndex;
		private final long cardinality;
		private final Set<String> vars;

		public Tail(int tail, long cardinality, Set<String> vars) {
			this.tailIndex = tail;
			this.cardinality = cardinality;
			this.vars = vars;
		}

		public int getTailIndex() {
			return tailIndex;
		}

		public long getCardinality() {
			return cardinality;
		}

		public Set<String> getVars() {
			return vars;
		}

		// public boolean isOptional() {
		// return nodes.get(tail).isOptional();
		// }

		public String toJoinString() {
			return String.valueOf(tailIndex);
		}

	}

	public long getCardinality() {
		return cardinality;
	}

}