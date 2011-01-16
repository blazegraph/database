package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.Bound;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Group;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.MathExpr;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Not;
import org.openrdf.query.algebra.Or;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.Regex;
import org.openrdf.query.algebra.SameTerm;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.MathExpr.MathOp;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.evaluation.iterator.FilterIterator;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.IPredicate.Annotations;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.constraint.AND;
import com.bigdata.bop.constraint.BOUND;
import com.bigdata.bop.constraint.EQ;
import com.bigdata.bop.constraint.INBinarySearch;
import com.bigdata.bop.constraint.NE;
import com.bigdata.bop.constraint.NOT;
import com.bigdata.bop.constraint.OR;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.solutions.ISortOrder;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.rdf.internal.DummyIV;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.MathBOp;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.sop.SOp;
import com.bigdata.rdf.sail.sop.SOp2BOpUtility;
import com.bigdata.rdf.sail.sop.SOpTree;
import com.bigdata.rdf.sail.sop.SOpTreeBuilder;
import com.bigdata.rdf.sail.sop.UnsupportedOperatorException;
import com.bigdata.rdf.sail.sop.SOpTree.SOpGroup;
import com.bigdata.rdf.spo.DefaultGraphSolutionExpander;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.NamedGraphSolutionExpander;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.BigdataBindingSetResolverator;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IAccessPathExpander;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IQueryOptions;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.RuleStats;
import com.bigdata.search.FullTextIndex;
import com.bigdata.search.IHit;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.DistinctFilter;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.util.concurrent.Haltable;

/**
 * Extended to rewrite Sesame {@link TupleExpr}s onto native {@link Rule}s and
 * to evaluate magic predicates for full text search, etc. Query evaluation can
 * proceed either by Sesame 2 evaluation or, if {@link Options#NATIVE_JOINS} is
 * enabled, then by translation of Sesame 2 query expressions into native
 * {@link IRule}s and native evaluation of those {@link IRule}s.
 * 
 * <h2>Query options</h2>
 * The following summarizes how various high-level query language feature are
 * mapped onto native {@link IRule}s.
 * <dl>
 * <dt>DISTINCT</dt>
 * <dd>{@link IQueryOptions#isDistinct()}, which is realized using
 * {@link DistinctFilter}.</dd>
 * <dt>ORDER BY</dt>
 * <dd>{@link IQueryOptions#getOrderBy()} is effected by a custom
 * {@link IKeyBuilderFactory} which generates sort keys that capture the desired
 * sort order from the bindings in an {@link ISolution}. Unless DISTINCT is
 * also specified, the generated sort keys are made unique by appending a one up
 * long integer to the key - this prevents sort keys that otherwise compare as
 * equals from dropping solutions. Note that the SORT is actually imposed by the
 * {@link DistinctFilter} using an {@link IKeyBuilderFactory} assembled from the
 * ORDER BY constraints.
 * 
 * FIXME BryanT - implement the {@link IKeyBuilderFactory}.
 * 
 * FIXME MikeP - assemble the {@link ISortOrder}[] from the query and set on
 * the {@link IQueryOptions}.</dd>
 * <dt>OFFSET and LIMIT</dt>
 * <dd>
 * <p>
 * {@link IQueryOptions#getSlice()}, which was effected as a conditional in
 * the old "Nested Subquery With Join Threads Task" based on the
 * {@link RuleStats#solutionCount}. Query {@link ISolution}s are counted as
 * they are generated, but they are only entered into the {@link ISolution}
 * {@link IBuffer} when the solutionCount is GE the OFFSET and LT the LIMIT.
 * Query evaluation halts once the LIMIT is reached.
 * </p>
 * <p>
 * Note that when DISTINCT and either LIMIT and/or OFFSET are specified
 * together, then the LIMIT and OFFSET <strong>MUST</strong> be applied after
 * the solutions have been generated since we may have to generate more than
 * LIMIT solutions in order to have LIMIT <em>DISTINCT</em> solutions. We
 * handle this for now by NOT translating the LIMIT and OFFSET onto the
 * {@link IRule} and instead let Sesame close the iterator once it has enough
 * solutions.
 * </p>
 * <p>
 * Note that LIMIT and SLICE requires an evaluation plan that provides stable
 * results. For a simple query this is achieved by setting
 * {@link IQueryOptions#isStable()} to <code>true</code>.
 * <p>
 * For a UNION query, you must also set {@link IProgram#isParallel()} to
 * <code>false</code> to prevent parallelized execution of the {@link IRule}s
 * in the {@link IProgram}.
 * </p>
 * </dd>
 * <dt>UNION</dt>
 * <dd>A UNION is translated into an {@link IProgram} consisting of one
 * {@link IRule} for each clause in the UNION.
 * 
 * FIXME MikeP - implement.</dd>
 * </dl>
 * <h2>Filters</h2>
 * The following provides a summary of how various kinds of FILTER are handled.
 * A filter that is not explicitly handled is left untranslated and will be
 * applied by Sesame against the generated {@link ISolution}s.
 * <p>
 * Whenever possible, a FILTER is translated into an {@link IConstraint} on an
 * {@link IPredicate} in the generated native {@link IRule}. Some filters are
 * essentially JOINs against the {@link LexiconRelation}. Those can be handled
 * either as JOINs (generating an additional {@link IPredicate} in the
 * {@link IRule}) or as an {@link INBinarySearch} constraint, where the inclusion set is
 * pre-populated by some operation on the {@link LexiconRelation}.
 * <dl>
 * <dt>EQ</dt>
 * <dd>Translated into an {@link EQ} constraint on an {@link IPredicate}.</dd>
 * <dt>NE</dt>
 * <dd>Translated into an {@link NE} constraint on an {@link IPredicate}.</dd>
 * <dt>IN</dt>
 * <dd>Translated into an {@link INBinarySearch} constraint on an {@link IPredicate}.</dd>
 * <dt>OR</dt>
 * <dd>Translated into an {@link OR} constraint on an {@link IPredicate}.</dd>
 * <dt></dt>
 * <dd></dd>
 * </dl>
 * <h2>Magic predicates</h2>
 * <p>
 * {@link BD#SEARCH} is the only magic predicate at this time. When the object
 * position is bound to a constant, the magic predicate is evaluated once and
 * the result is used to generate a set of term identifiers that are matches for
 * the token(s) extracted from the {@link Literal} in the object position. Those
 * term identifiers are then used to populate an {@link INBinarySearch} constraint. The
 * object position in the {@link BD#SEARCH} MUST be bound to a constant.
 * </p>
 * 
 * FIXME We are not in fact rewriting the query operation at all, simply
 * choosing a different evaluation path as we go. The rewrite should really be
 * isolated from the execution, e.g., in its own class. That more correct
 * approach is more than I want to get into right now as we will have to define
 * variants on the various operators that let us model the native rule system
 * directly, e.g., an n-ary IProgram, n-ary IRule operator, an IPredicate
 * operator, etc. Then we can handle evaluation using their model with anything
 * re-written to our custom operators being caught by our custom evaluate()
 * methods and everything else running their default methods. Definitely the
 * right approach, and much easier to write unit tests.
 * 
 * @todo REGEX : if there is a &quot;&circ;&quot; literal followed by a wildcard
 *       AND there are no flags which would cause problems (case-folding, etc)
 *       then the REGEX can be rewritten as a prefix scan on the lexicon, which
 *       is very efficient, and converted to an IN filter. When the set size is
 *       huge we should rewrite it as another tail in the query instead.
 *       <p>
 *       Otherwise, regex filters are left outside of the rule. We can't
 *       optimize that until we generate rules that perform JOINs across the
 *       lexicon and the spo relations (which we could do, in which case it
 *       becomes a constraint on that join).
 *       <p>
 *       We don't have any indices that are designed to optimize regex scans,
 *       but we could process a regex scan as a parallel iterator scan against
 *       the lexicon.
 * 
 * @todo Roll more kinds of filters into the native {@link IRule}s as
 *       {@link IConstraint}s on {@link IPredicate}s.
 *       <p>
 *       isURI(), etc. can be evaluated by testing a bit flag on the term
 *       identifier, which is very efficient.
 *       <p>
 * 
 * @todo Verify handling of datatype operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BigdataEvaluationStrategyImpl.java 2272 2009-11-04 02:10:19Z
 *          mrpersonick $
 */
public class BigdataEvaluationStrategyImpl3 extends EvaluationStrategyImpl 
		implements BigdataEvaluationStrategy {
    
    /**
     * Logger.
     */
    protected static final Logger log = 
        Logger.getLogger(BigdataEvaluationStrategyImpl3.class);

    protected final BigdataTripleSource tripleSource;

    protected final Dataset dataset;

    private final AbstractTripleStore database;

    /**
     */
    public BigdataEvaluationStrategyImpl3(
            final BigdataTripleSource tripleSource, final Dataset dataset,
            final boolean nativeJoins, 
            final boolean allowSesameQueryEvaluation) {
        
        super(tripleSource, dataset);
        
        this.tripleSource = tripleSource;
        this.dataset = dataset;
        this.database = tripleSource.getDatabase();
        this.nativeJoins = nativeJoins;
        this.allowSesameQueryEvaluation = allowSesameQueryEvaluation;
        
    }

    /**
     * If true, use native evaluation on the sesame operator tree if possible.
     */
    private boolean nativeJoins;
    
    /**
     * If true, allow queries that cannot be executed natively to be executed
     * by Sesame.
     */
    private final boolean allowSesameQueryEvaluation;
    
    /**
     * A set of properties that act as query hints during evaluation.
     */
    private Properties queryHints;
    
    /**
     * This is the top-level method called by the SAIL to evaluate a query.
     * The TupleExpr parameter here is guaranteed to be the root of the operator
     * tree for the query.  Query hints are parsed by the SAIL from the
     * namespaces in the original query.  See {@link QueryHints#NAMESPACE}.
     * <p>
     * The query root will be handled by the native Sesame evaluation until we
     * reach one of three possible top-level operators (union, join, or left
     * join) at which point we will take over and translate the sesame operator
     * tree into a native bigdata query.  If in the process of this translation
     * we encounter an operator that we cannot handle natively, we will log
     * a warning message and punt to Sesame to let it handle the entire
     * query evaluation process (much slower than native evaluation).
     */
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            final TupleExpr expr, final BindingSet bindings, 
            final Properties queryHints)
            throws QueryEvaluationException {
        
        // spit out the whole operator tree
        if (log.isInfoEnabled()) {
            log.info("operator tree:\n" + expr);
        }

        this.queryHints = queryHints;

        if (log.isInfoEnabled()) {
            log.info("queryHints:\n" + queryHints);
        }
        
        return super.evaluate(expr, bindings);
        
    }

    /**
     * Translate top-level UNIONs into native bigdata programs for execution.
     * This will attempt to look down the operator tree from this point and turn
     * the Sesame operators into a set of native rules within a single program.
     * <p>
     * FIXME A Union is a BinaryTupleOperator composed of two expressions.  This 
     * native evaluation only handles the special case where the left and right
     * args are one of: {Join, LeftJoin, StatementPattern, Union}.  It's
     * possible that the left or right arg is something other than one of those
     * operators, in which case we punt to the Sesame evaluation, which
     * degrades performance.
     * <p>
     * FIXME Also, even if the left or right arg is one of the cases we handle, 
     * it's possible that the translation of that arg into a native rule will 
     * fail because of an unsupported SPARQL language feature, such as an 
     * embedded UNION or an unsupported filter type.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            final Union union, final BindingSet bs) 
            throws QueryEvaluationException {
        
        if (!nativeJoins) {
            // Use Sesame 2 evaluation
            return super.evaluate(union, bs);
        }

        if (log.isInfoEnabled()) {
            log.info("evaluating top-level Union operator");
        }
        
        try {
            
            return evaluateNatively(union, bs);
            
        } catch (UnsupportedOperatorException ex) {
            
        	if (allowSesameQueryEvaluation) {
	            
        		// Use Sesame 2 evaluation
	            
        		log.warn("could not evaluate natively, using Sesame evaluation"); 
	            
        		if (log.isInfoEnabled()) {
	                log.info(ex.getOperator());
	            }
	            
        		// turn off native joins for the remainder, we can't do
	            // partial execution
	            nativeJoins = false;
	            
	            // defer to Sesame
	            return super.evaluate(union, bs);
	            
        	} else {
        		
        		// allow the query to fail
				throw new UnsupportedOperatorException(ex);
        		
        	}

        }
        
    }
    
    /**
     * Translate top-level JOINs into native bigdata programs for execution.
     * This will attempt to look down the operator tree from this point and turn
     * the Sesame operators into a native rule.
     * <p>
     * FIXME It's possible that the translation of the left or right arg into a 
     * native rule will fail because of an unsupported SPARQL language feature, 
     * such as an embedded UNION or an unsupported filter type.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            final Join join, final BindingSet bs) 
            throws QueryEvaluationException {
        
        if (!nativeJoins) {
            // Use Sesame 2 evaluation
            return super.evaluate(join, bs);
        }

        if (log.isInfoEnabled()) {
            log.info("evaluating top-level Join operator");
        }
        
        try {
            
            return evaluateNatively(join, bs);
            
        } catch (UnsupportedOperatorException ex) {
            
        	if (allowSesameQueryEvaluation) {
	            
        		// Use Sesame 2 evaluation
	            
        		log.warn("could not evaluate natively, using Sesame evaluation"); 
	            
        		if (log.isInfoEnabled()) {
	                log.info(ex.getOperator());
	            }
	            
        		// turn off native joins for the remainder, we can't do
	            // partial execution
	            nativeJoins = false;
	            
	            // defer to Sesame
	            return super.evaluate(join, bs);
	            
        	} else {
        		
        		// allow the query to fail
        		throw new UnsupportedOperatorException(ex);
        		
        	}
            
        }
        
    }
    
    /**
     * Translate top-level LEFTJOINs into native bigdata programs for execution.
     * This will attempt to look down the operator tree from this point and turn
     * the Sesame operators into a native rule.
     * <p>
     * FIXME It's possible that the translation of the left or right arg into a 
     * native rule will fail because of an unsupported SPARQL language feature, 
     * such as an embedded UNION or an unsupported filter type.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            final LeftJoin leftJoin, final BindingSet bs) 
            throws QueryEvaluationException {
        
        if (!nativeJoins) {
            // Use Sesame 2 evaluation
            return super.evaluate(leftJoin, bs);
        }

        if (log.isInfoEnabled()) {
            log.info("evaluating top-level LeftJoin operator");
        }
        
        try {
            
            return evaluateNatively(leftJoin, bs);
            
        } catch (UnsupportedOperatorException ex) {
            
        	if (allowSesameQueryEvaluation) {
	            
        		// Use Sesame 2 evaluation
	            
        		log.warn("could not evaluate natively, using Sesame evaluation"); 
	            
        		if (log.isInfoEnabled()) {
	                log.info(ex.getOperator());
	            }
	            
        		// turn off native joins for the remainder, we can't do
	            // partial execution
	            nativeJoins = false;
	            
	            // defer to Sesame
	            return super.evaluate(leftJoin, bs);
	            
        	} else {
        		
        		// allow the query to fail
        		throw new UnsupportedOperatorException(ex);
        		
        	}
            
        }
        
    }
    
    private CloseableIteration<BindingSet, QueryEvaluationException> 
		evaluateNatively(final TupleExpr tupleExpr, final BindingSet bs) 
		    throws QueryEvaluationException, UnsupportedOperatorException {
		try {
			return _evaluateNatively(tupleExpr, bs);
		} catch (UnrecognizedValueException ex) {
			if (log.isInfoEnabled()) {
				log.info("unrecognized value in query: " + ex.getValue());
			}
			return new EmptyIteration<BindingSet, QueryEvaluationException>();
		} catch(UnsupportedOperatorException ex) {
			/*
			 * Note: Do not wrap as a different exception type. The caller is
			 * looking for this.
			 */
			throw new UnsupportedOperatorException(ex);
		} catch (Throwable ex) {
//			log.error("Remove log stmt:"+ex,ex);// FIXME remove this - I am just looking for the root cause of something in the SAIL.
			throw new QueryEvaluationException(ex);
		}
	}

    private CloseableIteration<BindingSet, QueryEvaluationException> 
    	_evaluateNatively(final TupleExpr root, final BindingSet bs)
    		throws UnsupportedOperatorException, UnrecognizedValueException, 
    				QueryEvaluationException {
    	
    	final SOpTreeBuilder stb = new SOpTreeBuilder();
    	
    	/*
    	 * The sesame operator tree
    	 */
    	SOpTree sopTree;

    	/*
    	 * Turn the Sesame operator tree into something a little easier
    	 * to work with.
    	 */
    	sopTree = stb.collectSOps(root);

    	/*
    	 * We need to prune groups that contain terms that do not appear in
    	 * our lexicon.
    	 */
    	final Collection<SOpGroup> groupsToPrune = new LinkedList<SOpGroup>();
    	
    	/*
    	 * Iterate through the sop tree and translate statement patterns into
    	 * predicates.
    	 */
    	for (SOp sop : sopTree) {
    		final QueryModelNode op = sop.getOperator();
    		if (op instanceof StatementPattern) {
    			final StatementPattern sp = (StatementPattern) op;
    			try {
    				final IPredicate bop = toPredicate((StatementPattern) op);
    				sop.setBOp(bop);
    			} catch (UnrecognizedValueException ex) {
    				/*
    				 * If we encounter a value not in the lexicon, we can
    				 * still continue with the query if the value is in
    				 * either an optional tail or an optional join group (i.e.
    				 * if it appears on the right side of a LeftJoin).
    				 * Otherwise we can stop evaluating right now. 
    				 */
    				if (sop.isRightSideLeftJoin()) {
    					groupsToPrune.add(sopTree.getGroup(sop.getGroup()));
    				} else {
    					throw new UnrecognizedValueException(ex);
    				}
    			}
    		}
    	}
    	
    	/*
    	 * Prunes the sop tree of optional join groups containing values
    	 * not in the lexicon.
    	 */
    	sopTree = stb.pruneGroups(sopTree, groupsToPrune);
    	
    	/*
    	 * If we have a filter in the root group (one that can be safely applied
    	 * across the entire query) that we cannot translate into a native
    	 * bigdata constraint, we can run it as a FilterIterator after the
    	 * query has run natively.
    	 */
    	final Collection<Filter> sesameFilters = new LinkedList<Filter>();
    	
    	/*
    	 * We need to prune Sesame filters that we cannot translate into native
    	 * constraints (ones that require lexicon joins).
    	 */
    	final Collection<SOp> sopsToPrune = new LinkedList<SOp>();
    	
    	/*
    	 * Iterate through the sop tree and translate Sesame ValueExpr operators
    	 * into bigdata IConstraint boperators.
    	 */
    	for (SOp sop : sopTree) {
    		final QueryModelNode op = sop.getOperator();
    		if (op instanceof ValueExpr) {
    			/*
    			 * If we have a raw ValueExpr and not a Filter we know it must
    			 * be the condition of a LeftJoin, in which case we cannot
    			 * use the Sesame FilterIterator to safely evaluate it.  A
    			 * UnsupportedOperatorException here must just flow through
    			 * to Sesame evaluation of the entire query.
    			 */
//    			if (op instanceof Regex) {
//        			final Regex regex = (Regex) op;
//    				final IPredicate bop = toPredicate(regex);
//    				sop.setBOp(bop);
//    			} else {
	    			final ValueExpr ve = (ValueExpr) op;
					final IConstraint bop = toConstraint(ve);
					sop.setBOp(bop);
//    			}
    		} else if (op instanceof Filter) {
    			final Filter filter = (Filter) op;
    			final ValueExpr ve = filter.getCondition();
    			try {
//    				if (ve instanceof Regex) {
//            			final Regex regex = (Regex) ve;
//        				final IPredicate bop = toPredicate(regex);
//        				sop.setBOp(bop);
//        			} else {
        				final IConstraint bop = toConstraint(ve);
        				sop.setBOp(bop);
//        			}
    			} catch (UnsupportedOperatorException ex) {
    				/*
    				 * If we encounter a sesame filter (ValueExpr) that we
    				 * cannot translate, we can safely wrap the entire query
    				 * with a Sesame filter iterator to capture that
    				 * untranslatable value expression.  If we are not in the
    				 * root group however, we risk applying the filter to the
    				 * wrong context (for example a filter inside an optional
    				 * join group cannot be applied universally to the entire
    				 * solution).  In this case we must punt. 
    				 */
    				if (sop.getGroup() == SOpTreeBuilder.ROOT_GROUP_ID) {
    					sopsToPrune.add(sop);
    					sesameFilters.add(filter);
					} else {
						/*
						 * Note: DO NOT wrap with a different exception type -
						 * the caller is looking for this.
						 */
    					throw new UnsupportedOperatorException(ex);
    				}
    			}
    		}
    	}
    	
    	/*
    	 * Prunes the sop tree of untranslatable filters.
    	 */
    	sopTree = stb.pruneSOps(sopTree, sopsToPrune);
    	
    	/*
    	 * Make sure we don't have free text searches searching outside
    	 * their named graph scope.
    	 */
    	attachNamedGraphsFilterToSearches(sopTree);
    	
    	/*
    	 * Gather variables required by Sesame outside of the query
    	 * evaluation (projection and global sesame filters).
    	 */
    	final IVariable[] required = 
    		gatherRequiredVariables(root, sesameFilters);
    	
    	sopTree.setRequiredVars(required);
    	
        final QueryEngine queryEngine = tripleSource.getSail().getQueryEngine();

		final PipelineOp query;
		{
			/*
			 * Note: The ids are assigned using incrementAndGet() so ONE (1) is
			 * the first id that will be assigned when we pass in ZERO (0) as
			 * the initial state of the AtomicInteger.
			 */
			final AtomicInteger idFactory = new AtomicInteger(0);

			// Convert the step to a bigdata operator tree.
			query = SOp2BOpUtility.convert(sopTree, idFactory, database,
					queryEngine, queryHints);

			if (log.isInfoEnabled())
				log.info(BOpUtility.toString(query));

		}

		/*
		 * Begin native bigdata evaluation.
		 */
		CloseableIteration<BindingSet, QueryEvaluationException> result = _evaluateNatively(
				query, bs, queryEngine);// , sesameFilters);

		/*
		 * Use the basic filter iterator for any remaining filters which will be
		 * evaluated by Sesame.
		 * 
		 * Note: Some Sesame filters may pre-fetch one or more result(s). This
		 * could potentially cause the IRunningQuery to be asynchronously
		 * terminated by an interrupt. I have lifted the code to wrap the Sesame
		 * filters around the bigdata evaluation out of the code which starts
		 * the IRunningQuery evaluation in order to help clarify such
		 * circumstances as they might relate to [1].
		 * 
		 * [1] https://sourceforge.net/apps/trac/bigdata/ticket/230
		 */
		if (sesameFilters != null) {
			for (Filter f : sesameFilters) {
				if (log.isDebugEnabled()) {
					log.debug("attaching sesame filter: " + f);
				}
				result = new FilterIterator(f, result, this);
			}
		}

		return result;

    }
    
    private CloseableIteration<BindingSet, QueryEvaluationException> 
		_evaluateNatively(final PipelineOp query, final BindingSet bs,
			final QueryEngine queryEngine 
//			, final Collection<Filter> sesameFilters
			) 
			throws QueryEvaluationException {
	    
	    IRunningQuery runningQuery = null;
    	try {
    		
    		// Submit query for evaluation.
    		runningQuery = queryEngine.eval(query);

			/*
			 * Wrap up the native bigdata query solution iterator as Sesame
			 * compatible iteration with materialized RDF Values.
			 */
			return wrapQuery(runningQuery);//, sesameFilters);

		} catch (UnsupportedOperatorException t) {
			if (runningQuery != null) {
				// ensure query is halted.
				runningQuery.cancel(true/* mayInterruptIfRunning */);
			}
			/*
			 * Note: Do not wrap as a different exception type. The caller is
			 * looking for this.
			 */
			throw new UnsupportedOperatorException(t);
		} catch (Throwable t) {
			if (runningQuery != null) {
				// ensure query is halted.
				runningQuery.cancel(true/* mayInterruptIfRunning */);
			}
//			log.error("Remove log stmt"+t,t);// FIXME remove this - I am just looking for the root cause of something in the SAIL.
			throw new QueryEvaluationException(t);
		}
    	
	}

	/**
	 * Wrap the {@link IRunningQuery#iterator()}, returning a Sesame compatible
	 * iteration which will visit Sesame binding sets having materialized RDF
	 * Values.
	 * 
	 * @param runningQuery
	 *            The query.
	 * 
	 * @return The iterator.
	 * 
	 * @throws QueryEvaluationException
	 */
	private CloseableIteration<BindingSet, QueryEvaluationException> wrapQuery(
			final IRunningQuery runningQuery
			) throws QueryEvaluationException {

		// The iterator draining the query solutions.
		final IAsynchronousIterator<IBindingSet[]> it1 = runningQuery
				.iterator();

	    // De-chunk the IBindingSet[] visited by that iterator.
	    final IChunkedOrderedIterator<IBindingSet> it2 = 
	    	new ChunkedWrappedIterator<IBindingSet>(
	            new Dechunkerator<IBindingSet>(it1));

	    // Materialize IVs as RDF Values.
	    final CloseableIteration<BindingSet, QueryEvaluationException> result =
	    	// Monitor IRunningQuery and cancel if Sesame iterator is closed.
	    	new RunningQueryCloseableIteration<BindingSet, QueryEvaluationException>(runningQuery,
			// Convert bigdata binding sets to Sesame binding sets.
	        new Bigdata2Sesame2BindingSetIterator<QueryEvaluationException>(
        		// Materialize IVs as RDF Values.
	            new BigdataBindingSetResolverator(database, it2).start(
	            		database.getExecutorService())));
	
	    return result;

    }
    
//    /**
//     * This is the method that will attempt to take a top-level join or left
//     * join and turn it into a native bigdata rule. The Sesame operators Join
//     * and LeftJoin share only the common base class BinaryTupleOperator, but
//     * other BinaryTupleOperators are not supported by this method. Other
//     * specific types of BinaryTupleOperators will cause this method to throw
//     * an exception.
//     * <p>
//     * This method will also turn a single top-level StatementPattern into a
//     * rule with one predicate in it.
//     * <p>
//     * Note: As a pre-condition, the {@link Value}s in the query expression
//     * MUST have been rewritten as {@link BigdataValue}s and their term
//     * identifiers MUST have been resolved. Any term identifier that remains
//     * {@link IRawTripleStore#NULL} is an indication that there is no entry for
//     * that {@link Value} in the database. Since the JOINs are required (vs
//     * OPTIONALs), that means that there is no solution for the JOINs and an
//     * {@link EmptyIteration} is returned rather than evaluating the query.
//     * 
//     * @param join
//     * @return native bigdata rule
//     * @throws UnsupportedOperatorException
//     *          this exception will be thrown if the Sesame join contains any
//     *          SPARQL language constructs that cannot be converted into
//     *          the bigdata native rule model
//     * @throws QueryEvaluationException
//     */
//    private IRule createNativeQueryOld(final TupleExpr join)
//            throws UnsupportedOperatorException,
//            QueryEvaluationException {
//
//        if (!(join instanceof StatementPattern || 
//              join instanceof Join || join instanceof LeftJoin || 
//              join instanceof Filter)) {
//            throw new AssertionError(
//                    "only StatementPattern, Join, and LeftJoin supported");
//        }
//
//        // flattened collection of statement patterns nested within this join,
//        // along with whether or not each one is optional
//        final Map<StatementPattern, Boolean> stmtPatterns = 
//            new LinkedHashMap<StatementPattern, Boolean>();
//        // flattened collection of filters nested within this join
//        final Collection<Filter> filters = new LinkedList<Filter>();
//        
//        // will throw EncounteredUnknownTupleExprException if the join
//        // contains something we don't handle yet
////        collectStatementPatterns(join, stmtPatterns, filters);
//        
//        if (false) {
//            for (Map.Entry<StatementPattern, Boolean> entry : 
//                    stmtPatterns.entrySet()) {
//                log.debug(entry.getKey() + ", optional=" + entry.getValue());
//            }
//            for (Filter filter : filters) {
//                log.debug(filter.getCondition());
//            }
//        }
//        
//        // generate tails
//        Collection<IPredicate> tails = new LinkedList<IPredicate>();
//        // keep a list of free text searches for later to solve a named graphs
//        // problem
//        final Map<IPredicate, StatementPattern> searches = 
//            new HashMap<IPredicate, StatementPattern>();
//        for (Map.Entry<StatementPattern, Boolean> entry : stmtPatterns
//                .entrySet()) {
//            StatementPattern sp = entry.getKey();
//            boolean optional = entry.getValue();
//            IPredicate tail = toPredicate(sp, optional);
//            // encountered a value not in the database lexicon
//            if (tail == null) {
//                if (log.isDebugEnabled()) {
//                    log.debug("could not generate tail for: " + sp);
//                }
//                if (optional) {
//                    // for optionals, just skip the tail
//                    continue;
//                } else {
//                    // for non-optionals, skip the entire rule
//                    return null;
//                }
//            }
//            if (tail.getAccessPathExpander() instanceof FreeTextSearchExpander) {
//                searches.put(tail, sp);
//            }
//            tails.add(tail);
//        }
//        
//        /*
//         * When in quads mode, we need to go through the free text searches and
//         * make sure that they are properly filtered for the dataset where
//         * needed. Joins will take care of this, so we only need to add a filter
//         * when a search variable does not appear in any other tails that are
//         * non-optional.
//         * 
//         * @todo Bryan seems to think this can be fixed with a DISTINCT JOIN
//         * mechanism in the rule evaluation.
//         */
//        if (database.isQuads() && dataset != null) {
//            for (IPredicate search : searches.keySet()) {
//                final Set<URI> graphs;
//                StatementPattern sp = searches.get(search);
//                switch (sp.getScope()) {
//                case DEFAULT_CONTEXTS: {
//                    /*
//                     * Query against the RDF merge of zero or more source
//                     * graphs.
//                     */
//                    graphs = dataset.getDefaultGraphs();
//                    break;
//                }
//                case NAMED_CONTEXTS: {
//                    /*
//                     * Query against zero or more named graphs.
//                     */
//                    graphs = dataset.getNamedGraphs();
//                    break;
//                }
//                default:
//                    throw new AssertionError();
//                }
//                if (graphs == null) {
//                    continue;
//                }
//                // why would we use a constant with a free text search???
//                if (search.get(0).isConstant()) {
//                    throw new AssertionError();
//                }
//                // get ahold of the search variable
//                com.bigdata.bop.Var searchVar = 
//                    (com.bigdata.bop.Var) search.get(0);
//                if (log.isDebugEnabled()) {
//                    log.debug(searchVar);
//                }
//                // start by assuming it needs filtering, guilty until proven
//                // innocent
//                boolean needsFilter = true;
//                // check the other tails one by one
//                for (IPredicate<ISPO> tail : tails) {
//                    IAccessPathExpander<ISPO> expander = 
//                        tail.getAccessPathExpander();
//                    // only concerned with non-optional tails that are not
//                    // themselves magic searches
//                    if (expander instanceof FreeTextSearchExpander
//                            || tail.isOptional()) {
//                        continue;
//                    }
//                    // see if the search variable appears in this tail
//                    boolean appears = false;
//                    for (int i = 0; i < tail.arity(); i++) {
//                        IVariableOrConstant term = tail.get(i);
//                        if (log.isDebugEnabled()) {
//                            log.debug(term);
//                        }
//                        if (term.equals(searchVar)) {
//                            appears = true;
//                            break;
//                        }
//                    }
//                    // if it appears, we don't need a filter
//                    if (appears) {
//                        needsFilter = false;
//                        break;
//                    }
//                }
//                // if it needs a filter, add it to the expander
//                if (needsFilter) {
//                    if (log.isDebugEnabled()) {
//                        log.debug("needs filter: " + searchVar);
//                    }
//                    FreeTextSearchExpander expander = (FreeTextSearchExpander) 
//                            search.getAccessPathExpander();
//                    expander.addNamedGraphsFilter(graphs);
//                }
//            }
//        }
//        
//        // generate constraints
//        final Collection<IConstraint> constraints = 
//            new LinkedList<IConstraint>();
//        final Iterator<Filter> filterIt = filters.iterator();
//        while (filterIt.hasNext()) {
//            final Filter filter = filterIt.next();
//            final IConstraint constraint = toConstraint(filter.getCondition());
//            if (constraint != null) {
//                // remove if we are able to generate a native constraint for it
//                if (log.isDebugEnabled()) {
//                    log.debug("able to generate a constraint: " + constraint);
//                }
//                filterIt.remove();
//                constraints.add(constraint);
//            }
//        }
//        
//        /*
//         * FIXME Native slice, DISTINCT, etc. are all commented out for now.
//         * Except for ORDER_BY, support exists for all of these features in the
//         * native rules, but we need to separate the rewrite of the tupleExpr
//         * and its evaluation in order to properly handle this stuff.
//         */
//        IQueryOptions queryOptions = QueryOptions.NONE;
//        // if (slice) {
//        // if (!distinct && !union) {
//        // final ISlice slice = new Slice(offset, limit);
//        // queryOptions = new QueryOptions(false/* distinct */,
//        // true/* stable */, null/* orderBy */, slice);
//        // }
//        // } else {
//        // if (distinct && !union) {
//        // queryOptions = QueryOptions.DISTINCT;
//        // }
//        // }
//        
////        if (log.isDebugEnabled()) {
////            for (IPredicate<ISPO> tail : tails) {
////                IAccessPathExpander<ISPO> expander = tail.getAccessPathExpander();
////                if (expander != null) {
////                    IAccessPath<ISPO> accessPath = database.getSPORelation()
////                            .getAccessPath(tail);
////                    accessPath = expander.getAccessPath(accessPath);
////                    IChunkedOrderedIterator<ISPO> it = accessPath.iterator();
////                    while (it.hasNext()) {
////                        log.debug(it.next().toString(database));
////                    }
////                }
////            }
////        }
//        
//        /*
//         * Collect a set of variables required beyond just the join (i.e.
//         * aggregation, projection, filters, etc.)
//         */
//        Set<String> required = new HashSet<String>(); 
//        
//        try {
//            
//            QueryModelNode p = join;
//            while (true) {
//                p = p.getParentNode();
//                if (log.isDebugEnabled()) {
//                    log.debug(p.getClass());
//                }
//                if (p instanceof UnaryTupleOperator) {
//                    required.addAll(collectVariables((UnaryTupleOperator) p));
//                }
//                if (p instanceof QueryRoot) {
//                    break;
//                }
//            }
//
//            if (filters.size() > 0) {
//                for (Filter filter : filters) {
//                    required.addAll(collectVariables((UnaryTupleOperator) filter));
//                }
//            }
//            
//        } catch (Exception ex) {
//            throw new QueryEvaluationException(ex);
//        }
//
//        IVariable[] requiredVars = new IVariable[required.size()];
//        int i = 0;
//        for (String v : required) {
//            requiredVars[i++] = com.bigdata.bop.Var.var(v);
//        }
//        
//        if (log.isDebugEnabled()) {
//            log.debug("required binding names: " + Arrays.toString(requiredVars));
//        }
//        
////        if (starJoins) { // database.isQuads() == false) {
////            if (log.isDebugEnabled()) {
////                log.debug("generating star joins");
////            }
////            tails = generateStarJoins(tails);
////        }
//        
//        // generate native rule
//        IRule rule = new Rule("nativeJoin", 
//                // @todo should serialize the query string here for the logs.
//                null, // head
//                tails.toArray(new IPredicate[tails.size()]), queryOptions,
//                // constraints on the rule.
//                constraints.size() > 0 ? constraints
//                        .toArray(new IConstraint[constraints.size()]) : null,
//                null/* constants */, null/* taskFactory */, requiredVars);
//        
//        if (BigdataStatics.debug) {
//            System.err.println(join.toString());
//            System.err.println(rule.toString());
//        }
//
//        // we have filters that we could not translate natively
//        if (filters.size() > 0) {
//            if (log.isDebugEnabled()) {
//                log.debug("could not translate " + filters.size()
//                        + " filters into native constraints:");
//                for (Filter filter : filters) {
//                    log.debug("\n" + filter.getCondition());
//                }
//            }
//            // use the basic filter iterator for remaining filters
////            rule = new ProxyRuleWithSesameFilters(rule, filters);
//        }
//        
//        return rule;
//        
//    }
    
    private void attachNamedGraphsFilterToSearches(final SOpTree sopTree) {
    	
        /*
         * When in quads mode, we need to go through the free text searches and
         * make sure that they are properly filtered for the dataset where
         * needed. Joins will take care of this, so we only need to add a filter
         * when a search variable does not appear in any other tails that are
         * non-optional.
         * 
         * @todo Bryan seems to think this can be fixed with a DISTINCT JOIN
         * mechanism in the rule evaluation.
         */
        if (database.isQuads() && dataset != null) {
//            for (IPredicate search : searches.keySet()) {
        	for (SOp sop : sopTree) {
        		final QueryModelNode op = sop.getOperator();
        		if (!(op instanceof StatementPattern)) {
        			continue;
        		}
    			final StatementPattern sp = (StatementPattern) op;
    			final IPredicate pred = (IPredicate) sop.getBOp();
    			if (!(pred.getAccessPathExpander() 
    					instanceof FreeTextSearchExpander)) {
    				continue;
    			}
    			final FreeTextSearchExpander expander = (FreeTextSearchExpander) 
    				pred.getAccessPathExpander();
        		
                final Set<URI> graphs;
                switch (sp.getScope()) {
                case DEFAULT_CONTEXTS: {
                    /*
                     * Query against the RDF merge of zero or more source
                     * graphs.
                     */
                    graphs = dataset.getDefaultGraphs();
                    break;
                }
                case NAMED_CONTEXTS: {
                    /*
                     * Query against zero or more named graphs.
                     */
                    graphs = dataset.getNamedGraphs();
                    break;
                }
                default:
                    throw new AssertionError();
                }
                
                if (graphs == null) {
                    continue;
                }
                
                // get ahold of the search variable
                com.bigdata.bop.Var searchVar = 
                    (com.bigdata.bop.Var) pred.get(0);

                // start by assuming it needs filtering, guilty until proven
                // innocent
                boolean needsFilter = true;
                // check the other tails one by one
                for (SOp sop2 : sopTree) {
                	if (!(sop2.getOperator() instanceof StatementPattern)) {
                		continue;
                	}
        			final IPredicate pred2 = (IPredicate) sop.getBOp();
        			final IAccessPathExpander expander2 = 
        				pred.getAccessPathExpander();
                    // only concerned with non-optional tails that are not
                    // themselves magic searches
        			if (expander instanceof FreeTextSearchExpander) {
        				continue;
        			}
        			if (sop2.isRightSideLeftJoin()) {
        				continue;
        			}
                	
                    // see if the search variable appears in this tail
                    boolean appears = false;
                    for (int i = 0; i < pred2.arity(); i++) {
                        IVariableOrConstant term = pred2.get(i);
                        if (log.isDebugEnabled()) {
                            log.debug(term);
                        }
                        if (term.equals(searchVar)) {
                            appears = true;
                            break;
                        }
                    }
                    // if it appears, we don't need a filter
                    if (appears) {
                        needsFilter = false;
                        break;
                    }
                }
                // if it needs a filter, add it to the expander
                if (needsFilter) {
                    if (log.isDebugEnabled()) {
                        log.debug("needs filter: " + searchVar);
                    }
                    expander.addNamedGraphsFilter(graphs);
                }
            }
        }
        
    }

    protected IVariable[] gatherRequiredVariables(final TupleExpr root, 
    		final Collection<Filter> sesameFilters) {
    	
        /*
         * Collect a set of variables required beyond just the join (i.e.
         * aggregation, projection, filters, etc.)
         */
        Set<String> required = new HashSet<String>(); 
        
        QueryModelNode p = root;
        while (true) {
            p = p.getParentNode();
            if (log.isDebugEnabled()) {
                log.debug(p.getClass());
            }
            if (p instanceof UnaryTupleOperator) {
                required.addAll(collectVariables((UnaryTupleOperator) p));
            }
            if (p instanceof QueryRoot) {
                break;
            }
        }

        if (sesameFilters.size() > 0) {
            for (Filter f : sesameFilters) {
                required.addAll(collectVariables(f.getCondition()));
            }
        }

        final IVariable[] requiredVars = new IVariable[required.size()];
        int i = 0;
        for (String v : required) {
            requiredVars[i++] = com.bigdata.bop.Var.var(v);
        }
        
        if (log.isDebugEnabled()) {
            log.debug("required binding names: " + Arrays.toString(requiredVars));
        }
        
        return requiredVars;

    }
    
    /**
     * Collect the variables used by this <code>UnaryTupleOperator</code> so
     * they can be added to the list of required variables in the query for
     * correct binding set pruning.
     * 
     * @param op
     *          the <code>UnaryTupleOperator</code>
     * @return
     *          the variables it uses
     */
    protected Set<String> collectVariables(final QueryModelNode op) {

        final Set<String> vars = new HashSet<String>();
        if (op instanceof Projection) {
            final List<ProjectionElem> elems = 
                ((Projection) op).getProjectionElemList().getElements();
            for (ProjectionElem elem : elems) {
                vars.add(elem.getSourceName());
            }
        } else if (op instanceof MultiProjection) {
            final List<ProjectionElemList> elemLists = 
                ((MultiProjection) op).getProjections();
            for (ProjectionElemList list : elemLists) {
                List<ProjectionElem> elems = list.getElements();
                for (ProjectionElem elem : elems) {
                    vars.add(elem.getSourceName());
                }
            }
        } else if (op instanceof ValueExpr) {
            final ValueExpr ve = (ValueExpr) op;
            ve.visit(new QueryModelVisitorBase<RuntimeException>() {
                @Override
                public void meet(Var v) {
                    vars.add(v.getName());
                }
            });
        } else if (op instanceof Group) {
            final Group g = (Group) op;
            g.visit(new QueryModelVisitorBase<RuntimeException>() {
                @Override
                public void meet(Var v) {
                    vars.add(v.getName());
                }
            });
        } else if (op instanceof Order) {
            final Order o = (Order) op;
            o.visit(new QueryModelVisitorBase<RuntimeException>() {
                @Override
                public void meet(Var v) {
                    vars.add(v.getName());
                }
            });
        }
        return vars;

    }
    
//    /**
//     * This method will take a Union and attempt to turn it into a native
//     * bigdata program. If either the left or right arg is a Union, the method
//     * will act recursively to flatten the nested Unions into a single program.
//     * <p>
//     * See comments for {@link #evaluate(Union, BindingSet)}.
//     * 
//     * @param union
//     * @return native bigdata program
//     * @throws UnsupportedOperatorException
//     *             this exception will be thrown if the Sesame join contains any
//     *             SPARQL language constructs that cannot be converted into the
//     *             bigdata native rule model
//     * @throws QueryEvaluationException
//     */
//    private IProgram createNativeQueryOld(Union union)
//            throws UnsupportedOperatorException,
//            QueryEvaluationException {
//        
//        // create a new program that can run in parallel
//        Program program = new Program("union", true);
//        
//        TupleExpr left = union.getLeftArg();
//        // if the left arg is a union, create a program for it and merge it
//        if (left instanceof Union) {
//            Program p2 = (Program) createNativeQuery((Union) left);
//            program.addSteps(p2.steps());
//        } else if (left instanceof Join || left instanceof LeftJoin || 
//                left instanceof Filter) {
//            IRule rule = createNativeQueryOld(left);
//            if (rule != null) {
//                if (rule instanceof ProxyRuleWithSesameFilters) {
//                    // unfortunately I think we just have to punt to be super safe
//                    Collection<Filter> filters = 
//                        ((ProxyRuleWithSesameFilters) rule).getSesameFilters();
//                    if (log.isDebugEnabled()) {
//                        log.debug("could not translate " + filters.size()
//                                + " filters into native constraints:");
//                        for (Filter filter : filters) {
//                            log.debug("\n" + filter.getCondition());
//                        }
//                    }
//                    throw new UnsupportedOperatorException(filters.iterator().next());
//                }
//                program.addStep(rule);
//            }
//        } else if (left instanceof StatementPattern) {
//            IRule rule = createNativeQueryOld((StatementPattern) left);
//            if (rule != null) {
//                program.addStep(rule);
//            }
//        } else {
//            throw new UnsupportedOperatorException(left);
//        }
//        
//        TupleExpr right = union.getRightArg();
//        // if the right arg is a union, create a program for it and merge it
//        if (right instanceof Union) {
//            Program p2 = (Program) createNativeQuery((Union) right);
//            program.addSteps(p2.steps());
//        } else if (right instanceof Join || right instanceof LeftJoin ||
//                right instanceof Filter) {
//            IRule rule = createNativeQueryOld(right);
//            if (rule != null) {
//                if (rule instanceof ProxyRuleWithSesameFilters) {
//                    // unfortunately I think we just have to punt to be super safe
//                    Collection<Filter> filters = 
//                        ((ProxyRuleWithSesameFilters) rule).getSesameFilters();
//                    if (log.isDebugEnabled()) {
//                        log.debug("could not translate " + filters.size()
//                                + " filters into native constraints:");
//                        for (Filter filter : filters) {
//                            log.debug("\n" + filter.getCondition());
//                        }
//                    }
//                    throw new UnsupportedOperatorException(filters.iterator().next());
//                }
//                program.addStep(rule);
//            }
//        } else if (right instanceof StatementPattern) {
//            IRule rule = createNativeQueryOld((StatementPattern) right);
//            if (rule != null) {
//                program.addStep(rule);
//            }
//        } else {
//            throw new UnsupportedOperatorException(right);
//        }
//        
//        return program;
//        
//    }

//    /**
//     * Take the supplied tuple expression and flatten all the statement patterns
//     * into a collection that can then be fed into a bigdata rule.  So if the
//     * tuple expression is itself a statement pattern or a filter, simply cast 
//     * and add it to the appropriate collection.  If the tuple expression is a
//     * join or left join, use recursion on the left and right argument of the
//     * join.  If the tuple expression is anything else, for example a Union,
//     * this method will throw an exception.  Currently Unions nested inside
//     * of joins is not supported due to deficiencies in the native bigdata
//     * rule model.
//     * <p>
//     * @todo support nested Unions
//     *
//     * @param tupleExpr
//     * @param stmtPatterns
//     * @param filters
//     */
//    private void collectStatementPatterns(final TupleExpr tupleExpr,
//            final Map<StatementPattern, Boolean> stmtPatterns,
//            final Collection<Filter> filters) 
//        throws UnsupportedOperatorException {
//        
//        if (tupleExpr instanceof StatementPattern) {
//            stmtPatterns.put((StatementPattern) tupleExpr, Boolean.FALSE);
//        } else if (tupleExpr instanceof Filter) {
//            final Filter filter = (Filter) tupleExpr;
//            filters.add(filter);
//            final TupleExpr arg = filter.getArg();
//            collectStatementPatterns(arg, stmtPatterns, filters);
//        } else if (tupleExpr instanceof Join) {
//            final Join join = (Join) tupleExpr;
//            final TupleExpr left = join.getLeftArg();
//            final TupleExpr right = join.getRightArg();
//            collectStatementPatterns(left, stmtPatterns, filters);
//            collectStatementPatterns(right, stmtPatterns, filters);
//        } else if (tupleExpr instanceof LeftJoin) {
//
//            final LeftJoin join = (LeftJoin) tupleExpr;
//            
//            /*
//             * FIXME Another deficiency in the native rule model. Incorrect
//             * scoping of join.
//             * Example:
//             * SELECT *
//             * { 
//             *   ?X  :name "paul"
//             *   {?Y :name "george" . OPTIONAL { ?X :email ?Z } }
//             * }
//             * 
//             * 1. Join 
//             *    2. StatementPattern 
//             *    3. LeftJoin
//             *       4. StatementPattern
//             *       5. StatementPattern 
//             *       
//             * (1) starts collecting its child nodes and gets to (3), which
//             * puts us here in the code.  But this is not a case where we
//             * can just flatten the whole tree.  (3) needs to be evaluated
//             * independently, as a subprogram.
//             */
//            QueryModelNode operator = join;
//            while ((operator = operator.getParentNode()) != null) {
//                if (operator instanceof Join) {
//                    // Use Sesame 2 evaluation
//                    throw new UnsupportedOperatorException(join);
//                }
//            }
//            
//            // FIXME is this right?  what about multiple optionals - do they nest?
//            final TupleExpr left = join.getLeftArg();
//            final TupleExpr right = join.getRightArg();
//            // all we know how to handle right now is a left join of:
//            // { StatementPattern || Join || LeftJoin } x { StatementPattern }
//            if (!(right instanceof StatementPattern)) {
//                throw new UnsupportedOperatorException(right);
//            }
//            final ValueExpr condition = join.getCondition();
//            if (condition != null) {
//                /*
//                Filter filter = new Filter(right, condition);
//                // fake a filter, we just need the value expr later
//                filters.add(filter);
//                */
//                // we have to punt on nested optional filters just to be safe
//                throw new UnsupportedOperatorException(join);
//            }
//            stmtPatterns.put((StatementPattern) right, Boolean.TRUE);
//            collectStatementPatterns(left, stmtPatterns, filters);
//        } else {
//            throw new UnsupportedOperatorException(tupleExpr);
//        }
//        
//    }

    private IPredicate toPredicate(final Regex regex) 
    		throws QueryEvaluationException {
    	
    	final Var s = (Var) regex.getLeftArg();
    	final ValueConstant vc = (ValueConstant) regex.getRightArg();
    	final Var p = new Var();
    	p.setValue(BD.SEARCH);
    	final Var o = new Var();
    	o.setValue(vc.getValue());
    	return toPredicate(new StatementPattern(s, p, o));
    	
    }
    
    /**
     * Generate a bigdata {@link IPredicate} (tail) for the supplied
     * StatementPattern.
     * <p>
     * As a shortcut, if the StatementPattern contains any bound values that
     * are not in the database, this method will return null.
     * 
     * @param stmtPattern
     * @return the generated bigdata {@link Predicate} or <code>null</code> if
     *         the statement pattern contains bound values not in the database.
     */
    private IPredicate toPredicate(final StatementPattern stmtPattern) 
    		throws QueryEvaluationException {
        
        // create a solution expander for free text search if necessary
        IAccessPathExpander<ISPO> expander = null;
        final Value predValue = stmtPattern.getPredicateVar().getValue();
        if (log.isDebugEnabled()) {
            log.debug(predValue);
        }
        if (predValue != null && BD.SEARCH.equals(predValue)) {
            final Value objValue = stmtPattern.getObjectVar().getValue();
            if (log.isDebugEnabled()) {
                log.debug(objValue);
            }
            if (objValue != null && objValue instanceof Literal) {
                expander = new FreeTextSearchExpander(database,
                        (Literal) objValue);
            }
        }
        
        // @todo why is [s] handled differently?
        // because [s] is the variable in free text searches, no need to test
        // to see if the free text search expander is in place
        final IVariableOrConstant<IV> s = toVariableOrConstant(
                stmtPattern.getSubjectVar());
        if (s == null) {
            return null;
        }
        
        final IVariableOrConstant<IV> p;
        if (expander == null) {
            p = toVariableOrConstant(stmtPattern.getPredicateVar());
        } else {
            p = new Constant(DummyIV.INSTANCE);
        }
        if (p == null) {
            return null;
        }
        
        final IVariableOrConstant<IV> o;
        if (expander == null) {
            o = toVariableOrConstant(stmtPattern.getObjectVar());
        } else {
            o = new Constant(DummyIV.INSTANCE);
        }
        if (o == null) {
            return null;
        }
        
        // The annotations for the predicate.
        final List<NV> anns = new LinkedList<NV>();
        
        final IVariableOrConstant<IV> c;
        if (!database.isQuads()) {
            /*
             * Either triple store mode or provenance mode.
             */
            final Var var = stmtPattern.getContextVar();
            if (var == null) {
                // context position is not used.
                c = null;
            } else {
                final Value val = var.getValue();
                if (val != null && database.isStatementIdentifiers()) {
                    /*
                     * Note: The context position is used as a statement
                     * identifier (SID). SIDs may be used to retrieve provenance
                     * statements (statements about statement) using high-level
                     * query. SIDs are represented as blank nodes and is not
                     * possible to have them bound in the original query. They
                     * only become bound during query evaluation.
                     */
                    throw new QueryEvaluationException(
                            "Context position is a statement identifier and may not be bound in the original query: "
                                    + stmtPattern);
                }
                final String name = var.getName();
                c = com.bigdata.bop.Var.var(name);
            }
        } else {
            /*
             * Quad store mode.
             */
            if (expander != null) {
                /*
                 * This code path occurs when we are doing a free text search
                 * for this access path using the FreeTestSearchExpander. There
                 * is no need to do any named or default graph expansion work on
                 * a free text search access path.
                 */
                c = null;
            } else {
                // the graph variable iff specified by the query.
                final Var cvar = stmtPattern.getContextVar();
                // quads mode.
                anns.add(new NV(Rule2BOpUtility.Annotations.QUADS, true));
                // attach the Scope.
                anns.add(new NV(Rule2BOpUtility.Annotations.SCOPE, stmtPattern
                        .getScope()));
                if (dataset == null) {
                    // attach the appropriate expander : @todo drop expanders. 
                    if (cvar == null) {
                        /*
                         * There is no dataset and there is no graph variable,
                         * so the default graph will be the RDF Merge of ALL
                         * graphs in the quad store.
                         * 
                         * This code path uses an "expander" which strips off
                         * the context information and filters for the distinct
                         * (s,p,o) triples to realize the RDF Merge of the
                         * source graphs for the default graph.
                         */
                        c = null;
                        expander = new DefaultGraphSolutionExpander(null/* ALL */);
                    } else {
                        /*
                         * There is no data set and there is a graph variable,
                         * so the query will run against all named graphs and
                         * [cvar] will be to the context of each (s,p,o,c) in
                         * turn. This handles constructions such as:
                         * 
                         * "SELECT * WHERE {graph ?g {?g :p :o } }"
                         */
                        expander = new NamedGraphSolutionExpander(null/* ALL */);
                        c = toVariableOrConstant(cvar);
                    }
                } else { // dataset != null
                    // attach the DataSet.
                    anns.add(new NV(Rule2BOpUtility.Annotations.DATASET,
                            dataset));
                    // attach the appropriate expander : @todo drop expanders. 
                    switch (stmtPattern.getScope()) {
                    case DEFAULT_CONTEXTS: {
                        /*
                         * Query against the RDF merge of zero or more source
                         * graphs.
                         */
                        expander = new DefaultGraphSolutionExpander(dataset
                                .getDefaultGraphs());
                        /*
                         * Note: cvar can not become bound since context is
                         * stripped for the default graph.
                         */
                        if (cvar == null)
                            c = null;
                        else
                            c = toVariableOrConstant(cvar);
                        break;
                    }
                    case NAMED_CONTEXTS: {
                        /*
                         * Query against zero or more named graphs.
                         */
                        expander = new NamedGraphSolutionExpander(dataset
                                .getNamedGraphs());
                        if (cvar == null) {// || !cvar.hasValue()) {
                            c = null;
                        } else {
                            c = toVariableOrConstant(cvar);
                        }
                        break;
                    }
                    default:
                        throw new AssertionError();
                    }
                }
            }
        }

        /*
         * This applies a filter to the access path to remove any inferred
         * triples when [includeInferred] is false.
         * 
         * @todo We can now stack filters so are we missing out here by not
         * layering in other filters as well? [In order to rotate additional
         * constraints onto an access path we would need to either change
         * IPredicate and AbstractAccessPath to process an IConstraint[] or
         * write a delegation pattern that let's us wrap one filter inside of
         * another.]
         */
        final IElementFilter<ISPO> filter = 
            !tripleSource.includeInferred ? ExplicitSPOFilter.INSTANCE
                : null;

        // Decide on the correct arity for the predicate.
        final BOp[] vars;
        if (!database.isQuads() && !database.isStatementIdentifiers()) {
            vars = new BOp[] { s, p, o };
        } else if (c == null) {
            vars = new BOp[] { s, p, o, com.bigdata.bop.Var.var() };
        } else {
            vars = new BOp[] { s, p, o, c };
        }

        anns.add(new NV(IPredicate.Annotations.RELATION_NAME,
                new String[] { database.getSPORelation().getNamespace() }));//
        
        // filter on elements visited by the access path.
        if (filter != null)
            anns.add(new NV(IPredicate.Annotations.INDEX_LOCAL_FILTER,
                    ElementFilter.newInstance(filter)));

        // free text search expander or named graphs expander
        if (expander != null)
            anns.add(new NV(IPredicate.Annotations.ACCESS_PATH_EXPANDER, expander));

        // timestamp
        anns.add(new NV(Annotations.TIMESTAMP, database
                .getSPORelation().getTimestamp()));

        /*
         * Explicitly set the access path / iterator flags.
         * 
         * Note: High level query generally permits iterator level parallelism.
         * We set the PARALLEL flag here so it can be used if a global index
         * view is chosen for the access path.
         * 
         * Note: High level query for SPARQL always uses read-only access paths.
         * If you are working with a SPARQL extension with UPDATE or INSERT INTO
         * semantics then you will need to remote the READONLY flag for the
         * mutable access paths.
         */
        anns.add(new NV(IPredicate.Annotations.FLAGS, IRangeQuery.DEFAULT
                | IRangeQuery.PARALLEL | IRangeQuery.READONLY));
        
        return new SPOPredicate(vars, anns.toArray(new NV[anns.size()]));
//        return new SPOPredicate(
//                new String[] { database.getSPORelation().getNamespace() },
//                -1, // partitionId
//                s, p, o, c,
//                optional, // optional
//                filter, // filter on elements visited by the access path.
//                expander // free text search expander or named graphs expander
//                );
        
    }

    /**
     * Takes a ValueExpression from a sesame Filter or LeftJoin and turns it
     * into a bigdata {@link IConstraint}.
	 * <p>
	 * This method will throw an exception if the Sesame term is bound and the
	 * value does not exist in the lexicon.
     */
    private IConstraint toConstraint(final ValueExpr ve) {
        if (ve instanceof Or) {
            return toConstraint((Or) ve);
        } else if (ve instanceof And) {
            return toConstraint((And) ve);
        } else if (ve instanceof Not) {
            return toConstraint((Not) ve);
        } else if (ve instanceof SameTerm) {
            return toConstraint((SameTerm) ve);
        } else if (ve instanceof Compare) {
            return toConstraint((Compare) ve);
        } else if (ve instanceof Bound) {
            return toConstraint((Bound) ve);
        }
        
        throw new UnsupportedOperatorException(ve);
    }

    private IConstraint toConstraint(Or or) {
        final IConstraint right = toConstraint(or.getRightArg());
        final IConstraint left = toConstraint(or.getLeftArg());
        return new OR(left, right);
    }

    private IConstraint toConstraint(And and) {
        final IConstraint right = toConstraint(and.getRightArg());
        final IConstraint left = toConstraint(and.getLeftArg());
        return new AND(left, right);
    }

    private IConstraint toConstraint(Not not) {
        final IConstraint c = toConstraint(not.getArg());
        return new NOT(c);
    }

    private IConstraint toConstraint(SameTerm sameTerm) {
    	final IValueExpression<IV> iv1 = 
    		toValueExpression(sameTerm.getLeftArg());
    	final IValueExpression<IV> iv2 = 
    		toValueExpression(sameTerm.getRightArg());
        return new CompareBOp(iv1, iv2, CompareOp.EQ);
    }

    private IConstraint toConstraint(Compare compare) {
    	final IValueExpression<IV> iv1 = 
    		toValueExpression(compare.getLeftArg());
    	final IValueExpression<IV> iv2 = 
    		toValueExpression(compare.getRightArg());
        return new CompareBOp(iv1, iv2, compare.getOperator());
    }

    private IConstraint toConstraint(Bound bound) {
    	final IVariable<IV> var = 
    		com.bigdata.bop.Var.var(bound.getArg().getName());
    	return new BOUND(var);
    }

    /**
     * Generate a bigdata {@link IValueExpression} for a given Sesame
     * <code>ValueExpr</code> object.  We can currently handle variables,
     * value constants, and math expressions. Is there anything else we need
     * to handle?
     */
    private IValueExpression<IV> toValueExpression(final ValueExpr ve) 
    		throws UnsupportedOperatorException {
    	if (ve instanceof Var) {
        	return toVariableOrConstant((Var) ve);
        } else if (ve instanceof ValueConstant) {
        	return toConstant((ValueConstant) ve);
        } else if (ve instanceof MathExpr) {
        	return toValueExpression((MathExpr) ve);
        } 
        
        throw new UnsupportedOperatorException(ve);
    }

	/**
	 * Generate a bigdata term from a Sesame term.
	 * <p>
	 * This method will throw an exception if the Sesame term is bound and the
	 * value does not exist in the lexicon.
	 */
    private IVariableOrConstant<IV> toVariableOrConstant(final Var var) 
    		throws UnsupportedOperatorException {
        final String name = var.getName();
        final BigdataValue val = (BigdataValue) var.getValue();
        if (val == null) {
            return com.bigdata.bop.Var.var(name);
        } else {
            final IV iv = val.getIV();
            if (iv == null) {
            	throw new UnrecognizedValueException(val);
            }
            if (var.isAnonymous())
                return new Constant<IV>(iv);
            else 
                return new Constant<IV>(com.bigdata.bop.Var.var(name), iv);
        }
    }
    
	/**
	 * Generate a bigdata term from a Sesame term.
	 * <p>
	 * This method will throw an exception if the Sesame term is bound and the
	 * value does not exist in the lexicon.
	 */
    private IConstant<IV> toConstant(final ValueConstant vc) {
        final IV iv = ((BigdataValue) vc.getValue()).getIV();
        if (iv == null)
        	throw new UnrecognizedValueException(vc.getValue());
    	return new Constant<IV>(iv);
    }

    /**
     * Generate a {@link MathBOp} from a sesame MathExpr.  A MathBOp will
     * have two {@link IValueExpression} operands, each of which will be
     * either a variable, constant, or another math expression.
     */
    private MathBOp toValueExpression(final MathExpr mathExpr) {
    	final ValueExpr left = mathExpr.getLeftArg();
    	final ValueExpr right = mathExpr.getRightArg();
    	final MathOp op = mathExpr.getOperator();
    	final IValueExpression<IV> iv1 = toValueExpression(left);
    	final IValueExpression<IV> iv2 = toValueExpression(right);
        return new MathBOp(iv1, iv2, op);
    }
    
    /**
     * Override evaluation of StatementPatterns to recognize magic search 
     * predicate.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            final StatementPattern sp, final BindingSet bindings)
            throws QueryEvaluationException {
        
        if (database.isQuads() && sp.getParentNode() instanceof Projection) {
			/*
			 * Note: This is required in order to get the correct semantics for
			 * named graph or default graph access paths in quads mode. However,
			 * doing this in triples more imposes a significant performance
			 * penalty.
			 */
            return evaluateNatively(sp, bindings);
        }
        
        if (log.isDebugEnabled()) {
            log.debug("evaluating statement pattern:\n" + sp);
        }
        
        // check for magic search
        final Var predVar = sp.getPredicateVar();
        final Value predValue = getVarValue(predVar, bindings);
        if (BD.SEARCH.equals(predValue)) {
            final Var ovar = sp.getObjectVar();
            final Value oval = getVarValue(ovar, bindings);
            if (oval == null) {
                throw new QueryEvaluationException(BD.SEARCH
                        + " : object must be bound.");
            }
            if (!(oval instanceof Literal)) {
                throw new QueryEvaluationException(BD.SEARCH
                        + " : object must be literal.");
            }
            final Literal lit = (Literal) oval;
            if (lit.getDatatype() != null) {
                throw new QueryEvaluationException(BD.SEARCH
                        + " : object is datatype literal.");
            }
            return search(sp.getSubjectVar(), lit.getLanguage(),
                    lit.getLabel(), bindings, sp.getScope());
        }
        
        return super.evaluate(sp, bindings);
        
    }
     
    /**
     * Evaluates the {@link BD#SEARCH} magic predicate as a full-text search
     * against the index literal in the database, binding <i>svar</i> to each
     * matched literal in turn.
     * <p>
     * Note: The minimum cosine (relevance score) is set to ZERO (0d) in order
     * to make sure that any match within a literal qualifies that literal for
     * inclusion within the set of bindings that are materialized. This is in
     * contrast to a standard search engine, where a minimum relevance score is
     * used to filter out less likely matches. However, it is my sense that
     * query against the KB is often used to find ALL matches. Regardless, the
     * matches will be materialized in order of decreasing relevance and an
     * upper bound of 10000 matches is imposed by the search implementation. See
     * {@link FullTextIndex#search(String, String, double, int)}.
     * 
     * @param svar
     *            The variable from the subject position of the
     *            {@link StatementPattern} in which the {@link BD#SEARCH} magic
     *            predicate appears.
     * @param languageCode
     *            An optional language code from the bound literal appearing in
     *            the object position of that {@link StatementPattern}.
     * @param label
     *            The required label from the bound literal appearing in the
     *            object position of that {@link StatementPattern}.
     * @param bindings
     *            The current bindings.
     * @param scope
     *            The scope of the statement pattern when in quads mode. The
     *            bound values for the text search must appear in a statement in
     *            the scope of the statement pattern.
     * 
     * @return Iteration visiting the bindings obtained by the search.
     * 
     * @throws QueryEvaluationException
     * 
     * @todo consider options for term weights and normalization. Search on the
     *       KB is probably all terms with anything matching, stopwords are
     *       excluded, and term weights can be without normalization since
     *       ranking does not matter. And maxRank should probably be defeated
     *       (Integer.MAX_VALUE or ZERO - whichever does the trick).
     * 
     * @todo it would be nice if there were a way for the caller to express more
     *       parameters for the search, e.g., to give the minCosine and maxRank
     *       values directly as a tuple-based function call. I'm not sure if
     *       that is supported within Sesame/SPARQL.
     */
    protected CloseableIteration<BindingSet, QueryEvaluationException> search(
            final Var svar, final String languageCode, final String label,
            final BindingSet bindings, final Scope scope)
            throws QueryEvaluationException {
        
        if (log.isDebugEnabled()) {
            log.debug("languageCode=" + languageCode + ", label=" + label);
        }
        
        final Iterator<IHit> itr = database.getLexiconRelation()
                .getSearchEngine().search(label, languageCode,
                        false/* prefixMatch */, 0d/* minCosine */,
                        10000/* maxRank */, 1000L/* timeout */,
                        TimeUnit.MILLISECONDS);
        
        // ensure that named graphs are handled correctly for quads
        Set<URI> graphs = null;
        if (database.isQuads() && dataset != null) {
            switch (scope) {
            case DEFAULT_CONTEXTS: {
                /*
                 * Query against the RDF merge of zero or more source graphs.
                 */
                graphs = dataset.getDefaultGraphs();
                break;
            }
            case NAMED_CONTEXTS: {
                /*
                 * Query against zero or more named graphs.
                 */
                graphs = dataset.getNamedGraphs();
                break;
            }
            default:
                throw new AssertionError();
            }
        }
        
        // Return an iterator that converts the term identifiers to var bindings
        return new HitConvertor(database, itr, svar, bindings, graphs);
        
    }
    
//    protected Collection<IPredicate> generateStarJoins(
//            Collection<IPredicate> tails) {
//        
//        Collection<IPredicate> newTails = new LinkedList<IPredicate>();
//        
//        Map<IVariable,Collection<IPredicate>> subjects = 
//            new HashMap<IVariable,Collection<IPredicate>>();
//        
//        for (IPredicate pred : tails) {
//            IVariableOrConstant s = pred.get(0);
//            if (s.isVar() && /*pred.getSolutionExpander() == null &&*/ 
//                    pred.getIndexLocalFilter() == null&&
//                    pred.getAccessPathFilter() == null) {
//                IVariable v = (IVariable) s;
//                Collection<IPredicate> preds = subjects.get(v);
//                if (preds == null) {
//                    preds = new LinkedList<IPredicate>();
//                    subjects.put(v, preds);
//                }
//                preds.add(pred);
//                if (log.isDebugEnabled()) {
//                    log.debug("found a star joinable tail: " + pred);
//                }
//            } else {
//                newTails.add(pred);
//            }
//        }
//        
//        for (Map.Entry<IVariable,Collection<IPredicate>> e : subjects.entrySet()) {
//            Collection<IPredicate> preds = e.getValue();
//            if (preds.size() <= 2) {
//                newTails.addAll(preds);
//                continue;
//            }
//            IVariable s = e.getKey();
//            IPredicate mostSelective = null;
//            long minRangeCount = Long.MAX_VALUE;
//            int numOptionals = 0;
//            for (IPredicate pred : preds) {
//                if (pred.isOptional()) {
//                    numOptionals++;
//                    continue;
//                }
//                long rangeCount = database.getSPORelation().getAccessPath(
//                        (SPOPredicate) pred).rangeCount(false);
//                if (rangeCount < minRangeCount) {
//                    minRangeCount = rangeCount;
//                    mostSelective = pred;
//                }
//            }
//            if (preds.size() - numOptionals < 2) {
//                newTails.addAll(preds);
//                continue;
//            }
//            if (mostSelective == null) {
//                throw new RuntimeException(
//                        "??? could not find a most selective tail for: " + s);
//            }
//            boolean sharedVars = false;
//            Collection<IVariable> vars = new LinkedList<IVariable>();
//            for (IPredicate pred : preds) {
//                if (pred instanceof SPOPredicate) {
//                    SPOPredicate spoPred = (SPOPredicate) pred;
//                    if (spoPred.p().isVar()) {
//                        IVariable v = (IVariable) spoPred.p();
//                        if (vars.contains(v)) {
//                            sharedVars = true;
//                            break;
//                        }
//                        vars.add(v);
//                    }
//                    if (spoPred.o().isVar()) {
//                        IVariable v = (IVariable) spoPred.o();
//                        if (vars.contains(v)) {
//                            sharedVars = true;
//                            break;
//                        }
//                        vars.add(v);
//                    }
//                }
//            }
//            if (!sharedVars) {
//                SPOStarJoin starJoin = new SPOStarJoin(
//                        (SPOPredicate) mostSelective);
//                for (IPredicate pred : preds) {
//                    if (pred == mostSelective) {
//                        continue;
//                    }
//                    starJoin.addStarConstraint(
//                            new SPOStarJoin.SPOStarConstraint(
//                                    pred.get(1), pred.get(2), pred.isOptional()));
//                }
//                newTails.add(starJoin);
//                newTails.add(mostSelective);
//            } else {
//                newTails.addAll(preds);
//            }
//        }
//        
//        if (log.isDebugEnabled()) {
//            log.debug("number of new tails: " + newTails.size());
//            for (IPredicate tail : newTails) {
//                log.debug(tail);
//            }
//        }
//        
//        return newTails;
//        
//    }

    // /** @issue make protected in openrdf. */
    // protected Value getVarValue(final Var var, final BindingSet bindings) {
    //
    // if (var == null) {
    //
    // return null;
    //
    // } else if (var.hasValue()) {
    //
    // return var.getValue();
    //
    // } else {
    //
    // return bindings.getValue(var.getName());
    //
    // }
    //
    // }
    
//    protected static class ProxyRuleWithSesameFilters implements IRule {
//
//        private IRule rule;
//        
//        private Collection<Filter> filters;
//        
//        public ProxyRuleWithSesameFilters(IRule rule, 
//                Collection<Filter> filters) {
//            
//            this.rule = rule;
//            this.filters = filters;
//            
//        }
//        
//        public IRule getProxyRule() {
//            return rule;
//        }
//        
//        public Collection<Filter> getSesameFilters() {
//            return filters;
//        }
//        
//        public IQueryOptions getQueryOptions() {
//            return rule.getQueryOptions();
//        }
//
//        public boolean isRule() {
//            return rule.isRule();
//        }
//
//        public IBindingSet getConstants() {
//            return rule.getConstants();
//        }
//
//        public IConstraint getConstraint(int index) {
//            return rule.getConstraint(index);
//        }
//
//        public int getConstraintCount() {
//            return rule.getConstraintCount();
//        }
//
//        public Iterator getConstraints() {
//            return rule.getConstraints();
//        }
//
//        public IPredicate getHead() {
//            return rule.getHead();
//        }
//
//        public String getName() {
//            return rule.getName();
//        }
//
//        public Set getSharedVars(int index1, int index2) {
//            return rule.getSharedVars(index1, index2);
//        }
//
//        public Iterator getTail() {
//            return rule.getTail();
//        }
//
//        public IPredicate getTail(int index) {
//            return rule.getTail(index);
//        }
//
//        public int getTailCount() {
//            return rule.getTailCount();
//        }
//
//        public IRuleTaskFactory getTaskFactory() {
//            return rule.getTaskFactory();
//        }
//
//        public int getVariableCount() {
//            return rule.getVariableCount();
//        }
//
//        public int getVariableCount(int index, IBindingSet bindingSet) {
//            return rule.getVariableCount(index, bindingSet);
//        }
//
//        public Iterator getVariables() {
//            return rule.getVariables();
//        }
//
//        public int getRequiredVariableCount() {
//            return rule.getRequiredVariableCount();
//        }
//
//        public Iterator getRequiredVariables() {
//            return rule.getRequiredVariables();
//        }
//
//        public boolean isConsistent(IBindingSet bindingSet) {
//            return rule.isConsistent(bindingSet);
//        }
//
//        public boolean isDeclared(IVariable var) {
//            return rule.isDeclared(var);
//        }
//
//        public boolean isFullyBound(IBindingSet bindingSet) {
//            return rule.isFullyBound(bindingSet);
//        }
//
//        public boolean isFullyBound(int index, IBindingSet bindingSet) {
//            return rule.isFullyBound(index, bindingSet);
//        }
//
//        public IRule specialize(IBindingSet bindingSet, IConstraint[] constraints) {
//            return rule.specialize(bindingSet, constraints);
//        }
//
//        public IRule specialize(String name, IBindingSet bindingSet, IConstraint[] constraints) {
//            return rule.specialize(name, bindingSet, constraints);
//        }
//
//        public String toString(IBindingSet bindingSet) {
//            return rule.toString(bindingSet);
//        }
//        
//        public String toString() {
//            return rule.toString();
//        }
//        
//    }
    
    static private class UnrecognizedValueException extends RuntimeException {

        /**
		 * 
		 */
		private static final long serialVersionUID = 7409038222083458821L;
		
		private Value value;

		/**
		 * Wrap another instance of this exception class.
		 * @param cause
		 */
		public UnrecognizedValueException(final UnrecognizedValueException cause) {
		
			super(cause);
			
			this.value = cause.value;
			
		}

        public UnrecognizedValueException(final Value value) {
            this.value = value;
        }

        public Value getValue() {
        	return value;
        }
        
    }
    
}
