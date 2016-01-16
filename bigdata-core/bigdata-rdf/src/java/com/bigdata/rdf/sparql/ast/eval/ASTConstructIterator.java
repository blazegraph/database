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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import info.aduna.iteration.CloseableIteration;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.impl.EmptyBindingSet;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.ap.filter.DistinctFilter;
import com.bigdata.bop.rdf.filter.NativeDistinctFilter;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataQuadWrapper;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;

import cutthecrap.utils.striterators.ICloseable;
import cutthecrap.utils.striterators.IFilterTest;

/**
 * Iterator consumes the solutions from a query and interprets them according to
 * a {@link ConstructNode}. Ground triples in the template are output
 * immediately. Any non-ground triples are output iff they are fully (and
 * validly) bound for a given solution. Blank nodes are scoped to a solution.
 * <p>
 * Note: This supports construct of quads, but the SPARQL grammar does not.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTConstructIterator.java 5131 2011-09-05 20:48:48Z thompsonbry
 *          $
 */
public class ASTConstructIterator implements
        CloseableIteration<BigdataStatement, QueryEvaluationException> {

    private static final Logger log = Logger
            .getLogger(ASTConstructIterator.class);
    
    private static final boolean DEBUG = log.isDebugEnabled();

    /**
     * When <code>false</code>, no DISTINCT SPO filter will be imposed.
     * 
     * @see https://jira.blazegraph.com/browse/BLZG-1341 (performance of dumping
     *      single graph)
     */
    private final boolean constructDistinctSPO;

    private final BigdataValueFactory f;

    /**
     * The non-ground statement patterns.
     */
    private final List<StatementPatternNode> templates;
    
    /**
     * A mapping of blank node IDs to {@link BigdataBNode}s that will be imposed
     * across the {@link ASTConstructIterator}. This MUST be <code>null</code>
     * for a CONSTRUCT query since the semantics of CONSTRUCT require that blank
     * node assignments are scoped to the solution. It must be non-
     * <code>null</code> for a DESCRIBE query since we want to have consistent
     * blank node ID assigments for all statements in the response graph.
     * <p>
     * Note: DO NOT access this directly. Instead, use the
     * {@link #getBNodeMap()} to set a <strong>lexically local</strong> map
     * reference and then clear that map reference to <code>null</code> when you
     * are done with a given solution. If the {@link #bnodes} reference is
     * <code>null</code>, then a <strong>new</strong> blank node map will be
     * returned by every invocation of {@link #getBNodeMap()}. If it is non-
     * <code>null</code> then the <strong>same</strong> map is returned by every
     * invocation. This makes it possible to determine the scope of the blank
     * nodes simply by whether or not this map is supplied to the constructor.
     */
    final Map<String, BigdataBNode> bnodes;

    /**
     * Return the blank node map. If this was specified to the constructor, then
     * the same map is returned for each invocation. Otherwise a
     * <strong>new</strong> map instance is returned for each invocation.
     * 
     * @see #bnodes
     */
    private final Map<String, BigdataBNode> getBNodeMap() {
        
        if (bnodes == null) {
        
            return new LinkedHashMap<String, BigdataBNode>();
            
        }

        return bnodes;
        
    }

    
    /**
     * Ground triples from the template.
     */
    private final List<BigdataStatement> groundTriples;

    private final CloseableIteration<BindingSet, QueryEvaluationException> src;

    /**
     * A list of {@link Statement}s constructed from the most recently visited
     * solution. Statements are drained from this buffer, sending them to the
     * sink. Once the buffer is empty, we will go back to the {@link #src} to
     * refill it.
     * <p>
     * The buffer is pre-populated with any ground triples in the construct
     * template by the constructor.
     */
    private final LinkedList<BigdataStatement> buffer = new LinkedList<BigdataStatement>();

    /**
     * A filter which restricts the emitted statements to the distinct
     * {@link ISPO}. The {@link DistinctFilter} is based on the Java heap. The
     * {@link NativeDistinctFilter} is based on persistence capable data
     * structures and can scale to high cardinality outputs. Unfortunately, a
     * complex CONSTRUCT template can make it impossible to predict the
     * <p>
     * Note: It is possible to disable this filter (in the code) by setting it
     * to <code>null</code>.
     * 
     * @see DistinctFilter
     * @see NativeDistinctFilter
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
     *      CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
     */
    private final IFilterTest filter;
    
//    /**
//     * Return <code>true</code>iff {@link LexiconRelation#isStoreBlankNodes()}
//     * is <code>true</code>.
//     */
//    private final boolean toldBNodes;
    
    /**
     * A factory for blank node identifiers, which are scoped to a solution.
     */
    private int bnodeIdFactory = 0;
    
    private boolean open = true;

    /**
     * <code>false</code> until we get the first solution for the WHERE clause.
     * We do not output ground triples in the template until we see that first
     * solution. It is Ok if it is empty, but we need to know that the WHERE
     * clause succeeded before we can output the ground triples.
     */
    private boolean haveFirstSolution = false;

	public static boolean flagToCheckNativeDistinctQuadsInvocationForJUnitTesting = false;

    /**
     * 
     * @param tripleStore
     * @param construct
     *            The {@link ConstructNode}
     * @param whereClause
     *            The WHERE clause (used to identify construct templates which
     *            obviously produce DISTINCT triples).
     * @param bnodes
     *            A mapping from bnode IDs to {@link BigdataBNode}s that will be
     *            imposed (optional). This MUST be <code>null</code> when the
     *            top-level operation is a CONSTRUCT since the semantics of
     *            CONSTRUCT <strong>require</strong> that blank node ID
     *            assignments are scoped to the solution. However, when a
     *            DESCRIBE query is rewritten into a CONSTRUCT, the blank node
     *            identifier mappings must be scoped to the response graph. For
     *            a DESCRIBE that uses a recursive expansion (CBD, etc.), the
     *            blank node IDs must be scoped to the entire response, not just
     *            the individual DESCRIBE queries that are issued in support of
     *            the top-level DESCRIBE request.
     * @param src
     *            The solutions that will be used to construct the triples.
     */
    public ASTConstructIterator(//
            final AST2BOpContext context,//
            final AbstractTripleStore tripleStore,//
            final ConstructNode construct,//
            final GraphPatternGroup<?> whereClause,//
            final Map<String, BigdataBNode> bnodesIn,//
            final CloseableIteration<BindingSet, QueryEvaluationException> src) {

        this.constructDistinctSPO = context.constructDistinctSPO;
        
        this.f = tripleStore.getValueFactory();

        // Note: MAY be null (MUST be null for CONSTRUCT).
        this.bnodes = bnodesIn;
        
//        this.toldBNodes = store.getLexiconRelation().isStoreBlankNodes();
        
        templates = new LinkedList<StatementPatternNode>();

        groundTriples = new LinkedList<BigdataStatement>();

        // Blank nodes (scoped to the solution).
        Map<String, BigdataBNode> bnodes = null;

        for (StatementPatternNode pat : construct) {

            if (pat.isGround()) {

                if (bnodes == null) {
                    // Either new bnodes map or reuse global map.
                    bnodes = getBNodeMap();
                }
                
                // Create statement from the template.
                final BigdataStatement stmt = makeStatement(pat,
                        EmptyBindingSet.getInstance(), bnodes);
                
//                final BigdataStatement stmt = f.createStatement(//
//                        (Resource) pat.s().getValue(),//
//                        (URI) pat.p().getValue(),//
//                        (Value) pat.o().getValue(),//
//                        pat.c() == null ? null : (Resource) pat.c().getValue()//
//                        );

                if (DEBUG)
                    log.debug("Ground statement:\npattern=" + pat + "\nstmt="
                            + stmt);

                groundTriples.add(stmt);

            } else {

                /*
                 * A statement pattern that we will process for each solution.
                 */

                templates.add(pat);

            }

        }

        this.src = src;

        filter = createDistinctFilter(tripleStore, construct, whereClause);
        
    }

	private IFilterTest createDistinctFilter(final AbstractTripleStore tripleStore, final ConstructNode construct,
			final GraphPatternGroup<?> whereClause) {
		/*
         * Setup the DISTINCT SPO filter.
         * 
         * Note: CONSTRUCT is sometimes used to materialize all triples for some
         * triple pattern. For that use case, the triples are (of necessity)
         * already DISTINCT. Therefore, when there is a single triple pattern in
         * the WHERE clause and a single template in the CONSTRUCT, we DO NOT
         * impose a DISTINCT filter. This saves resources when the CONSTRUCTed
         * graph is large.
         */

		final boolean distinctQuads = construct.isDistinctQuads() && tripleStore.isQuads() && hasMixedQuadData(templates);
		final boolean nativeDistinct = construct.isNativeDistinct();
		
        if (!constructDistinctSPO) {
            /**
             * DISTINCT SPO filter was disabled by a query hint. The output is
             * NOT guaranteed to be distinct.
             * 
             * @see BLZG-1341.
             */
            // No filter will be imposed.
            return null;
        }
        
		if (nativeDistinct && construct.isDistinctQuads()) {
			flagToCheckNativeDistinctQuadsInvocationForJUnitTesting = true;
		}
		
        /*
         * Test the CONSTRUCT clause and WHERE clause to see if we need to
         * impose a DISTINCT SPO filter.
         */
        final boolean isObviouslyDistinct = isObviouslyDistinct(tripleStore.isQuads(),
                templates, whereClause);

		if (isObviouslyDistinct) {

            // Do not impose a DISTINCT filter.
            
            return null;
            
        }

		if (distinctQuads) {
			
			if (nativeDistinct) {
				return createNativeDistinctQuadsFilter(construct);
			} else {
			    return createHashDistinctQuadsFilter(construct);
			}
			
		} else {

			if (nativeDistinct) {
				return createNativeDistinctTripleFilter(construct);
			} else {
				// JVM Based DISTINCT filter.
				return new DistinctFilter.DistinctFilterImpl(construct);
			}
		}
        
	}

	/**
	 * FIXME NATIVE DISTINCT : This needs to create a filter using a HTree to
	 * impose a scalable distinct.  
	 * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-260"> native
     *      distinct in quad mode (insert/delete) </a>
	 */
	private IFilterTest createNativeDistinctQuadsFilter(final ConstructNode construct) {
		return createHashDistinctQuadsFilter(construct);
	}

	@SuppressWarnings("serial")
	private IFilterTest createHashDistinctQuadsFilter(final ConstructNode construct) {
        return new DistinctFilter.DistinctFilterImpl(construct){
        	@Override
        	public boolean isValid(Object o){
        		return super.isValid(new BigdataQuadWrapper((BigdataStatement)o));
        	}
        };
	}

	private boolean hasMixedQuadData(final List<StatementPatternNode> templates) {
		if (templates.size() == 0) {
			return false;
		}
		TermNode singleValue = templates.get(0).c();
		if (singleValue instanceof VarNode) {
			return true;
		}
		for (StatementPatternNode spn:templates) {
			TermNode tn = spn.c();
			if (!equals(singleValue ,tn )) {
				// this is a little too strong, but it is merely inefficient not incorrect
				// to return true when false would be correct.
				return true;
			}
		}
		return false;
	}

	private boolean equals(final TermNode a, final TermNode b) {
		return a == b || ( a != null && a.equals(b));
	}

	private IFilterTest createNativeDistinctTripleFilter(final ConstructNode construct) {
		/*
		 * Construct a predicate for the first triple template. We will
		 * use that as the bias for the scalable DISTINCT SPO filter.
		 */
		@SuppressWarnings("rawtypes")
		final IPredicate pred;
		{

		    final StatementPatternNode sp = templates.get(0/* index */);

		    @SuppressWarnings("rawtypes")
		    final IVariableOrConstant<IV> s = sp.s()
		            .getValueExpression();

		    @SuppressWarnings("rawtypes")
		    final IVariableOrConstant<IV> p = sp.p()
		            .getValueExpression();
		    
		    @SuppressWarnings("rawtypes")
		    final IVariableOrConstant<IV> o = sp.o()
		            .getValueExpression();

		    // // The graph term/variable iff specified by the query.
		    // final TermNode cvar = sp.c();
		    // final IVariableOrConstant<IV> c = cvar == null ? null :
		    // cvar
		    // .getValueExpression();

		    final BOp[] vars = new BOp[] { s, p, o /* , c */};

		    pred = new SPOPredicate(vars, BOp.NOANNS);

		}

		/*
		 * The index that will be used to read on the B+Tree access
		 * path.
		 */
		@SuppressWarnings({ "unchecked", "rawtypes" })
		final SPOKeyOrder indexKeyOrder = SPOKeyOrder.getKeyOrder(
		        (IPredicate) pred, 3/* keyArity */);

		construct.setProperty(
		        NativeDistinctFilter.Annotations.KEY_ORDER,
		        indexKeyOrder);

		// Native memory based DISTINCT filter.
		return new NativeDistinctFilter.DistinctFilterImpl(construct);
	}
    
    /**
     * Return <code>true</code> iff this CONSTRUCT template will obviously
     * produce distinct triples.
     * <p>
     * Note: CONSTRUCT is sometimes used to materialize all triples for some
     * triple pattern. For that use case, the triples are (of necessity) already
     * DISTINCT. Therefore, when there is a single triple pattern in the WHERE
     * clause and a single template in the CONSTRUCT, we DO NOT impose a
     * DISTINCT filter. This saves resources when the CONSTRUCTed graph is
     * large.
     * <p>
     * Note: The specifics of the triple pattern in the where clause and the
     * template do not matter as long as the triple pattern in the WHERE clause
     * can not introduce a cardinality which is higher than the cardinality of
     * the triples constructed by the template. This condition is satisfied if
     * all variables used in the triple pattern in the WHERE clause are also
     * used in the triple pattern in the template.
     * <p>
     * Note: The ground triples in the construct template are ignored for these
     * purposes. We are only concerned with disabling the DISTINCT filter when
     * the #of duplicate triples must be zero or near zero, not with providing a
     * 100% guarantee that there are no duplicate triples in the CONSTRUCT
     * output.
     * 
     * @param quads
     *            <code>true</code> iff the database is in quads mode.
     * @param templates
     *            The non-ground triple templates.
     * @param whereClause
     *            The WHERE clause for the query.
     * 
     * @return <code>true</code> iff the construct will obviously produce a
     *         graph containing distinct triples.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
     *      CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
     */
	// Note: package private to expose to test suite.
    static boolean isObviouslyDistinct(//
            final boolean quads,//
            final List<StatementPatternNode> templates,//
            final GraphPatternGroup<?> whereClause//
    ) {
        
        if (templates.isEmpty()) {
        
            /*
             * The CONSTRUCT does not involve any parameterized (non-ground)
             * triple templates.
             */
            
            return true;
            
        }
 
        if (templates.size() != 1 || whereClause.size() != 1) {

            /*
             * Either the templates and/or the where clause is complex. In this
             * case we can not easily predict whether there will be duplicate
             * triples constructed.
             */
            
            return false;
            
        }
        
        if (!(whereClause.get(0) instanceof StatementPatternNode)) {
            
            /*
             * The WHERE clause is not a single statement pattern.
             */

            return false;
            
        }

        /*
         * Both the templates and the where clause are a single statement
         * pattern.  If they are the *same* statement pattern, then the
         * CONSTRUCT will not produce any duplicate triples using that
         * template.  
         */
        
        final StatementPatternNode sp1 = templates.get(0);

        final StatementPatternNode sp2 = (StatementPatternNode) whereClause
                .get(0);

        /*
         * Make sure that all variables used in the WHERE clause triple pattern
         * also appear in the CONSTRUCT template triple pattern.
         */

        final Set<IVariable<?>> vars1 = StaticAnalysis.getSPOVariables(sp1);
        
        final Set<IVariable<?>> vars2 = StaticAnalysis.getSPOVariables(sp2);

        if (!vars1.equals(vars2)) {

            /*
             * Some variable(s) do not appear in both places.
             */
            
            return false;
            
        }
        
//        if (!sp1.s().equals(sp2.s()))
//            return false;
//
//        if (!sp1.p().equals(sp2.p()))
//            return false;
//        
//        if (!sp1.o().equals(sp2.o()))
//            return false;

        /**
         * CONSTRUCT always produces triples, but the access path for the
         * statement pattern in the WHERE clause may visit multiple named
         * graphs. If it does, then duplicate triples can result unless the
         * access path is the RDF MERGE (default graph access path).
         * 
         * @see https://jira.blazegraph.com/browse/BLZG-1341 (do not de-dup for very large graphs)
         */

        if (quads && sp2.c() == null && sp2.getScope() == Scope.NAMED_CONTEXTS) {

            /*
             * Multiple named graphs could be visited, so the statement pattern
             * in the CONSTRUCT could produce duplicate triples.
             */
            
            return false;

        }

        /*
         * The construct should produce distinct triples without our having to
         * add a DISTINCT filter.
         */

        return true;
        
    }

    @Override
    public boolean hasNext() throws QueryEvaluationException {

        while (true) {

            if (!buffer.isEmpty()) {

                /*
                 * At least one statement is ready in the buffer.
                 */

                return true;
                
            }

            if (!src.hasNext()) {

                /*
                 * Nothing left to visit.
                 */
                
                close();

                return false;

            }

            /*
             * Refill the buffer from the next available solution.
             */
            
            fillBuffer(src.next());

            /*
             * Check to see whether we can assemble any statements from that
             * solution.
             */
            
            continue;
            
        }
        
    }

    @Override
    public BigdataStatement next() throws QueryEvaluationException {

        if (!hasNext())
            throw new NoSuchElementException();

        /*
         * Remove and return the first statement from the buffer.
         */
        
        return buffer.removeFirst();
        
    }

    @Override
    public void close() throws QueryEvaluationException {

        if (open) {
        
            open = false;
            
            src.close();
            
            if (filter instanceof ICloseable) {

                /*
                 * Ensure that we release the backing MemoryManager in a timely
                 * fashion.
                 * 
                 * @see <a
                 * href="https://sourceforge.net/apps/trac/bigdata/ticket/582">
                 * IStriterator does not support close() protocol for IFilter
                 * </a>
                 */
                ((ICloseable) filter).close();

            }

        }

    }

    @Override
    public void remove() throws QueryEvaluationException {

        throw new UnsupportedOperationException();

    }

    /**
     * Refill the buffer from a new solution. This method is responsible for the
     * scope of blank nodes and for discarding statements which are ill-formed
     * (missing slots, bad type for a slot, etc).
     * 
     * @param solution
     */
    private void fillBuffer(final BindingSet solution) {

        // Should only be invoked when the buffer is empty.
        assert buffer.isEmpty();
        
        if (!haveFirstSolution) {

            /*
             * Once we see the first solution (even if it is empty) we can
             * output the ground triples from the template, but not before.
             */

            haveFirstSolution = true;
            
            for(BigdataStatement stmt : groundTriples) {
                
                addStatementToBuffer(stmt);
                
            }
            
        }

        /*
         * Blank nodes. Either scoped to the solution (new bnodes map) or reuse
         * a global bnodes map.
         */
        final Map<String, BigdataBNode> bnodes = getBNodeMap();

        // #of statements generated from the current solution.
        int ngenerated = 0;

        // For each statement pattern in the template.
        for (StatementPatternNode pat : templates) {

            /*
             * Attempt to build a statement from this statement pattern and
             * solution.
             */

            final BigdataStatement stmt = makeStatement(pat, solution, bnodes);

            if (stmt != null) {

                // If successful, then add to the buffer.
                addStatementToBuffer(stmt);
                
                ngenerated++;

            }

        }
        
        if (ngenerated == 0 && DEBUG) {

            log.debug("No statements generated for this solution: " + solution);
            
        }

    }

    /**
     * Add a statement to the output buffer.
     * 
     * @param stmt
     *            The statement.
     * 
     *            FIXME NATIVE DISTINCT: This method needs to be vectored for
     *            native distinct.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-260"> native
     *      distinct in quad mode (insert/delete) </a>
     */
    private void addStatementToBuffer(final BigdataStatement stmt) {

        if (DEBUG)
            log.debug(stmt.toString());

        if (filter != null) {

            /*
             * Impose a DISTINCT SPO filter on the generated statements in the
             * constructed graph.
             * 
             * @see <a
             * href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
             * CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
             */

            if (filter.isValid(stmt)) {

                buffer.add(stmt);
                
            }
            
        } else {

            buffer.add(stmt);

        }
        
    }
    
    /**
     * Return a statement if a valid statement could be constructed for that
     * statement pattern and this solution.
     * 
     * @param pat
     *            A statement pattern from the construct template.
     * @param solution
     *            A solution from the query.
     * @param bnodes
     *            A map used to scope blank nodes to the solution.
     * 
     * @return A statement if a valid statement could be constructed for that
     *         statement pattern and this solution.
     */
    private BigdataStatement makeStatement(final StatementPatternNode pat,
            final BindingSet solution, final Map<String, BigdataBNode> bnodes) {

        // resolve values from template and/or solution.
        final BigdataValue s = getValue(pat.s(), solution, bnodes);
        final BigdataValue p = getValue(pat.p(), solution, bnodes);
        final BigdataValue o = getValue(pat.o(), solution, bnodes);
        final BigdataValue c = pat.c() == null ? null : getValue(pat.c(),
                solution, bnodes);

        // filter out unbound values.
        if (s == null || p == null || o == null)
            return null;

        // filter out bindings which do not produce legal statements.
        if (!(s instanceof Resource))
            return null;
        if (!(p instanceof URI))
            return null;
        if (!(o instanceof Value))
            return null;
        if (c != null && !(c instanceof Resource))
            return null;

        // return the statement
        return f.createStatement((Resource) s, (URI) p, (Value) o, (Resource) c);
        
    }

    /**
     * Return the as-bound value of the variable or constant given the solution.
     * 
     * @param term
     *            Either a variable or a constant from the statement pattern in
     *            the template.
     * @param solution
     *            A solution from the query.
     * @param bnodes
     *            A map used to scope blank nodes to the solution.
     * 
     * @return The as-bound value.
     */
    private BigdataValue getValue(//
            final TermNode term,//
            final BindingSet solution,//
            final Map<String, BigdataBNode> bnodes//
            ) {

        if (term instanceof ConstantNode) {

            /*
             * A constant. Something that was specified in the original query as
             * a constant.
             */

            // Get the BigdataValue from the valueCache on the IV for the
            // Constant.
            final BigdataValue value = term.getValue();

            if (value == null) {

                /**
                 * If the value is null, then the valueCache was not set on the
                 * IV for the Constant. Since the Constant was a Constant
                 * specified in the original query, this means that is an IV in
                 * the original AST whose valueCache was not set.
                 * 
                 * This problem was first observed when developing CBD support.
                 * The CBD class is explicitly setting the IVs when it builds
                 * the DESCRIBE queries for the CBD expansions. However, it was
                 * failing to resolve the BigdataValue for those IVs and set the
                 * valueCache on the IV. This assert was added to detect that
                 * problem and prevent regressions.
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/578">
                 *      Concise Bounded Description </a>
                 */

                throw new AssertionError("BigdataValue not available: " + term
                        + ", term.iv=" + term.getValueExpression().get());

            }

            if (value instanceof BigdataBNode) {

                /*
                 * Blank nodes require special handling.
                 */
                
                if (this.bnodes != null) {
                    
                    /*
                     * DESCRIBE
                     * 
                     * Note: This preserves the ID/IV for constants in the
                     * query, but it breaks semantics for CONSTRUCT. The test
                     * [this.bnodes!=null] is a clear indicator that the blank
                     * nodes need to be scoped for a DESCRIBE query.
                     * 
                     * Note: This can only happen when we hand-code a DESCRIBE
                     * query for a CBD expansion round and explicitly set the IV
                     * on the blank node in the SELECT clause.
                     */
          
                    return value;
                    
                }

                /*
                 * CONSTRUCT.
                 * 
                 * Note: The blank nodes will be scoped to the solution.
                 */
                
                final String id = ((BigdataBNode) value).getID();

                final BigdataBNode bnode = getBNode(id, bnodes);

                return bnode;

            }
            
            return value;

        } else if (term instanceof VarNode) {

            /*
             * A variable.
             * 
             * There are two cases: (1) Variables that were specified in the
             * original query as variables; and (2) "Anonymous" variables, which
             * were specified in the original query and then translated into
             * anonymous variables.
             */
            
            final VarNode v = (VarNode) term;

            final String varname = v.getValueExpression().getName();

            if (v.isAnonymous()) {

                /*
                 * Anonymous variable. I can't quite say whether or not this is
                 * a hack, so let me explain what is going on instead. When the
                 * SPARQL grammar parses a blank node in a query, it is *always*
                 * turned into an anonymous variable. So, when we interpret the
                 * CONSTRUCT template, we are going to see anonymous variables
                 * and we have to recognize them and treat them as if they were
                 * really blank nodes.
                 * 
                 * The code here tests VarNode.isAnonymous() and, if the
                 * variable is anonymous, it uses the variable's *name* as a
                 * blank node identifier (ID). It then obtains a unique within
                 * scope blank node which is correlated with that blank node ID.
                 */
                
                return getBNode(varname, bnodes);

            }

            /*
             * Given variable.
             */

            // Resolve the binding for the variable in the solution.
            final BigdataValue val = (BigdataValue) solution.getValue(varname);

            /*
             * Note: Doing this will cause several of the DAWG CONSTRUCT tests
             * to fail...
             */
            if (false && val instanceof BigdataBNode) {
 
                return getBNode(((BigdataBNode) val).getID(), bnodes);
            
            }
            
            return val;

        } else {
            
            // TODO Support the BNode() function here?
            throw new UnsupportedOperationException("term: "+term);
            
        }
        
    }
    
    /**
     * Scope the bnode ID to the solution. The same ID in each solution is
     * mapped to the same bnode. The same ID in a new solution is mapped to a
     * new BNode.
     */
    private BigdataBNode getBNode(final String id,
            final Map<String, BigdataBNode> bnodes) {

        final BigdataBNode tmp = bnodes.get(id);

        if (tmp != null) {

            // We've already seen this ID for this solution.
            return tmp;

        }

        /*
         * This is the first time we have seen this ID for this solution. We
         * create a new blank node with an identifier which will be unique
         * across the solutions.
         */

        // new bnode, which will be scoped by the bnodes map (that is, to
        // the solution for CONSTRUCT and to the graph for DESCRIBE).
        final BigdataBNode bnode = f.createBNode("b"
                + Integer.valueOf(bnodeIdFactory++).toString());

        // put into the per-solution cache.
        bnodes.put(id, bnode);
        
        return bnode;

    }
    
}
