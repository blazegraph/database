package com.bigdata.rdf.store;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.openrdf.model.Value;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.AbstractChunkedResolverator;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Efficiently resolve term identifiers in Bigdata {@link ISolution}s to RDF
 * {@link BigdataValue}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataSolutionResolverator
        extends
        AbstractChunkedResolverator<ISolution, IBindingSet, AbstractTripleStore> {

    /**
     * 
     * @param db
     *            Used to resolve term identifiers to {@link Value} objects.
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed).
     * 
     *            FIXME must accept reverse bnodes map (from term identifier to
     *            blank nodes) for resolution of blank nodes within a Sesame
     *            connection context.
     */
    public BigdataSolutionResolverator(final AbstractTripleStore db,
            final IChunkedOrderedIterator<ISolution> src) {

        super(db, src, new BlockingBuffer<IBindingSet[]>(
                db.getChunkOfChunksCapacity(),
                db.getChunkCapacity(),
                db.getChunkTimeout(),
                TimeUnit.MILLISECONDS));

    }

    /**
     * Strengthens the return type.
     */
    public BigdataSolutionResolverator start(ExecutorService service) {

        return (BigdataSolutionResolverator) super.start(service);
        
    }

    /**
     * Resolve a chunk of {@link ISolution}s into a chunk of
     * {@link IBindingSet}s in which term identifiers have been resolved to
     * {@link BigdataValue}s.
     */
    protected IBindingSet[] resolveChunk(final ISolution[] chunk) {

        if (log.isInfoEnabled())
            log.info("Fetched chunk: size=" + chunk.length);

        /*
         * Create a collection of the distinct term identifiers used in this
         * chunk.
         */
        
        final Collection<IV> ids = new HashSet<IV>(chunk.length
                * state.getSPOKeyArity());

        for (ISolution solution : chunk) {

            final IBindingSet bindingSet = solution.getBindingSet();

            assert bindingSet != null;

            final Iterator<Map.Entry<IVariable, IConstant>> itr = bindingSet
                    .iterator();

            while (itr.hasNext()) {

                final Map.Entry<IVariable, IConstant> entry = itr.next();

                final IV iv = (IV) entry.getValue().get();

                if (iv == null) {

                    throw new RuntimeException("NULL? : var=" + entry.getKey()
                            + ", " + bindingSet);
                    
                }
                
                ids.add(iv);

            }

        }

        if (log.isInfoEnabled())
            log.info("Resolving " + ids.size() + " term identifiers");

        // batch resolve term identifiers to terms.
        final Map<IV, BigdataValue> terms = state.getLexiconRelation()
                .getTerms(ids);

        /*
         * Assemble a chunk of resolved elements.
         */
        {

            final IBindingSet[] chunk2 = new IBindingSet[chunk.length];
            int i = 0;
            for (ISolution e : chunk) {

                final IBindingSet f = getBindingSet(e, terms);

                chunk2[i++] = f;

            }

            // return the chunk of resolved elements.
            return chunk2;
            
        }
        
    }

    /**
     * Resolve the term identifiers in the {@link ISolution} using the map
     * populated when we fetched the current chunk and return the
     * {@link IBindingSet} for that solution in which term identifiers have been
     * resolved to their corresponding {@link BigdataValue}s.
     * 
     * @param solution
     *            A solution whose {@link Long}s will be interpreted as term
     *            identifiers and resolved to the corresponding
     *            {@link BigdataValue}s.
     * 
     * @return The corresponding {@link IBindingSet} in which the term
     *         identifiers have been resolved to {@link BigdataValue}s.
     * 
     * @throws IllegalStateException
     *             if the {@link IBindingSet} was not materialized with the
     *             {@link ISolution}.
     * 
     * @todo this points out a problem where we would be better off strongly
     *       typing the term identifiers with their own class rather than using
     *       {@link Long} since we can not distinguish a {@link Long}
     *       materialized by a join against some non-RDF relation from a
     *       {@link Long} that is a term identifier.
     */
    private IBindingSet getBindingSet(final ISolution solution,
            final Map<IV, BigdataValue> terms) {

        if (solution == null)
            throw new IllegalArgumentException();
        
        if (terms == null)
            throw new IllegalArgumentException();

        final IBindingSet bindingSet = solution.getBindingSet();
        
        if(bindingSet == null) {
            
            throw new IllegalStateException("BindingSet was not materialized");
            
        }

        final Iterator<Map.Entry<IVariable, IConstant>> itr = bindingSet
                .iterator();

        while (itr.hasNext()) {

            final Map.Entry<IVariable, IConstant> entry = itr.next();

            final Object boundValue = entry.getValue().get();

            if (!(boundValue instanceof IV)) {

                /*
                 * FIXME This assumes that any Long is a term identifier. The
                 * term identifiers should be strongly typed.
                 */

                continue;

            }

            final IV iv = (IV) boundValue;

            final BigdataValue value = terms.get(iv);

            if (value == null) {

                throw new RuntimeException("Could not resolve termId="
                        + iv);
            }

            /*
             * Replace the binding.
             * 
             * FIXME This probably needs to strip out the BigdataSail#NULL_GRAPH
             * since that should not become bound.
             */
            bindingSet.set(entry.getKey(), new Constant<BigdataValue>(
                    value));
            
        }
        
        return bindingSet;

    }
    
}
