package com.bigdata.rdf.store;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.openrdf.model.Value;

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
     */
    public BigdataSolutionResolverator(AbstractTripleStore db,
            IChunkedOrderedIterator<ISolution> src) {

        super(db, src, new BlockingBuffer<IBindingSet[]>(
                db.chunkOfChunksCapacity, db.chunkCapacity, db.chunkTimeout,
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

        if (INFO)
            log.info("Fetched chunk: size=" + chunk.length);

        /*
         * Create a collection of the distinct term identifiers used in this
         * chunk.
         */

        final Collection<Long> ids = new HashSet<Long>(chunk.length * 4);

        for (ISolution solution : chunk) {

            final IBindingSet bindingSet = solution.getBindingSet();

            assert bindingSet != null;

            final Iterator<Map.Entry<IVariable, IConstant>> itr = bindingSet
                    .iterator();

            while (itr.hasNext()) {

                final Map.Entry<IVariable, IConstant> entry = itr.next();

                final Long termId = (Long) entry.getValue().get();

                ids.add(termId);

            }

        }

        if (INFO)
            log.info("Resolving " + ids.size() + " term identifiers");

        // batch resolve term identifiers to terms.
        final Map<Long,BigdataValue> terms = state.getLexiconRelation().getTerms(ids);

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
     *            A solution whose {@link Long}s will be interepreted as term
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
    private IBindingSet getBindingSet(ISolution solution, Map<Long, BigdataValue> terms) {

        if (solution == null)
            throw new IllegalArgumentException();
        
        if (terms == null)
            throw new IllegalArgumentException();

        final IBindingSet bindingSet = solution.getBindingSet();
        
        if(bindingSet == null) {
            
            throw new IllegalStateException("BindingSet was not materialized");
            
        }
        
        final Iterator<Map.Entry<IVariable,IConstant>> itr = bindingSet.iterator();

        while(itr.hasNext()) {

            final Map.Entry<IVariable,IConstant> entry = itr.next();
            
            final Object boundValue = entry.getValue().get();
            
            if (!(boundValue instanceof Long)) {

                /*
                 * FIXME This assumes that any Long is a term identifier. The
                 * term identifiers should be strongly typed.
                 */
                
                continue;
                
            }
            
            final Long termId = (Long)boundValue;
            
            final BigdataValue value = terms.get(termId);

            if (value == null) {

                throw new RuntimeException("Could not resolve termId="
                        + termId);
            }
            
            // replace the binding.
            bindingSet.set(entry.getKey(), new Constant<BigdataValue>(
                    value));
            
        }
        
        return bindingSet;

    }
    
}
