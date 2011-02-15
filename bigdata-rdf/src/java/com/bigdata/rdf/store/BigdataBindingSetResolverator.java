package com.bigdata.rdf.store;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.openrdf.model.Value;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.AbstractChunkedResolverator;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Efficiently resolve term identifiers in Bigdata {@link ISolution}s to RDF
 * {@link BigdataValue}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BigdataSolutionResolverator.java 3448 2010-08-18 20:55:58Z thompsonbry $
 */
public class BigdataBindingSetResolverator
        extends
        AbstractChunkedResolverator<IBindingSet, IBindingSet, AbstractTripleStore> {
	
	private final IVariable[] required;

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
    public BigdataBindingSetResolverator(final AbstractTripleStore db,
            final IChunkedOrderedIterator<IBindingSet> src) {
    	
    	this(db, src, null);
    	
    }
    
    public BigdataBindingSetResolverator(final AbstractTripleStore db,
            final IChunkedOrderedIterator<IBindingSet> src,
            final IVariable[] required) {

        super(db, src, new BlockingBuffer<IBindingSet[]>(
                db.getChunkOfChunksCapacity(),
                db.getChunkCapacity(),
                db.getChunkTimeout(),
                TimeUnit.MILLISECONDS));
        
        this.required = required;
        
    }

    /**
     * Strengthens the return type.
     */
    public BigdataBindingSetResolverator start(ExecutorService service) {

        return (BigdataBindingSetResolverator) super.start(service);
        
    }

    /**
     * Resolve a chunk of {@link ISolution}s into a chunk of
     * {@link IBindingSet}s in which term identifiers have been resolved to
     * {@link BigdataValue}s.
     */
    protected IBindingSet[] resolveChunk(final IBindingSet[] chunk) {

        if (log.isInfoEnabled())
            log.info("Fetched chunk: size=" + chunk.length);

        /*
         * Create a collection of the distinct term identifiers used in this
         * chunk.
         */
        
        final Collection<IV> ids = new HashSet<IV>(chunk.length
                * state.getSPOKeyArity());

        for (IBindingSet solution : chunk) {

            final IBindingSet bindingSet = solution;

            assert bindingSet != null;

            if (required == null) {
            	
                final Iterator<Map.Entry<IVariable, IConstant>> itr = 
                	bindingSet.iterator();

	            while (itr.hasNext()) {
	
	                final Map.Entry<IVariable, IConstant> entry = itr.next();
	                
	                final IV iv = (IV) entry.getValue().get();
	
	                if (iv == null) {
	
	                    throw new RuntimeException("NULL? : var=" + entry.getKey()
	                            + ", " + bindingSet);
	                    
	                }
	                
	                ids.add(iv);
	
	            }
	            
            } else {
            	
            	for (IVariable v : required) {
            		
            		final IV iv = (IV) bindingSet.get(v).get();
	            	
	                if (iv == null) {
	
	                    throw new RuntimeException("NULL? : var=" + v
	                            + ", " + bindingSet);
	                    
	                }
	                
	                ids.add(iv);
	                
            	}
            	
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
            for (IBindingSet e : chunk) {

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
    private IBindingSet getBindingSet(final IBindingSet solution,
            final Map<IV, BigdataValue> terms) {

        if (solution == null)
            throw new IllegalArgumentException();
        
        if (terms == null)
            throw new IllegalArgumentException();

        if(solution == null) {
            
            throw new IllegalStateException("BindingSet was not materialized");
            
        }

        final IBindingSet bindingSet;
        if (required == null) {
        	bindingSet = solution;
        } else {
        	bindingSet = solution.copy(required);
        }
        	
        final Iterator<Map.Entry<IVariable, IConstant>> itr = bindingSet
                .iterator();

        while (itr.hasNext()) {

            final Map.Entry<IVariable, IConstant> entry = itr.next();

            final Object boundValue = entry.getValue().get();

            if (!(boundValue instanceof IV)) {

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
