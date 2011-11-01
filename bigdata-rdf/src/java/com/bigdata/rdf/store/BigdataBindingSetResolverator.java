package com.bigdata.rdf.store;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.striterator.AbstractChunkedResolverator;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Efficiently resolve term identifiers in Bigdata {@link IBindingSet}s to RDF
 * {@link BigdataValue}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BigdataSolutionResolverator.java 3448 2010-08-18 20:55:58Z thompsonbry $
 */
public class BigdataBindingSetResolverator
        extends
        AbstractChunkedResolverator<IBindingSet, IBindingSet, AbstractTripleStore> {

    final private static Logger log = Logger
            .getLogger(BigdataBindingSetResolverator.class);

    @SuppressWarnings("rawtypes")
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
    
    /**
     * 
     * @param db
     *            Used to resolve term identifiers to {@link Value} objects.
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed).
     * @param required
     *            The variables to be resolved (optional). When
     *            <code>null</code>, all variables will be resolved.
     */
    public BigdataBindingSetResolverator(final AbstractTripleStore db,
            final IChunkedOrderedIterator<IBindingSet> src,
            final IVariable[] required) {

        super(db, src, new BlockingBuffer<IBindingSet[]>(
                db.getChunkOfChunksCapacity(),
                db.getChunkCapacity(),
                db.getChunkTimeout(),
                TimeUnit.MILLISECONDS));
        
        this.required = required;
        
//        System.err.println("required: " + (required != null ? Arrays.toString(required) : "null"));

    }

    /**
     * Strengthens the return type.
     */
    public BigdataBindingSetResolverator start(final ExecutorService service) {

        return (BigdataBindingSetResolverator) super.start(service);
        
    }

    /**
     * Resolve a chunk of {@link IBindingSet}s into a chunk of
     * {@link IBindingSet}s in which term identifiers have been resolved to
     * {@link BigdataValue}s.
     */
    protected IBindingSet[] resolveChunk(final IBindingSet[] chunk) {

        return resolveChunk(required, state.getLexiconRelation(), chunk);

    }

    /**
     * Resolve a chunk of {@link IBindingSet}s into a chunk of
     * {@link IBindingSet}s in which term identifiers have been resolved to
     * {@link BigdataValue}s.
     * 
     * @param required
     *            The variable(s) to be materialized. When <code>null</code>,
     *            everything will be materialized. If a zero length array is
     *            given, then NOTHING will be materialized and the outputs
     *            solutions will be empty.
     * @param lex
     *            The lexicon reference.
     * @param chunk
     *            The chunk of solutions whose variables will be materialized.
     * 
     * @return A new chunk of solutions in which those variables have been
     *         materialized.
     */
    static public IBindingSet[] resolveChunk(final IVariable<?>[] required,
            final LexiconRelation lex,//
            final IBindingSet[] chunk//
            ) {
    
        if (log.isInfoEnabled())
            log.info("Fetched chunk: size=" + chunk.length + ", chunk="
                    + Arrays.toString(chunk));

        /*
         * Create a collection of the distinct term identifiers used in this
         * chunk.
         */
        
        /*
         * Estimate the capacity of the hash map based on the #of variables to
         * materialize per solution and the #of solutions.
         */
        final int initialCapacity = required == null ? chunk.length
                : ((required.length == 0) ? 1 : chunk.length * required.length);

        final Collection<IV<?, ?>> ids = new HashSet<IV<?, ?>>(initialCapacity);

        for (IBindingSet solution : chunk) {

            final IBindingSet bindingSet = solution;

//            System.err.println(solution);
            
            assert bindingSet != null;

            if (required == null) {
            	
                @SuppressWarnings("rawtypes")
                final Iterator<Map.Entry<IVariable, IConstant>> itr = 
                	bindingSet.iterator();

	            while (itr.hasNext()) {
	
	                @SuppressWarnings("rawtypes")
                    final Map.Entry<IVariable, IConstant> entry = itr.next();
	                
	                final IV<?,?> iv = (IV<?,?>) entry.getValue().get();
	
	                if (iv == null) {
	
	                    throw new RuntimeException("NULL? : var=" + entry.getKey()
	                            + ", " + bindingSet);
	                    
	                }
	                
	                ids.add(iv);
	
	            }
	            
            } else {
            	
            	for (IVariable<?> v : required) {

            		final IConstant<?> c = bindingSet.get(v);
            		
            		if (c == null) {
            			continue;
            		}
            		
            		final IV<?,?> iv = (IV<?,?>) c.get();
	            	
	                if (iv == null) {
	
	                    throw new RuntimeException("NULL? : var=" + v
	                            + ", " + bindingSet);
	                    
	                }
	                
	                ids.add(iv);
	                
            	}
            	
            }

        }
        
//        System.err.println("resolving: " + Arrays.toString(ids.toArray()));

        if (log.isInfoEnabled())
            log.info("Resolving " + ids.size() + " term identifiers, required="
                    + Arrays.toString(required));

        // batch resolve term identifiers to terms.
        final Map<IV<?, ?>, BigdataValue> terms = lex.getTerms(ids);

        /*
         * Assemble a chunk of resolved elements.
         */
        {

            final IBindingSet[] chunk2 = new IBindingSet[chunk.length];
            int i = 0;
            for (IBindingSet e : chunk) {

                final IBindingSet f = getBindingSet(e, required, terms);

                chunk2[i++] = f;

            }

            if (log.isInfoEnabled())
                log.info("Resolved chunk: size=" + chunk2.length + ", chunk="
                        + Arrays.toString(chunk2));

            // return the chunk of resolved elements.
            return chunk2;
            
        }
        
    }

    /**
     * Resolve the term identifiers in the {@link IBindingSet} using the map
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
     *             {@link IBindingSet}.
     */
    static private IBindingSet getBindingSet(final IBindingSet solution,
            final IVariable<?>[] required,
            final Map<IV<?,?>, BigdataValue> terms) {

        if (solution == null)
            throw new IllegalArgumentException();
        
        if (terms == null)
            throw new IllegalArgumentException();

        final IBindingSet bindingSet;
        if (required == null) {
        	bindingSet = solution;
        } else {
        	bindingSet = solution.copy(required);
        }
        	
        @SuppressWarnings("rawtypes")
        final Iterator<Map.Entry<IVariable, IConstant>> itr = bindingSet
                .iterator();

        while (itr.hasNext()) {

            @SuppressWarnings("rawtypes")
            final Map.Entry<IVariable, IConstant> entry = itr.next();

            final Object boundValue = entry.getValue().get();

            if (!(boundValue instanceof IV<?,?>)) {

                continue;

            }

            final IV<?,?> iv = (IV<?,?>) boundValue;

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
