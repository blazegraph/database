package com.bigdata.rdf.store;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.AbstractChunkedResolverator;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Efficiently resolve openrdf {@link BindingSet}s to bigdata {@link IBindingSets}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataOpenRDFBindingSetsResolverator
        extends
        AbstractChunkedResolverator<BindingSet, IBindingSet, AbstractTripleStore> {

    final private static Logger log = Logger
            .getLogger(BigdataOpenRDFBindingSetsResolverator.class);

    /**
     * 
     * @param db
     *            Used to resolve RDF {@link Value}s to {@link IV}s.
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed).
     * 
     *            FIXME must accept reverse bnodes map (from term identifier to
     *            blank nodes) for resolution of blank nodes within a Sesame
     *            connection context. [Is this comment relevant for this class?]
     */
    public BigdataOpenRDFBindingSetsResolverator(final AbstractTripleStore db,
            final IChunkedOrderedIterator<BindingSet> src) {

        super(db, src, new BlockingBuffer<IBindingSet[]>(
                db.getChunkOfChunksCapacity(),
                db.getChunkCapacity(),
                db.getChunkTimeout(),
                TimeUnit.MILLISECONDS));

    }

    /**
     * Strengthens the return type.
     */
    public BigdataOpenRDFBindingSetsResolverator start(
            final ExecutorService service) {

        return (BigdataOpenRDFBindingSetsResolverator) super.start(service);
        
    }

    /**
     * Resolve a chunk of {@link BindingSet}s into a chunk of
     * {@link IBindingSet}s in which RDF {@link Value}s have been resolved to
     * {@link IV}s.
     */
    protected IBindingSet[] resolveChunk(final BindingSet[] chunk) {

        if (log.isInfoEnabled())
            log.info("Fetched chunk: size=" + chunk.length);

        /*
         * Create a collection of the distinct term identifiers used in this
         * chunk.
         * 
         * Note: The [initialCapacity] is only an estimate. There are normally
         * multiple values in each binding set. However, it is also common for
         * the same Value to appear across different solutions in a chunk.
         */

        final int initialCapacity = chunk.length;

        final Collection<Value> valueSet = new LinkedHashSet<Value>(initialCapacity);

        for (BindingSet bindingSet : chunk) {

            for(Binding binding : bindingSet) {
                
                final Value value = binding.getValue();
                
                if(value!=null) { 
                 
                    valueSet.add(value);
                    
                }
                
            }

        }

        if (log.isInfoEnabled())
            log.info("Resolving " + valueSet.size() + " term identifiers");

        final LexiconRelation r = state.getLexiconRelation();
        
        final BigdataValueFactory vf = r.getValueFactory();
        
        final int nvalues = valueSet.size();

        /*
         * Convert to a BigdataValue[], building up a Map used to translate from
         * Value to BigdataValue as we go.
         */
        final BigdataValue[] values = new BigdataValue[nvalues];
        final Map<Value,BigdataValue> map = new LinkedHashMap<Value, BigdataValue>(nvalues);
        {

            int i = 0;
            
            for (Value value : valueSet) {

                final BigdataValue val = vf.asValue(value); 
                
                map.put(value, val);
                
                values[i++] = val; 
                
            }

        }
        
        // Batch resolve against the database.
        r.addTerms(values, nvalues, true/*readOnly*/);
        
        // Assemble a chunk of resolved elements
        {

            final IBindingSet[] chunk2 = new IBindingSet[chunk.length];
            int i = 0;
            for (BindingSet e : chunk) {

                final IBindingSet f = getBindingSet(e, map);

                chunk2[i++] = f;

            }

            // return the chunk of resolved elements.
            return chunk2;
            
        }
        
    }

    /**
     * Resolve the RDF {@link Value}s in the {@link BindingSet} using the map
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
     */
    @SuppressWarnings("unchecked")
    private IBindingSet getBindingSet(final BindingSet bindingSet,
            final Map<Value,BigdataValue> map) {

        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        if (map == null)
            throw new IllegalArgumentException();

        final IBindingSet out = new ListBindingSet();
        
        for(Binding binding : bindingSet) {
            
            final String name = binding.getName();
            
            final Value value = binding.getValue();
            
            final BigdataValue outVal = map.get(value);

            assert outVal != null;

            final Constant<?> c;
            
            if (outVal.getIV() == null) {

                c = new Constant(TermId.mockIV(VTE.valueOf(value)));
                
            } else {
                
                c = new Constant(outVal.getIV());
                
            }
            
            out.set(Var.var(name), c);

        }
        
        return out;

    }
    
}
