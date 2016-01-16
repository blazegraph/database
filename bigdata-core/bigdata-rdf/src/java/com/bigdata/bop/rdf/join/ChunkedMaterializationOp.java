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
 * Created on Nov 3, 2011
 */

package com.bigdata.bop.rdf.join;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.store.BigdataBindingSetResolverator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A vectored materialization operator based on pretty much the same logic as
 * {@link BigdataBindingSetResolverator}. However, this class caches the
 * resolved {@link BigdataValue} reference on the {@link IV} while the
 * {@link BigdataBindingSetResolverator} replaces the {@link IV} in the solution
 * with the {@link BigdataValue}. Also, this class does not filter out variables
 * which are not being materialized.
 * 
 * @see ChunkedMaterializationIterator
 * @see BigdataBindingSetResolverator
 */
public class ChunkedMaterializationOp extends PipelineOp {

    private final static Logger log = Logger
            .getLogger(ChunkedMaterializationOp.class);

    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * The {@link IVariable}[] identifying the variables to be materialized.
         * When <code>null</code> or not specified, ALL variables will be
         * materialized. This may not be an empty array as that would imply that
         * there is no need to use this operator.
         */
        String VARS = ChunkedMaterializationOp.class.getName()+".vars";

        String RELATION_NAME = Predicate.Annotations.RELATION_NAME;

        String TIMESTAMP = Predicate.Annotations.TIMESTAMP;
        
        /**
         * If true, materialize inline values in addition to term IDs.
         */
        String MATERIALIZE_INLINE_IVS = ChunkedMaterializationOp.class.getName()+".materializeAll";
        
        /**
         * Default materialize all is false.
         */
        boolean DEFAULT_MATERIALIZE_INLINE_IVS = false;

    }

    /**
     * @param args
     * @param annotations
     */
    public ChunkedMaterializationOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);
        
        final IVariable<?>[] vars = getVars();

        if (vars != null && vars.length == 0)
            throw new IllegalArgumentException();

        getRequiredProperty(Annotations.RELATION_NAME);
        
        getRequiredProperty(Annotations.TIMESTAMP);

    }

    /**
     * @param op
     */
    public ChunkedMaterializationOp(final ChunkedMaterializationOp op) {

        super(op);
        
    }

    public ChunkedMaterializationOp(final BOp[] args, final NV... annotations) {

        this(args, NV.asMap(annotations));

    }

    /**
     * 
     * @param vars
     *            The variables to be materialized. Materialization is only
     *            attempted for those variables which are actually bound in
     *            given solution.
     * @param namespace
     *            The namespace of the {@link LexiconRelation}.
     * @param timestamp
     *            The timestamp against which to read.
     */
    public ChunkedMaterializationOp(final BOp[] args,
            final IVariable<?>[] vars, final String namespace,
            final long timestamp) {
        this(args, //
                new NV(Annotations.VARS, vars),//
                new NV(Annotations.RELATION_NAME, new String[] { namespace }), //
                new NV(Annotations.TIMESTAMP, timestamp) //
        );
    }
    
    /**
     * Return the variables to be materialized.
     * 
     * @return The variables to be materialized -or- <code>null</code> iff all
     *         variables should be materialized.
     * 
     * @see Annotations#VARS
     */
    public IVariable<?>[] getVars() {

        return (IVariable<?>[]) getProperty(Annotations.VARS);
        
    }

    /**
     * When <code>true</code>, inline {@link IV}s are also materialized.
     * 
     * @see Annotations#MATERIALIZE_INLINE_IVS
     */
    public boolean materializeInlineIVs() {

        return getProperty(Annotations.MATERIALIZE_INLINE_IVS,
                Annotations.DEFAULT_MATERIALIZE_INLINE_IVS);

    }

    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(this, context));

    }

    /**
     * Task executing on the node.
     */
    static private class ChunkTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        /**
         * The variables to be materialized.
         */
        private final IVariable<?>[] vars;

        private final String namespace;

        private final long timestamp;
        
        private final boolean materializeInlineIVs;

        ChunkTask(final ChunkedMaterializationOp op,
                final BOpContext<IBindingSet> context
                ) {

            this.context = context;

            this.vars = op.getVars();

            namespace = ((String[]) op.getProperty(Annotations.RELATION_NAME))[0];

            timestamp = (Long) op.getProperty(Annotations.TIMESTAMP);
            
            materializeInlineIVs = op.materializeInlineIVs();

        }

        @Override
        public Void call() throws Exception {

            final BOpStats stats = context.getStats();

            final ICloseableIterator<IBindingSet[]> itr = context
                    .getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            try {

                final LexiconRelation lex = (LexiconRelation) context
                        .getResource(namespace, timestamp);

                while (itr.hasNext()) {

                    final IBindingSet[] a = itr.next();

                    stats.chunksIn.increment();
                    stats.unitsIn.add(a.length);

                    resolveChunk(vars, lex, a, materializeInlineIVs);

                    sink.add(a);

                }

                sink.flush();

                // done.
                return null;

            } finally {

                sink.close();

            }

        }

    } // ChunkTask

    /**
     * Resolve a chunk of {@link IBindingSet}s into a chunk of
     * {@link IBindingSet}s in which {@link IV}s have been resolved to
     * {@link BigdataValue}s.
     * 
     * @param required
     *            The variable(s) to be materialized or <code>null</code> to
     *            materialize all variable bindings.
     * @param lex
     *            The lexicon reference.
     * @param chunk
     *            The chunk of solutions whose variables will be materialized.
     */
    static void resolveChunk(final IVariable<?>[] required,
            final LexiconRelation lex,//
            final IBindingSet[] chunk,//
            final boolean materializeInlineIVs//
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

            // System.err.println(solution);

            assert bindingSet != null;

            if (required == null) {

                // Materialize all variable bindings.
                @SuppressWarnings("rawtypes")
                final Iterator<Map.Entry<IVariable, IConstant>> itr = bindingSet
                        .iterator();

                while (itr.hasNext()) {

                    @SuppressWarnings("rawtypes")
                    final Map.Entry<IVariable, IConstant> entry = itr.next();

                    final IV<?, ?> iv = (IV<?, ?>) entry.getValue().get();

                    if (iv == null) {

                        throw new RuntimeException("NULL? : var="
                                + entry.getKey() + ", " + bindingSet);

                    }

                    if (iv.needsMaterialization() || materializeInlineIVs) {

                        ids.add(iv);

                    }

//                    handleIV(iv, ids, materializeInlineIVs);
                    
                }

            } else {

                // Materialize the specified variable bindings.
                for (IVariable<?> v : required) {

                    final IConstant<?> c = bindingSet.get(v);

                    if (c == null) {
                        continue;
                    }

                    final IV<?, ?> iv = (IV<?, ?>) c.get();

                    if (iv == null) {

                        throw new RuntimeException("NULL? : var=" + v + ", "
                                + bindingSet);

                    }

                    if (iv.needsMaterialization() || materializeInlineIVs) {

                        ids.add(iv);

                    }

//                    handleIV(iv, ids, materializeInlineIVs);
                    
                }

            }

        }

        // System.err.println("resolving: " +
        // Arrays.toString(ids.toArray()));

        if (log.isInfoEnabled())
            log.info("Resolving " + ids.size() + " IVs, required="
                    + Arrays.toString(required));

        // batch resolve term identifiers to terms.
        final Map<IV<?, ?>, BigdataValue> terms = lex.getTerms(ids);

        /*
         * Resolve the IVs.
         */
        for (IBindingSet e : chunk) {

            getBindingSet(required, e, terms);

        }

    }
    
//    /**
//     * Either add the IV to the list if it needs materialization, or else
//     * delegate to {@link #handleSid(SidIV, Collection, boolean)} if it's a
//     * SidIV.
//     */
//    static private void handleIV(final IV<?, ?> iv, 
//    		final Collection<IV<?, ?>> ids, 
//    		final boolean materializeInlineIVs) {
//    	
//    	if (iv instanceof SidIV) {
//    		
//    		handleSid((SidIV<?>) iv, ids, materializeInlineIVs);
//    		
//    	} else if (iv.needsMaterialization() || materializeInlineIVs) {
//    		
//    		ids.add(iv);
//    		
//    	}
//    	
//    }
//    
//    /**
//     * Sids need to be handled specially because their individual ISPO
//     * components might need materialization.
//     */
//    static private void handleSid(final SidIV<?> sid,
//    		final Collection<IV<?, ?>> ids, 
//    		final boolean materializeInlineIVs) {
//    	
//    	final ISPO spo = sid.getInlineValue();
//    	
//    	System.err.println("handling a sid");
//    	System.err.println("adding s: " + spo.s());
//    	System.err.println("adding p: " + spo.p());
//    	System.err.println("adding o: " + spo.o());
//    	
//    	handleIV(spo.s(), ids, materializeInlineIVs);
//    	
//    	handleIV(spo.p(), ids, materializeInlineIVs);
//    	
//    	handleIV(spo.o(), ids, materializeInlineIVs);
//    	
//    	if (spo.c() != null) {
//    		
//    		handleIV(spo.c(), ids, materializeInlineIVs);
//    		
//    	}
//
//    }

    /**
     * Resolve the term identifiers in the {@link IBindingSet} using the map
     * populated when we fetched the current chunk.
     * 
     * @param required
     *            The variables to be resolved -or- <code>null</code> if all
     *            variables should have been resolved.
     * @param bindingSet
     *            A solution whose {@link IV}s will be resolved to the
     *            corresponding {@link BigdataValue}s in the caller's
     *            <code>terms</code> map. The {@link IVCache} associations are
     *            set as a side-effect.
     * @param terms
     *            A map from {@link IV}s to {@link BigdataValue}s.
     * 
     * @throws IllegalStateException
     *             if the {@link IBindingSet} was not materialized with the
     *             {@link IBindingSet}.
     */
    static private void getBindingSet(//
            final IVariable<?>[] required,
            final IBindingSet bindingSet,
            final Map<IV<?, ?>, BigdataValue> terms) {

        if (bindingSet == null)
            throw new IllegalArgumentException();

        if (terms == null)
            throw new IllegalArgumentException();

        if (required != null) {

            /*
             * Only the specified variables.
             */
            
            for (IVariable<?> var : required) {

                @SuppressWarnings("unchecked")
                final IConstant<IV<?, ?>> c = bindingSet.get(var);

                if (c == null) {
                    // Variable is not bound in this solution.
                    continue;

                }

                final IV<?, ?> iv = (IV<?, ?>) c.get();

                if (iv == null) {

                    continue;

                }

                final BigdataValue value = terms.get(iv);
                
                conditionallySetIVCache(iv,value);

            }
            
        } else {
            
            /*
             * Everything in the binding set.
             */
            
            @SuppressWarnings("rawtypes")
            final Iterator<Map.Entry<IVariable, IConstant>> itr = bindingSet
                    .iterator();

            while (itr.hasNext()) {

                @SuppressWarnings("rawtypes")
                final Map.Entry<IVariable, IConstant> entry = itr.next();

                final Object boundValue = entry.getValue().get();

                if (!(boundValue instanceof IV)) {

                    continue;

                }

                final IV<?, ?> iv = (IV<?, ?>) boundValue;

                final BigdataValue value = terms.get(iv);

                conditionallySetIVCache(iv, value);

            }

        }

	}

    /**
	 * If the {@link BigdataValue} is non-null, then set it on the
	 * {@link IVCache} interface.
	 * 
	 * @param iv
	 *            The {@link IV}
	 * @param value
	 *            The {@link BigdataValue} for that {@link IV} (from the
	 *            dictionary).
	 * 
	 * @throws RuntimeException
	 *             If the {@link BigdataValue} is null (could not be discovered
	 *             in the dictionary) and the {@link IV} requires
	 *             materialization ({@link IV#needsMaterialization() is
	 *             <code>true</code>).
	 * 
	 * @see #1028 (xsd:boolean materialization issue)
	 */
	private static void conditionallySetIVCache(IV<?, ?> iv, BigdataValue value) {

		if (value == null) {

			if (iv.needsMaterialization()) {
				// Not found in dictionary. This is an error.
				throw new RuntimeException("Could not resolve: iv=" + iv);

			} // else NOP - Value is not required.

		} else {

			/*
			 * Value was found in the dictionary, so replace the binding.
			 * 
			 * FIXME This probably needs to strip out the BigdataSail#NULL_GRAPH
			 * since that should not become bound.
			 */
			((IV) iv).setValue(value);
		}

	}

}
