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
 * Created on Mar 16, 2012
 */

package com.bigdata.bop.rdf.update;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.ILocatableResourceAnnotations;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Vectored operator adds and/or resolves the RDF {@link Value}s associated with
 * mock {@link IV}s. On input, the variables must be mock {@link IV}s whose
 * {@link IVCache} is set to the corresponding {@link BigdataValue}. On output,
 * the bindings of the variables will be replaced by the corresponding
 * {@link IV} if it exists or is an inline {@link IV} and (it writing is
 * enabled) the newly assigned {@link IV} for the term.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ChunkedResolutionTask.java 6153 2012-03-16 20:10:15Z
 *          thompsonbry $
 */
public class ChunkedResolutionOp extends PipelineOp {

    private static final transient Logger log = Logger
            .getLogger(ChunkedResolutionOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations,
            ILocatableResourceAnnotations {

        /**
         * An optional {@link IVariable}[] identifying the variables to be
         * resolved. All variables are resolved unless this annotation is
         * specified.
         */
        String VARS = ChunkedResolutionOp.class.getName() + ".vars";

    }

    /**
     * @param args
     * @param annotations
     */
    public ChunkedResolutionOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);
        
    }

    /**
     * @param op
     */
    public ChunkedResolutionOp(final ChunkedResolutionOp op) {
        super(op);
    }

    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(context, this));

    }

    static private class ChunkTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final LexiconRelation lex;

        private final boolean readOnly;

        /**
         * The variables to resolve -or- <code>null</code> to resolve all
         * variables.
         */
        private final IVariable<?>[] vars;

        public ChunkTask(final BOpContext<IBindingSet> context,
                final ChunkedResolutionOp op) {

            this.context = context;

            final String namespace = ((String[]) op
                    .getRequiredProperty(Annotations.RELATION_NAME))[0];

            final long timestamp = (Long) op
                    .getRequiredProperty(Annotations.TIMESTAMP);

            this.lex = (LexiconRelation) context.getResource(namespace,
                    timestamp);

            this.readOnly = TimestampUtility.isReadOnly(timestamp);

            this.vars = (IVariable[]) op.getProperty(Annotations.VARS);

        }

        @Override
        public Void call() throws Exception {

            /*
             * Materialize everything.
             */
            final IBindingSet[] a = BOpUtility.toArray(context.getSource(),
                    context.getStats());

            final IBindingSet[] b = resolve(a);

            context.getSink().add(b);

            context.getSink().flush();

            // done.
            return null;

        }

        /**
         * Resolve the values.
         * 
         * @return New solutions with resolved {@link IV}s.
         */
        @SuppressWarnings({ "rawtypes", "unchecked" })
        private IBindingSet[] resolve(final IBindingSet[] bindingSets) {

            final Map<Value, BigdataValue> values = new LinkedHashMap<Value, BigdataValue>();

            final BigdataValueFactory valueFactory = lex.getValueFactory();

            for (IBindingSet bindings : bindingSets) {

                if (vars == null) {

                    // All bindings.
                    final Iterator<Map.Entry<IVariable, IConstant>> it = bindings
                            .iterator();

                    while (it.hasNext()) {

                        final Map.Entry<IVariable, IConstant> e = it.next();

                        final IV<?, ?> iv = (IV<?, ?>) e.getValue().get();

                        final Value val = iv.getValue();

                        // add BigdataValue variant of the var's Value.
                        values.put(val, valueFactory.asValue(val));

                    }

                } else {

                    // Just the named variables.
                    for (IVariable<?> var : vars) {

                        final IConstant<?> c = bindings.get(var);

                        if (c == null)
                            continue;

                        final IV<?, ?> iv = (IV<?, ?>) c.get();

                        final BigdataValue val = iv.getValue();

                        // add BigdataValue variant of the var's Value.
                        values.put(val, valueFactory.asValue(val));

                    }

                }

            }

            /*
             * Batch resolve term identifiers for those BigdataValues.
             * 
             * Note: If any value does not exist in the lexicon then its term
             * identifier will be ZERO (0L).
             */
            final long mutationCount;
            {

                final BigdataValue[] terms = values.values().toArray(
                        new BigdataValue[] {});

                final long ndistinct = lex.addTerms(terms, terms.length,
                        readOnly);
                
                if (!readOnly) {
                    mutationCount = ndistinct;
                } else {
                    mutationCount = 0;
                }

                // cache the BigdataValues on the IVs for later
                for (BigdataValue term : terms) {

                    final IV iv = term.getIV();

                    if (iv == null) {

                        /*
                         * Since the term identifier is NULL this value is not
                         * known to the kb.
                         */

                        if (log.isInfoEnabled())
                            log.info("Not in knowledge base: " + term);

                        /*
                         * Create a dummy iv and cache the unknown value on it
                         * so that it can be used during query evalution.
                         */
                        final IV dummy = TermId.mockIV(VTE.valueOf(term));

                        term.setIV(dummy);

                        dummy.setValue(term);

                    } else {

                        iv.setValue(term);

                    }

                }

            }

            /*
             * Replace the bindings with one's which have their IV set.
             */

            // Allocate the output solutions array.
            final IBindingSet[] bindingSets2 = new IBindingSet[bindingSets.length];

            // Generate each output solution from each source solution.
            for (int i = 0; i < bindingSets.length; i++) {

                // Make a copy.
                final IBindingSet bindingSet = bindingSets2[i] = bindingSets[i]
                        .clone();

                if (vars == null) {

                    // All bindings.
                    final Iterator<Map.Entry<IVariable, IConstant>> it = bindingSet
                            .iterator();

                    while (it.hasNext()) {

                        final Map.Entry<IVariable, IConstant> e = it.next();

                        final IV<?, ?> iv = (IV<?, ?>) e.getValue().get();

                        final Value val = (Value) iv.getValue();

                        // Lookup the resolved BigdataValue object.
                        final BigdataValue val2 = values.get(val);

                        // Note: if read-only, then val2 can be null.
                        assert readOnly || val2 != null : "value not found: "
                                + val2;

                        if (log.isDebugEnabled())
                            log.debug("value: " + val + " : " + val2 + " ("
                                    + val2.getIV() + ")");

                        // rewrite the constant in the query.
                        bindingSet.set(e.getKey(), new Constant(val2.getIV()));

                    }

                } else {

                    // Just the named variables.
                    for (IVariable<?> var : vars) {

                        final IConstant<?> c = bindingSet.get(var);

                        if (c == null)
                            continue;

                        final IV<?, ?> iv = (IV<?, ?>) c.get();

                        final BigdataValue val = iv.getValue();

                        // Lookup the resolved BigdataValue object.
                        final BigdataValue val2 = values.get(val);

                        // Note: if read-only, then val2 can be null.
                        assert readOnly || val2 != null : "value not found: "
                                + val2;

                        // Verify IVCache is set.
                        assert val2.getIV() == val2;

                        if (log.isDebugEnabled())
                            log.debug("value: " + val + " : " + val2 + " ("
                                    + val2.getIV() + ")");

                        // rewrite the constant in the query.
                        bindingSet.set(var, new Constant(val2.getIV()));

                    }

                }

            }

            if (mutationCount != 0)
                context.getStats().mutationCount.add(mutationCount);

            return bindingSets2;

        }

    } // ChunkTask

} // ChunkedResolutionTask
