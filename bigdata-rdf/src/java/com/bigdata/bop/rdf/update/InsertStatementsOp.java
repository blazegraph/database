/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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

import java.beans.Statement;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Vectored insert operator for RDF Statements. The solutions flowing through
 * this operator MUST bind the <code>s</code>, <code>p</code>, <code>o</code>,
 * and MAY bind the <code>c</code> variable. Those variables correspond to the
 * Subject, Predicate, Object, and Context/Graph position of an RDF
 * {@link Statement} respectively. On input, the variables must be real
 * {@link IV}s. The {@link IVCache} does NOT need to be set. The output is an
 * empty solution.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public final class InsertStatementsOp extends PipelineOp {

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * TODO Lift into a common interface shared by {@link IPredicate}.
         */
        public String TIMESTAMP = IPredicate.Annotations.TIMESTAMP;

        /**
         * TODO Lift into a common interface shared by {@link IPredicate} (and
         * all other operations which require the NAMESPACE of the lexicon?).
         * 
         * Note: We need the lexicon namespace for the terms (one operator) and
         * the spo namespace for the spo add/remove.
         */
        public String RELATION_NAME = IPredicate.Annotations.RELATION_NAME;

    }

    public InsertStatementsOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

    }

    public InsertStatementsOp(final InsertStatementsOp op) {
        super(op);
    }

    /**
         * 
         */
    private static final long serialVersionUID = 1L;

    // public BaseJoinStats newStats() {
    //
    // return new BaseJoinStats();
    //
    // }

    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(context, this));

    }

    static private class ChunkTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final AbstractTripleStore tripleStore;

        private final boolean sids;

        private final boolean quads;

        private final Var<?> s = Var.var("s"), p = Var.var("p"), o = Var
                .var("o"), c = Var.var("c");

        public ChunkTask(final BOpContext<IBindingSet> context,
                final InsertStatementsOp op) {

            this.context = context;

            final String namespace = ((String[]) op
                    .getRequiredProperty(Annotations.RELATION_NAME))[0];

            final long timestamp = (Long) op
                    .getRequiredProperty(Annotations.TIMESTAMP);

            this.tripleStore = (AbstractTripleStore) context.getResource(
                    namespace, timestamp);

            this.sids = tripleStore.isStatementIdentifiers();

            this.quads = tripleStore.isQuads();

        }

        @Override
        public Void call() throws Exception {

            // Build set of distinct ISPOs.
            final Set<ISPO> b = acceptSolutions();

            // Convert into array.
            final ISPO[] stmts = b.toArray(new ISPO[b.size()]);

            // Write on the database.
            final long nmodified = tripleStore.addStatements(stmts,
                    stmts.length);

            // TODO Report mutation stats.

            // done.
            return null;

        }

        private Set<ISPO> acceptSolutions() {

            final Set<ISPO> set = new LinkedHashSet<ISPO>();

            final BOpStats stats = context.getStats();

            final ICloseableIterator<IBindingSet[]> itr = context.getSource();

            try {

                int nchunks = 0, nelements = 0;

                while (itr.hasNext()) {

                    final IBindingSet[] a = itr.next();

                    for (IBindingSet bset : a) {

                        set.add(getSPO(bset));

                    }

                    nchunks++;

                    nelements += a.length;

                }

                if (stats != null) {
                    stats.chunksIn.add(nchunks);
                    stats.unitsIn.add(nelements);
                }

                return set;

            } finally {

                if (itr instanceof ICloseableIterator<?>) {

                    ((ICloseableIterator<?>) itr).close();

                }

            }

        }

        /**
         * Return the bound value for the variable.
         * 
         * @param v
         *            The variable.
         * @param bset
         *            The source solution.
         * @param required
         *            <code>true</code> iff the variable must be bound.
         * 
         * @return The bound value and <code>null</code> if the variable is not
         *         bound.
         */
        @SuppressWarnings("rawtypes")
        private IV getIV(final IVariable<?> var, final IBindingSet bset,
                final boolean required) {

            @SuppressWarnings("unchecked")
            final IConstant<IV> constant = bset.get(var);

            if (constant == null) {

                if (required)
                    throw new RuntimeException("Variable is not bound: " + var);

                return null;

            }

            final IV _s = (IV) constant.get();

            return _s;

        }

        /**
         * Return an {@link StatementEnum#Explicit} {@link ISPO} constructed
         * from the source solution.
         * 
         * @param bset
         *            The source solution.
         * 
         * @return The {@link ISPO}.
         */
        private ISPO getSPO(final IBindingSet bset) {

            return new SPO(//
                    getIV(s, bset, false),//
                    getIV(p, bset, false),//
                    getIV(o, bset, false),//
                    (sids | quads ? getIV(c, bset, false) : null),//
                    StatementEnum.Explicit);

        }

    } // ChunkTask

} // InsertStatements