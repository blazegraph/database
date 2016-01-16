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
 * Created on Mar 17, 2012
 */

package com.bigdata.bop.rdf.update;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.ILocatableResourceAnnotations;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Abstract base class for operations which add or remove statements from the
 * statement indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractAddRemoveStatementsOp extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * The s, p, o, and c variable names.
     */
    static private final Var<?> s = Var.var("s"), p = Var.var("p"), o = Var
            .var("o"), c = Var.var("c");

    public interface Annotations extends PipelineOp.Annotations,
            ILocatableResourceAnnotations {

    }

    /**
     * @param op
     */
    public AbstractAddRemoveStatementsOp(final AbstractAddRemoveStatementsOp op) {

        super(op);
        
    }

    /**
     * @param args
     * @param annotations
     */
    public AbstractAddRemoveStatementsOp(final BOp[] args,
            final Map<String, Object> annotations) {
        
        super(args, annotations);
        
    }

    /**
     * Return the {@link Set} of distinct {@link ISPO}s extracted from the
     * source solutions.
     * 
     * @param context
     *            The query engine context.
     * @param bindsC
     *            <code>true</code> if the graph variable should be bound in the
     *            statements (SIDs or QUADs).
     *            
     * @return The distinct {@link ISPO}.
     */
    static protected Set<ISPO> acceptSolutions(
            final BOpContext<IBindingSet> context, final boolean bindsC) {

        final Set<ISPO> set = new LinkedHashSet<ISPO>();

        final BOpStats stats = context.getStats();

        final ICloseableIterator<IBindingSet[]> itr = context.getSource();

        try {

            int nchunks = 0, nelements = 0;

            while (itr.hasNext()) {

                final IBindingSet[] a = itr.next();

                for (IBindingSet bset : a) {

                    set.add(getSPO(bset, bindsC, StatementEnum.Explicit));

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
     * Return an {@link ISPO} constructed from the source solution.
     * 
     * @param bset
     *            The source solution.
     * @param bindsC
     *            if the context position should be bound.
     * @param type
     *            The {@link StatementEnum}.
     *            
     * @return The {@link ISPO}.
     */
    static protected ISPO getSPO(final IBindingSet bset,
            final boolean bindsC, final StatementEnum type) {

        return new SPO(//
                getIV(s, bset, false),//
                getIV(p, bset, false),//
                getIV(o, bset, false),//
                (bindsC ? getIV(c, bset, false) : null),//
                type);

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
    static protected IV getIV(final IVariable<?> var, final IBindingSet bset,
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

}
