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

import java.beans.Statement;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Vectored remove operator for RDF Statements. The solutions flowing through
 * this operator MUST bind the <code>s</code>, <code>p</code>, <code>o</code>,
 * and (depending on the database mode) MAY bind the <code>c</code> variable.
 * Those variables correspond to the Subject, Predicate, Object, and
 * Context/Graph position of an RDF {@link Statement} respectively. On input,
 * the variables must be real {@link IV}s. The {@link IVCache} does NOT need to
 * be set. The output is an empty solution.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public final class RemoveStatementsOp extends AbstractAddRemoveStatementsOp {

    public RemoveStatementsOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

    }

    public RemoveStatementsOp(final RemoveStatementsOp op) {
        super(op);
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(context, this));

    }

    static private class ChunkTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final AbstractTripleStore tripleStore;

        private final boolean sids;

        private final boolean quads;

        public ChunkTask(final BOpContext<IBindingSet> context,
                final RemoveStatementsOp op) {

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

            final boolean bindsC = sids | quads;

            // Build set of distinct ISPOs.
            final Set<ISPO> b = acceptSolutions(context, bindsC);

            // Convert into array.
            final ISPO[] stmts = b.toArray(new ISPO[b.size()]);

//            System.err.println("before:\n" + tripleStore.dumpStore());
            
            // Write on the database.
            final long nmodified = tripleStore.removeStatements(stmts,
                    stmts.length);

            // Increment by the #of statements written.
            context.getStats().mutationCount.add(nmodified);
            
            // done.
            return null;

        }

    } // ChunkTask

} // RemoveStatements
