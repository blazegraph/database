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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.ITimestampAnnotations;
import com.bigdata.bop.PipelineOp;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;

/**
 * Commit the operation. If the operation is isolated by a transaction, then the
 * transaction is committed. Otherwise an unisolated commit is performed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public final class CommitOp extends PipelineOp {

    public interface Annotations extends PipelineOp.Annotations,
            ITimestampAnnotations {

    }

    public CommitOp(final BOp[] args, final Map<String, Object> annotations) {

        super(args, annotations);

        getRequiredProperty(Annotations.TIMESTAMP);

        if (getProperty(Annotations.PIPELINED, Annotations.DEFAULT_PIPELINED)) {
            
            /*
             * Pipelined evaluation is not permitted for COMMIT. You need to
             * know that everything upstream was successfully evaluated before
             * you can invoke commit(). "At once" evaluation semantics given us
             * that guarantee.
             */

            throw new IllegalArgumentException(Annotations.PIPELINED);
            
        }
        
    }

    public CommitOp(final CommitOp op) {
 
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

        private final long timestamp;

        private final Journal store;

        public ChunkTask(final BOpContext<IBindingSet> context,
                final CommitOp op) {

            timestamp = (Long) op.getRequiredProperty(Annotations.TIMESTAMP);

            if (TimestampUtility.isReadOnly(timestamp)) {

                /*
                 * Must be read-write tx or unisolated operation.
                 */

                throw new UnsupportedOperationException();

            }

            /*
             * TODO Could allow on a temporary store also, but it is a NOP there
             * and does not need to be in the query plan.
             */

            store = (Journal) context.getIndexManager();

        }

        @Override
        public Void call() throws Exception {

            final long commitTime;

            if (TimestampUtility.isUnisolated(timestamp)) {

                /*
                 * Unisolated commit.
                 */

                commitTime = store.commit();

            } else {

                /*
                 * Commit transaction.
                 */

                commitTime = store.commit(timestamp);

            }

            /*
             * TODO Pass back the commit time and/or #of commits via mutation
             * stats.
             */

            // done.
            return null;

        }

    } // ChunkTask

} // CommitOp
