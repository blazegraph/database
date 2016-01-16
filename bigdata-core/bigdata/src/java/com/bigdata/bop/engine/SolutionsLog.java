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
 * Created on Sep 29, 2011
 */

package com.bigdata.bop.engine;

import java.util.Iterator;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.util.Bytes;

/**
 * Class provides a logger which may be used for observing all solutions flowing
 * into each operator in the query plan and the final solutions flowing into the
 * query buffer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SolutionsLog {

    /**
     * Logging for this class should be performed when this logger has a detail
     * level of at least INFO.
     */
    public static final Logger solutionsLog = Logger.getLogger(SolutionsLog.class);

    public static final boolean INFO = solutionsLog.isInfoEnabled();
    
    /**
     * A single buffer is reused to keep down the heap churn.
     */
    final private static StringBuilder sb = new StringBuilder(
            Bytes.kilobyte32 * 4);

    private static boolean first = true;
    
    static private void prefix(final UUID queryId, final BOp bop, final int bopId,
            final int partitionId, final int chunkSize) {

        if (first) {

            // If first time, then write out the header.
            
            solutionsLog.info("QueryUUID\tbop\tbopId\tpartitionId\tchunkSize\n");
            
            first = false;
            
        }

        sb.setLength(0);

        sb.append(queryId);
        sb.append('\t');
        sb.append(bop == null ? "N/A" : bop.getClass().getSimpleName());
//        sb.append('\t');
//        sb.append(getPredSummary(bop));
        sb.append('\t');
        sb.append(bopId);
        sb.append('\t');
        sb.append(partitionId);
        sb.append('\t');
        sb.append(chunkSize);
        sb.append('\t');

    }

    /**
     * If the bop is a join then return a summary of the predicate.
     * 
     * @param bop
     *            The bop.
     *            
     * @return The predicate summary iff the bop is a join.
     */
    static private String getPredSummary(final BOp bop) {
        final IPredicate<?> pred = (IPredicate<?>) bop
                .getProperty(PipelineJoin.Annotations.PREDICATE);
        if (pred == null)
            return "";
        final StringBuilder sb = new StringBuilder();
        final Integer predId = pred == null ? null : (Integer) pred
                .getProperty(BOp.Annotations.BOP_ID);
        sb.append(pred.getClass().getSimpleName());
        sb.append("[" + predId + "](");
        final Iterator<BOp> itr = pred.argIterator();
        boolean first = true;
        while (itr.hasNext()) {
            if (first) {
                first = false;
            } else
                sb.append(", ");
            final IVariableOrConstant<?> x = (IVariableOrConstant<?>) itr
                    .next();
            if (x.isVar()) {
                sb.append("?");
                sb.append(x.getName());
            } else {
                sb.append(x.get());
                // sb.append(((IV)x.get()).getValue());
            }
        }
        sb.append(")");
        return sb.toString();
    }
    
//    /**
//     * 
//     * TODO This can not be used because the {@link LocalChunkMessage} wraps an
//     * iterator rather than an {@link IBindingSet}[]. Visiting the iterator on
//     * that class therefore *drains* the chunk and it can not be "rewound". We
//     * should entirely get rid of the {@link IAsynchronousIterator} in {@link
//     * IChunkMessage}s. It was original developed to provide chunked
//     * transmission of solutions between asynchronous processes, but that is now
//     * handled by the {@link QueryEngine} and the {@link PipelineOp}s and the
//     * {@link IChunkMessage} could both operate in terms of a simpler
//     * abstraction. (It can not be as simple as a single embedded {@link
//     * IBindingSet}[] because the data should not be fully materialized onto the
//     * JVM heap for an {@link NIOChunkMessage}, but it could be an
//     * IClosableIterator visiting either IBindingSets or IBindingSet chunks (if
//     * two levels of chunking makes sense for an {@link NIOChunkMessage}, which
//     * I doubt)).
//     */
//    synchronized static void log(final IChunkMessage<IBindingSet> msg) {
//
//        prefix(msg.getQueryId(), msg.getBOpId(), msg.getPartitionId());
//        
//        final int headerLen = sb.length();
//
//        final IAsynchronousIterator<IBindingSet[]> itr = msg.getChunkAccessor()
//                .iterator();
//
//        try {
//
//            while (itr.hasNext()) {
//
//                final IBindingSet[] a = itr.next();
//
//                for (IBindingSet bset : a) {
//
//                    sb.append(bset.toString());
//
//                    sb.append('\n');
//
//                    solutionsLog.info(sb);
//
//                    sb.setLength(headerLen);
//
//                }
//
//            }
//
//        } finally {
//
//            itr.close();
//
//        }
//
//    }

    public synchronized static void log(final UUID queryId, final BOp bop,
            final int bopId, final int partitionId, final IBindingSet[] a) {

        prefix(queryId, bop, bopId, partitionId, a.length);

        final int headerLen = sb.length();

        for (IBindingSet bset : a) {

            if (bset == null)
                sb.append("NA");
            else
                sb.append(bset.toString());

            sb.append('\n');

            solutionsLog.info(sb);

            sb.setLength(headerLen);

        }
        
    }

}
