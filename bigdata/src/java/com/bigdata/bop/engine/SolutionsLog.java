/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Sep 29, 2011
 */

package com.bigdata.bop.engine;

import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rawstore.Bytes;

/**
 * Class provides a logger which may be used for observing all solutions flowing
 * into each operator in the query plan and the final solutions flowing into the
 * query buffer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class SolutionsLog {

    static final Logger solutionsLog = Logger.getLogger(SolutionsLog.class);

    /**
     * A single buffer is reused to keep down the heap churn.
     */
    final private static StringBuilder sb = new StringBuilder(
            Bytes.kilobyte32 * 4);

    private static boolean first = true;
    
    static private void prefix(final UUID queryId, final int bopId,
            final int partitionId, final int chunkSize) {

        if (first) {

            // If first time, then write out the header.
            
            solutionsLog.info("QueryUUID\tbopId\tpartitionId\tchunkSize\n");
            
            first = false;
            
        }

        sb.setLength(0);

        sb.append(queryId);
        sb.append('\t');
        sb.append(bopId);
        sb.append('\t');
        sb.append(partitionId);
        sb.append('\t');
        sb.append(chunkSize);
        sb.append('\t');

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

    synchronized static void log(final UUID queryId, final int bopId,
            final int partitionId, final IBindingSet[] a) {

        prefix(queryId, bopId, partitionId, a.length);

        final int headerLen = sb.length();

        for (IBindingSet bset : a) {

            sb.append(bset.toString());

            sb.append('\n');

            solutionsLog.info(sb);

            sb.setLength(headerLen);

        }
        
    }

}
