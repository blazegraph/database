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
 * Created on Feb 12, 2012
 */

package com.bigdata.bop.engine;

/**
 * A message sent to the {@link IQueryClient} when an operator is done executing
 * for some chunk of inputs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IHaltOpMessage extends IOpLifeCycleMessage {

    /**
     * The cause and <code>null</code> if the operator halted normally.
     */
    Throwable getCause();

//    /**
//     * The operator identifier for the primary sink -or- <code>null</code> if
//     * there is no primary sink (for example, if this is the last operator in
//     * the pipeline).
//     */
//    Integer getSinkId();

    /**
     * The number of the {@link IChunkMessage}s that were output for the primary
     * sink. (This information is used for the atomic termination decision.)
     * <p>
     * For a given downstream operator this is ONE (1) for scale-up. For
     * scale-out, this is one per index partition over which the intermediate
     * results were mapped.
     */
    int getSinkMessagesOut();

//    /**
//     * The operator identifier for the alternative sink -or- <code>null</code>
//     * if there is no alternative sink.
//     */
//    Integer getAltSinkId();

    /**
     * The number of the {@link IChunkMessage}s that were output for the
     * alternative sink. (This information is used for the atomic termination
     * decision.)
     * <p>
     * For a given downstream operator this is ONE (1) for scale-up. For
     * scale-out, this is one per index partition over which the intermediate
     * results were mapped. It is zero if there was no alternative sink for the
     * operator.
     */
    int getAltSinkMessagesOut();

    /**
     * The statistics for the execution of the bop against the partition on the
     * service.
     */
    BOpStats getStats();

}
