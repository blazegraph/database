/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 5, 2010
 */

package com.bigdata.bop.engine;

import com.bigdata.bop.BOp;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.journal.IIndexManager;
import com.bigdata.service.IBigdataFederation;

/**
 * Interface exposing a limited set of the state of an executing query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRunningQuery {
    
    /**
     * The {@link IBigdataFederation} IFF the operator is being evaluated on an
     * {@link IBigdataFederation}. When evaluating operations against an
     * {@link IBigdataFederation}, this reference provides access to the
     * scale-out view of the indices and to other bigdata services.
     */
    IBigdataFederation<?> getFederation();

    /**
     * The <strong>local</strong> {@link IIndexManager}. Query evaluation occurs
     * against the local indices. In scale-out, query evaluation proceeds shard
     * wise and this {@link IIndexManager} MUST be able to read on the
     * {@link ILocalBTreeView}.
     */
    IIndexManager getIndexManager();

//    /**
//     * The timestamp or transaction identifier against which the query is
//     * reading.
//     * 
//     * @deprecated move into the individual operator. See
//     *             {@link BOp.Annotations#TIMESTAMP}
//     */
//    long getReadTimestamp();
//
//    /**
//     * The timestamp or transaction identifier against which the query is
//     * writing.
//     * 
//     * @deprecated moved into the individual operator. See
//     *       {@link BOp.Annotations#TIMESTAMP}
//     */
//    long getWriteTimestamp();

    /**
     * Terminate query evaluation
     */
    void halt();

}
