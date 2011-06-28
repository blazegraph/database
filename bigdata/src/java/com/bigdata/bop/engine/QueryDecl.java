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
 * Created on Sep 9, 2010
 */

package com.bigdata.bop.engine;

import java.io.Serializable;
import java.util.UUID;

import com.bigdata.bop.PipelineOp;

/**
 * Default implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QueryDecl implements IQueryDecl, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final UUID queryId;

    private final IQueryClient clientProxy;

    private final PipelineOp query;

    public QueryDecl(final IQueryClient clientProxy, final UUID queryId,
            final PipelineOp query) {

        if (clientProxy == null)
            throw new IllegalArgumentException();

        if (queryId == null)
            throw new IllegalArgumentException();

        if (query == null)
            throw new IllegalArgumentException();

        this.clientProxy = clientProxy;

        this.queryId = queryId;

        this.query = query;

    }

    public PipelineOp getQuery() {
        return query;
    }

    public IQueryClient getQueryController() {
        return clientProxy;
    }

    public UUID getQueryId() {
        return queryId;
    }

}
