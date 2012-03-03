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
 * Created on Mar 3, 2012
 */

package com.bigdata.rdf.sparql.ast.service;

/**
 * Configurable options for a remote service end point.
 * 
 * @see RemoteServiceCallImpl
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RemoteServiceOptions implements IServiceOptions {

    private boolean isBindingsClause = true;

    public RemoteServiceOptions() {

    }

    @Override
    final public boolean isBigdataService() {
        return false;
    }

    @Override
    final public boolean isRemoteService() {
        return true;
    }

    @Override
    public boolean isBindingsClause() {
        return isBindingsClause;
    }

    public void setBindingsClause(final boolean isBindingsClause) {

        this.isBindingsClause = isBindingsClause;

    }

}
