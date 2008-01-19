/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jan 16, 2008
 */
package com.bigdata.btree;

/**
 * A factory for {@link IIndexProcedure}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IIndexProcedureConstructor {

    /**
     * 
     * @param n
     *            The #of tuples on which the procedure will operate.
     * @param offset
     *            The offset of the 1st tuple into <i>keys[]</i> and
     *            <i>vals[]</i>.
     * @param keys
     *            The keys.
     * @param vals
     *            The values.
     * 
     * @return An instance of the procedure.
     * 
     * @todo we will need a different method signature to support
     *       hash-partitioned (vs range partitioned) indices.
     */
    public IIndexProcedure newInstance(int n, int offset, byte[][] keys, byte[][] vals);
    
}
