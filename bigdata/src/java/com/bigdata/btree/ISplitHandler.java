/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Feb 12, 2008
 */

package com.bigdata.btree;

import com.bigdata.sparse.SparseRowStore;

/**
 * An interface that may be used to constrain the allowable separator keys when
 * splitting an index partition into two or more index partitions. This is
 * important for some indices, such as the {@link SparseRowStore}, which need
 * to maintain an guarentee of atomic operations within a key range. The
 * {@link ISplitHandler} allows them to do this by constraining the split points
 * such that the index partition boundaries only fall on acceptable separator
 * keys.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo define interface, integrate into the {@link SparseRowStore}, etc.
 */
public interface ISplitHandler {

}
