/*

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
 * Created on May 29, 2008
 */

package com.bigdata.btree.proc;

/**
 * Base class has some utility methods.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class AbstractIndexProcedure<T> implements IIndexProcedure<T> {

    /**
    * Note: Serialization is not used for durable data, just RMI. However,
    * declaring this field MAY break serialization of {@link IIndexProcedure}
    * instances. Those instances are only used in the scale-out architecture. If
    * there is a problem, you can upgrade the server instances as well as the
    * clients. Note that the failure to specify a specific serialVersionUID
    * means that the actual value was being computed at runtime and that
    * round-trip serialization depended on the JVMs computing the same value for
    * this field, so there is not any well known historical value that can be
    * used reliably.
    */
   private static final long serialVersionUID = 1L;

//   /**
//     * Return the thread-local key builder configured for the {@link IIndex}
//     * 
//     * @param ndx
//     *            The index.
//     * 
//     * @return The {@link IKeyBuilder}.
//     * 
//     * @see IndexMetadata#getKeyBuilder()
//     */
//    protected IKeyBuilder getKeyBuilder(final IIndex ndx) {
//
//        return ndx.getIndexMetadata().getKeyBuilder();
//        
////        return ((AbstractJournal) ((AbstractBTree) ndx).getStore())
////                .getKeyBuilder();
//
//    }

}
