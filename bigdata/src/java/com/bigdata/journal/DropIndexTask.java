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
package com.bigdata.journal;

/**
 * Drop a named index (unisolated write operation).
 * <p>
 * Note: the dropped index will continue to be visible to unisolated readers or
 * {@link IsolationEnum#ReadCommitted} isolated operations (since they read from
 * the most recent committed state) until the next commit. However, unisolated
 * writers that execute after the index has been dropped will NOT be able to see
 * the index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DropIndexTask extends AbstractTask {

    public DropIndexTask(IConcurrencyManager concurrencyManager, String name) {

        super(concurrencyManager, ITx.UNISOLATED, name);

    }

    /**
     * Drop the named index.
     * 
     * @return A {@link Boolean} value that is <code>true</code> iff the index
     *         was pre-existing at the time that this task executed and
     *         therefore was dropped. <code>false</code> is returned iff the
     *         index did not exist at the time that this task was executed.
     */
    public Object doTask() throws Exception {

        String name = getOnlyResource();

        try {

            getJournal().dropIndex(name);

        } catch (NoSuchIndexException ex) {

            /*
             * The index does not exist.
             */

            log.info("Index does not exist: " + name);

            return Boolean.FALSE;

        }

        return Boolean.TRUE;

    }
    
}
