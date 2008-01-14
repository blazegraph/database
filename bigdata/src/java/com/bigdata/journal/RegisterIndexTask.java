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

import java.util.UUID;

import com.bigdata.btree.IIndex;

/**
 * Register a named index (unisolated write operation).
 * <p>
 * Note: The return value of {@link #doTask()} is the {@link UUID} of the named
 * index. You can test this value to determine whether the index was created
 * based on the supplied {@link IIndex} object or whether the index was
 * pre-existing at the time that this operation was executed.
 * <p>
 * Note: the registered index will NOT be visible to unisolated readers or
 * isolated operations until the next commit. However, unisolated writers that
 * execute after the index has been registered will be able to see the
 * registered index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RegisterIndexTask extends AbstractTask {

    final private IIndex btree;
    
    /**
     * @param journal
     * @param name
     * @param ndx
     *            The index object. Use
     * 
     * <pre>
     * UUID indexUUID = UUID.randomUUID();
     * 
     * new UnisolatedBTree(journal, indexUUID)
     * </pre>
     * 
     * to register a new index that supports isolation.
     */
    public RegisterIndexTask(ConcurrentJournal journal, String name, IIndex ndx) {

        super(journal, ITx.UNISOLATED, false/*readOnly*/, name);
        
        if (ndx == null)
            throw new NullPointerException();
        
        this.btree = ndx;
        
    }

    /**
     * Create the named index if it does not exist.
     * 
     * @return The {@link UUID} of the named index.
     */
    protected Object doTask() throws Exception {

        String name = getOnlyResource();

        try {

            // register the index.
            journal.registerIndex(name, btree);

        } catch (IndexExistsException ex) {

            IIndex ndx = journal.getIndex(name);

            UUID indexUUID = ndx.getIndexUUID();

            log.info("Index exists: name=" + name + ", indexUUID=" + indexUUID);

            return indexUUID;

        }

        log.info("Registered index: name=" + name + ", class="
                + btree.getClass() + ", indexUUID=" + btree.getIndexUUID());

        return btree.getIndexUUID();
        
    }

}