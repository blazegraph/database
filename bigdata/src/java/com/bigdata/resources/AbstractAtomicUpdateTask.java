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
 * Created on Dec 2, 2008
 */

package com.bigdata.resources;

import java.util.UUID;

import com.bigdata.btree.BTree;

/**
 * Abstract base class for tasks responsible for the atomic update of the view
 * of an index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractAtomicUpdateTask<T> extends
        AbstractResourceManagerTask<T> {

    /**
     * 
     * @param resourceManager
     * @param timestamp
     * @param resource
     */
    public AbstractAtomicUpdateTask(
            final ResourceManager resourceManager,
            final long timestamp,
            final String[] resource
            ) {
     
        super(resourceManager, timestamp, resource);

    }

    /**
     * @param resourceManager
     * @param timestamp
     * @param resource
     * @param indexUUID
     *            The {@link UUID} associated with the scale-out index.
     */
    public AbstractAtomicUpdateTask(
            final ResourceManager resourceManager,
            final long timestamp,
            final String resource) {

        super(resourceManager, timestamp, resource);
    
    }

    /**
     * Verifies that the view is a view of the expected scale-out index.
     * <p>
     * This is used to detect drop/add sequences where a scale-out index exists
     * with the same name but a different index {@link UUID}. Atomic update
     * tasks check for this and disallow the update in order to prevent
     * incorporation of the old index state into the new index.
     * 
     * @param expectedIndexUUID
     *            The expected index UUID.
     * @param view
     *            {@link BTree} whose view will be re-defined (must be part of
     *            the same scale-out index).
     * 
     * @throws IllegalStateException
     *             if the index {@link UUID} does not agree.
     * 
     * FIXME make sure that everyone tests this.
     */
    protected void assertSameIndex(final UUID expectedIndexUUID,
            final BTree view) {

        if (!expectedIndexUUID.equals(view.getIndexMetadata().getIndexUUID())) {

            /*
             * This can happen if you drop/add a scale-out index during overflow
             * processing. The new index will have a new UUID. We check this to
             * prevent merging in data from the old index.
             */

            throw new RuntimeException(
                    "Different UUID: presuming drop/add of index: name="
                            + getOnlyResource());

        }

    }

}
