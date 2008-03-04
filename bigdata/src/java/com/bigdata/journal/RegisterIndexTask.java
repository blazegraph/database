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

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.service.IDataService;

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

    final private IndexMetadata metadata;
    
    /**
     * @param journal
     * @param name
     *            The name under which to register the index.
     * @param metadata
     *            The index metadata.
     *            <p>
     *            The {@link LocalPartitionMetadata#getResources()} property on
     *            the {@link IndexMetadata#getPartitionMetadata()} SHOULD NOT be
     *            set. The correct {@link IResourceMetadata}[] will be assigned
     *            when the index is registered on the {@link IDataService}.
     */
    public RegisterIndexTask(IConcurrencyManager concurrencyManager,
            String name, IndexMetadata metadata) {

        super(concurrencyManager, ITx.UNISOLATED, name);

        if (metadata == null)
            throw new NullPointerException();
        
        this.metadata = metadata;
        
    }

    /**
     * Create the named index if it does not exist.
     * 
     * @return The {@link UUID} of the named index.
     */
    protected Object doTask() throws Exception {

        String name = getOnlyResource();

        IIndex ndx = getJournal().getIndex(name);
            
        if (ndx != null) {

            final UUID indexUUID = ndx.getIndexMetadata().getIndexUUID();

            log.info("Index exists: name=" + name + ", indexUUID=" + indexUUID);

            return indexUUID;

        }

        /*
         * Verify some aspects of the partition metadata.
         */
        final LocalPartitionMetadata pmd = metadata.getPartitionMetadata();
        if (pmd != null) {

            if (pmd.getResources() == null) {

                /*
                 * A [null] for the resources field is a specific indication
                 * that we need to specify the resource metadata for the live
                 * journal at the time that the index partition is registered.
                 * This indicator is used when the metadata service registers an
                 * index partition remotely on a data service since it does not
                 * (and can not) have access to the resource metadata for the
                 * live journal as of the time that the index partition actually
                 * gets registered on the data service.
                 * 
                 * The index partition split and join tasks do not have this
                 * problem since they are run locally. However, an index
                 * partition move operation also needs to do this.
                 */

                metadata.setPartitionMetadata(new LocalPartitionMetadata(//
                        pmd.getPartitionId(),//
                        pmd.getLeftSeparatorKey(),//
                        pmd.getRightSeparatorKey(),//
                        new IResourceMetadata[] {//
                            // The live journal.
                            getJournal().getResourceMetadata() //
                        }));

            } else {

                if (pmd.getResources().length == 0) {

                    throw new RuntimeException(
                            "Missing resource description: name=" + name
                                    + ", pmd=" + pmd);

                }

                if (!pmd.getResources()[0].isJournal()) {

                    throw new RuntimeException(
                            "Expecting resources[0] to be journal: name="
                                    + name + ", pmd=" + pmd);

                }

                if (!pmd.getResources()[0].getUUID().equals(
                        getJournal().getRootBlockView().getUUID())) {

                    throw new RuntimeException(
                            "Expecting resources[0] to be this journal but has wrong UUID: name="
                                    + name + ", pmd=" + pmd);

                }

            }
            
        }
        
        // create index instance.
        final BTree btree = BTree.create(getJournal(), metadata);

        // register the index.
        getJournal().registerIndex(name, btree);

        final UUID indexUUID = btree.getIndexMetadata().getIndexUUID();

        log.info("Registered index: name=" + name + ", class="
                + btree.getClass() + ", indexUUID=" + indexUUID);

        return indexUUID;
        
    }

}
