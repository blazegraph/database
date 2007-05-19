/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Apr 30, 2007
 */

package com.bigdata.service;

import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.Journal;
import com.bigdata.scaleup.PartitionMetadata;

/**
 * A specialization of {@link Journal} that maintains some additional state for
 * a {@link DataService} (the partitions mapped onto that {@link DataService}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated The additional state is maintained by each
 *             {@link UnisolatedBTreePartition}
 */
public class DataServiceJournal extends Journal {

    /**
     * The root slot in which the address of the btree containing the mapped
     * partitions definitions is stored.
     */
    public static transient final int ROOT_PARTITIONS = 1;
    
    /**
     * The btree containing the mapped partitions definitions.
     */
    private BTree partitionsBTree = null;

    /**
     * The index containing the metadata for the partitions mapped onto a
     * {@link DataService}. The keys are formed as:
     * 
     * <pre>
     *      indexUUID:UUID, partitionId:int32 
     * </pre>
     * 
     * The values are serialized {@link PartitionMetadata} objects.
     */
    public BTree getPartitionsBTree() {
        
        /*
         * Note: Java assigns a null to this field _after_ the constructor is
         * done, so that means that the valid reference to the btree setup by
         * {@link #setCommitter(int, com.bigdata.journal.ICommitter)} is being
         * overwritten as soon as the constructor is finished. For that reason
         * we do not assert that the field is non-null and silently resolve it
         * against the store if it is null.
         */
//        assert partitionsBTree != null;        
        if(partitionsBTree == null ) {

            setupPartitionsBTree(getRootAddr(ROOT_PARTITIONS));

        }

        assert partitionsBTree != null;
        
        return partitionsBTree;
        
    }
    
    /**
     * Extends the default behavior to register the {@link #ROOT_PARTITIONS}
     * index.
     */
    public void setupCommitters() {

        super.setupCommitters();

        setupPartitionsBTree(getRootAddr(ROOT_PARTITIONS));
        
    }
    
    /**
     * Extends the default behavior to discard hard references for the
     * {@link #partitionsBTree}.
     */
    public void discardCommitters() {
        
        super.discardCommitters();

        partitionsBTree = null;
        
    }

    /**
     * Setup the btree that resolves index partitions that have been assigned to
     * a {@link DataService}.
     * 
     * @param addr
     *            The root address of the btree -or- 0L iff the btree has not
     *            been defined yet.
     */
    private void setupPartitionsBTree(long addr) {

        assert partitionsBTree == null;

        if (addr == 0L) {

            /*
             * The btree has either never been created or if it had been created
             * then the store was never committed and the btree had since been
             * discarded. In any case we create a new btree now.
             */

            partitionsBTree = new UnisolatedBTree(this, UUID.randomUUID());

        } else {

            /*
             * Reload the btree from its root address.
             */

            partitionsBTree = (UnisolatedBTree) BTree.load(this, addr);

        }

        // register for commit notices.
        setCommitter(ROOT_PARTITIONS, partitionsBTree);

    }

    /**
     * @param properties
     */
    public DataServiceJournal(Properties properties) {

        super(properties);
        
    }

}
