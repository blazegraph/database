/**

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
package com.bigdata.scaleup;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexSegment;
import com.bigdata.io.SerializerUtil;
import com.bigdata.isolation.IIsolatableIndex;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Name2Addr;

/**
 * Class delegates the {@link #overflow()} event to a {@link MasterJournal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME refactor the metadata index so that it may be run as an embedded
 * process or remote service.
 */
public class SlaveJournal extends Journal {
    
    /**
     * The index of the root slot whose value is the address of the persistent
     * {@link Name2Addr} mapping names to {@link MetadataIndex}s registered
     * for the store.
     */
    public static transient final int ROOT_NAME_2_METADATA_ADDR = 2;
    
    /**
     * BTree mapping btree names to the last metadata record committed for the
     * {@link MetadataIndex} for the named btree. The keys are index names
     * (unicode strings). The values are the last known address of the
     * {@link MetadataIndex} for the named btree.
     */
    protected Name2MetadataAddr name2MetadataAddr;

    private final MasterJournal master;

    protected MasterJournal getMaster() {

        return master;
        
    }
    
    public SlaveJournal(MasterJournal master,Properties properties) {
        
        super(properties);
        
        if (master == null)
            throw new IllegalArgumentException("master");
        
        this.master = master;

    }
    
    /**
     * The overflow event is delegated to the master.
     */
    public boolean overflow() {
    
        // handles event reporting.
        super.overflow();
        
        return master.overflow();
        
    }

    public void discardCommitters() {

        super.discardCommitters();
        
        // discard.
        name2MetadataAddr = null;
        
    }
    
    public void setupCommitters() {

        super.setupCommitters();
        
        setupName2MetadataAddrBTree();

    }
    
    /**
     * Setup the btree that resolved the {@link MetadataIndex} for named
     * indices.
     */
    private void setupName2MetadataAddrBTree() {

        assert name2MetadataAddr == null;
        
        // the root address of the btree.
        long addr = getRootAddr(ROOT_NAME_2_METADATA_ADDR);

        if (addr == 0L) {

            /*
             * The btree has either never been created or if it had been created
             * then the store was never committed and the btree had since been
             * discarded.  In any case we create a new btree now.
             */

            // create btree mapping names to addresses.
            name2MetadataAddr= new Name2MetadataAddr(this);

        } else {

            /*
             * Reload the btree from its root address.
             */

            name2MetadataAddr = (Name2MetadataAddr)BTree
                    .load(this, addr);

        }

        // register for commit notices.
        setCommitter(ROOT_NAME_2_METADATA_ADDR, name2MetadataAddr);

    }

    /**
     * Registers and returns a {@link PartitionedIndexView} under the given name and
     * assigns an {@link UnisolatedBTree} to absorb writes for that
     * {@link PartitionedIndexView}. The resulting index will support transactional
     * isolation.
     * <p>
     * A {@link MetadataIndex} is also registered under the given name and an
     * initial partition for that index is created using the separator key
     * <code>new byte[]{}</code>. The partition will initially consist of
     * zero {@link IndexSegment}s.
     * <p>
     * Note: The returned object is invalid once the {@link MasterJournal}
     * {@link MasterJournal#overflow()}s.
     * <p>
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     */
    public IIndex registerIndex(String name) {
        
        return registerIndex(name, new UnisolatedBTree(this, UUID.randomUUID()));
        
    }
    
    /**
     * Registers and returns a {@link PartitionedIndexView} under the given name
     * and assigns the supplied {@link IIndex} to absorb writes for that
     * {@link PartitionedIndexView}.
     * <p>
     * A {@link MetadataIndex} is also registered under the given name and an
     * initial partition for that index is created using the separator key
     * <code>new byte[]{}</code>. The partition will initially consist of
     * zero {@link IndexSegment}s.
     * <p>
     * Note: The returned object is invalid once the {@link MasterJournal}
     * {@link MasterJournal#overflow()}s.
     * <p>
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     */
    public IIndex registerIndex(String name, IIndex btree) {

        if( super.getIndex(name) != null ) {
            
            throw new IllegalStateException("Already registered: name="
                    + name);
            
        }

        // make sure there is no metadata index for that btree.
        if(getMetadataIndex(name) != null) {
            
            throw new AssertionError();
            
        }
        
        if (!(btree instanceof BTree)) {
        
            throw new IllegalArgumentException("Index must extend: "
                    + BTree.class);
            
        }

        /*
         * Note: there are two UUIDs here - the UUID for the metadata index
         * describing the partitions of the named scale-out index and the UUID
         * of the named scale-out index. The metadata index UUID MUST be used by
         * all B+Tree objects having data for the metadata index (its mutable
         * btrees on journals and its index segments) while the managed named
         * index UUID MUST be used by all B+Tree objects having data for the
         * named index (its mutable btrees on journals and its index segments).
         */
        
        final UUID metadataIndexUUID = UUID.randomUUID();
        
        final UUID managedIndexUUID = btree.getIndexUUID();
        
        MetadataIndex mdi = new MetadataIndex(this, metadataIndexUUID,
                managedIndexUUID, name);
        
        /*
         * Create the initial partition which can accept any key.
         * 
         * @todo specify the DataSerivce(s) that will accept writes for this
         * index partition. This should be done as part of refactoring the
         * metadata index into a first level service.
         * 
         * @todo specify the journal as the initial resource.
         */
        
        final UUID[] dataServices = new UUID[]{};
        
        mdi.put(new byte[]{}, new PartitionMetadata(0, dataServices ));
        
        // add to the persistent name map.
        name2MetadataAddr.registerIndex(name, mdi);

        /*
         * Now register the view on the super class. The view will delegate
         * writes and the commit protocol to the btree specified by the caller,
         * but will support index partitions and will read from a fused view of
         * the resources for each index partition.
         */
        return super.registerIndex(name, getView((BTree) btree, mdi));
        
    }
    
    /**
     * Returns a {@link PartitionedIndexView} or
     * {@link IsolatablePartitionedIndexView} depending on whether or not the
     * named index supports isolation.
     */
    public IIndex getIndex(String name) {

        /*
         * Obtain the persistence capable index. If this is a cache hit, then it
         * will be the view. Otherwise this will be either a BTree or an
         * UnisolatedBTree that was just loaded from the store and we will have
         * to wrap it up as a view.
         */
        IIndex index = super.getIndex(name);

        if (index instanceof BTree) {

            /*
             * Wrap up the mutable B+-Tree responsible for absorbing writes as
             * a view.
             */
            
            MetadataIndex mdi = getMetadataIndex(name);
            
            /*
             * Choose the type of view based on whether or not the registered index
             * supports isolation.
             */
            index = getView((BTree)index,mdi);

        }

        return index;

    }

    /**
     * Choose the type of view based on whether or not the registered index
     * supports isolation.
     */
    private PartitionedIndexView getView(BTree btree, MetadataIndex mdi) {
        
        if (btree instanceof IIsolatableIndex) {
            
            return new IsolatablePartitionedIndexView((UnisolatedBTree) btree,
                    mdi);
        } else {
            
            return new PartitionedIndexView((BTree) btree, mdi);
    
        }

    }
    
    /**
     * Return the {@link MetadataIndex} for the named
     * {@link PartitionedIndexView}. This object is used to maintain the
     * definitions of the partitions for that index, including where each
     * resource is located that contains data for each partition of the index.
     */
    public MetadataIndex getMetadataIndex(String name) {

        if(name==null) throw new IllegalArgumentException();
        
        return (MetadataIndex) name2MetadataAddr.get(name);

    }

    /**
     * FIXME write tests. use to clean up the test suites. note that we can not
     * drop files until we have done an atomic commit. also note that
     * restart-safe removal of files is not going to happen without some
     * additional sophistication, eg, placing a task to schedule deletion of
     * resources onto the restart safe schedule. one solution is to periodically
     * compare the named indices and the segments directory, either deleting or
     * flagging for an operator those things which do not belong.
     */
    public void dropIndex(String name) {
        
        PartitionedIndexView ndx = (PartitionedIndexView) getIndex(name);
        
        if (ndx == null)
            throw new IllegalArgumentException("Not registered: " + name);
        
        // close all views, including any index segments in used by a view.
        ndx.closeViews();
        
        MetadataIndex mdi = getMetadataIndex(name);
        
        name2MetadataAddr.dropIndex(name);
        
        super.dropIndex(name);
        
        /*
         * atomic commit makes sure that the index is dropped before we
         * start deleting resources in the file system.
         */
        commit();
        
        IEntryIterator itr = mdi.entryIterator();

        while (itr.hasNext()) {

            final PartitionMetadata pmd = (PartitionMetadata) SerializerUtil
                    .deserialize((byte[]) itr.next());

            final IResourceMetadata[] resources = pmd.getResources();
            
            for (int i = 0; i < resources.length; i++) {

                IResourceMetadata rmd = resources[i];

                if (rmd.isIndexSegment()) {

                    File file = new File(rmd.getFile());

                    if (file.exists() && !file.delete()) {

                        log.warn("Could not remove file: "
                                + file.getAbsolutePath());

                    }

                }

            }

            File dir = master.getPartitionDirectory(name, pmd.getPartitionId());

            if (!dir.delete()) {

                log.warn("Could not delete directory: " + dir);

            }

        } // next partition.

        /*
         * all segments in all partitions have been removed (or at least
         * we have tried to remove them).
         */

        File dir = master.getIndexDirectory(name);

        if (!dir.delete()) {

            log.warn("Could not delete directory: " + dir);

        }

    }

}
