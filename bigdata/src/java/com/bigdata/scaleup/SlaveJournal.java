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
package com.bigdata.scaleup;

import java.io.File;
import java.util.Properties;

import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Name2Addr;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.objndx.IEntryIterator;
import com.bigdata.objndx.IIndex;
import com.bigdata.objndx.IndexSegment;
import com.bigdata.rawstore.Addr;

/**
 * Class delegates the {@link #overflow()} event to a master
 * {@link IJournal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
     * (unicode strings). The values are the last known {@link Addr address} of
     * the {@link MetadataIndex} for the named btree.
     */
    protected Name2MetadataAddr name2MetadataAddr;

    private final PartitionedJournal master;
    
    protected PartitionedJournal getMaster() {

        return master;
        
    }
    
    public SlaveJournal(PartitionedJournal master,Properties properties) {
        
        super(properties);
        
        if (master == null)
            throw new IllegalArgumentException("master");
        
        this.master = master;

    }

    /**
     * The overflow event is delegated to the master.
     */
    public void overflow() {
    
        master.overflow();
        
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

            name2MetadataAddr = (Name2MetadataAddr)BTreeMetadata
                    .load(this, addr);

        }

        // register for commit notices.
        setCommitter(ROOT_NAME_2_METADATA_ADDR, name2MetadataAddr);

    }

    /**
     * Registers and returns a {@link PartitionedIndex} under the given name and
     * assigns an {@link UnisolatedBTree} to absorb writes for that
     * {@link PartitionedIndex}. The resulting index will support transactional
     * isolation.
     * <p>
     * A {@link MetadataIndex} is also registered under the given name and an
     * initial partition for that index is created using the separator key
     * <code>new byte[]{}</code>. The partition will initially consist of
     * zero {@link IndexSegment}s.
     * <p>
     * Note: The returned object is invalid once the {@link PartitionedJournal}
     * {@link PartitionedJournal#overflow()}s.
     * <p>
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     */
    public IIndex registerIndex(String name) {
        
        return registerIndex(name, new UnisolatedBTree(this));
        
    }
    
    /**
     * Registers and returns a {@link PartitionedIndex} under the given name and
     * assigns the supplied {@link IIndex} to absorb writes for that
     * {@link PartitionedIndex}.
     * <p>
     * A {@link MetadataIndex} is also registered under the given name and an
     * initial partition for that index is created using the separator key
     * <code>new byte[]{}</code>. The partition will initially consist of
     * zero {@link IndexSegment}s.
     * <p>
     * Note: The returned object is invalid once the {@link PartitionedJournal}
     * {@link PartitionedJournal#overflow()}s.
     * <p>
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     * 
     * @todo use a prototype model so that the registered btree type is
     *       preserved? (Only the metadata extensions are preserved right now).
     *       One way to do this is by putting the constructor on the metadata
     *       object. Another is to make the btree Serializable and then just
     *       declare everything else as transient.
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
        
        MetadataIndex mdi = new MetadataIndex(this,
                BTree.DEFAULT_BRANCHING_FACTOR, name);
        
        // create the initial partition which can accept any key.
        mdi.put(new byte[]{}, new PartitionMetadata(0));
        
        // add to the persistent name map.
        name2MetadataAddr.add(name, mdi);

        /*
         * Now register the partitioned index on the super class.
         */
        return super.registerIndex(name, new PartitionedIndex(
                (BTree) btree, mdi));
        
    }
    
    public IIndex getIndex(String name) {

        IIndex index = super.getIndex(name);
        
        if(index instanceof BTree) {
            
            index = new PartitionedIndex((BTree)index,getMetadataIndex(name));
            
        }

        return index;
        
    }

    /**
     * Return the {@link MetadataIndex} for the named {@link PartitionedIndex}.
     * This object is used to maintain the definitions of the partitions for
     * that index, including where each file is located that contains live data
     * for the index.
     */
    public MetadataIndex getMetadataIndex(String name) {

        if(name==null) throw new IllegalArgumentException();
        
        return (MetadataIndex) name2MetadataAddr.get(name);

    }

    /**
     * FIXME write tests. use to clean up the test suites. note that we can
     * not drop files until we have done an atomic commit. also note that
     * restart-safe removal of files is not going to happen without some
     * additional sophistication.  one solution is to periodically compare
     * the named indices and the segments directory, either deleting or
     * flagging for an operator those things which do not belong.
     */
    public void dropIndex(String name) {
        
        PartitionedIndex ndx = (PartitionedIndex) getIndex(name);
        
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

            final PartitionMetadata pmd = (PartitionMetadata) itr.next();

            for (int i = 0; i < pmd.segs.length; i++) {

                SegmentMetadata smd = pmd.segs[i];

                File file = new File(smd.filename);

                if (file.exists() && !file.delete()) {

                    log.warn("Could not remove file: "
                            + file.getAbsolutePath());

                }

            }

            File dir = master.getPartitionDirectory(name, pmd.partId);

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