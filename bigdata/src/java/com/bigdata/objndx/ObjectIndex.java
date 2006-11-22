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
 * Created on Nov 16, 2006
 */

package com.bigdata.objndx;

import java.util.TreeMap;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.IObjectIndex;
import com.bigdata.journal.IRawStore;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.SimpleObjectIndex;

/**
 * FIXME This needs to implement the object index semantics and provide a
 * strongly typed API. We could just hide the btree as a field rather than
 * extending it. The easy way to get the object index semantics is using a one
 * for one substitution of the btree for a {@link TreeMap} in
 * {@link SimpleObjectIndex}. We also need to write and apply a test suite and
 * test the journal using the persistence capable object index and not just the
 * {@link SimpleObjectIndex}.
 * 
 * @todo Review the logic that isolations mutations on the object index. For
 *       example, a delete of an object does not remove an entry from the base
 *       object index but rather records that the object is marked as deleted in
 *       that entry. When the isolated object index is merged down onto the
 *       global object index entries for objects that are marked as deleted are
 *       then deleted on the global index. When the transaction commits, the
 *       state change in the global index is atomically committed.
 * 
 * @todo Verify that we cause the storage allocated to the object index to be
 *       released when it is not longer accessible, e.g., the transaction
 *       commits or objects have been deleted and are no longer visible so we
 *       now remove their entries from the object index -- this can cause leaves
 *       and nodes in the object index to become empty, at which point they are
 *       deleted.
 * 
 * @todo Consider whether a call back "IValueSetter" would make it possible to
 *       update the value on the leaf in a more sophisticated manner. E.g.,
 *       rather than having to lookup to see if a value exists and then insert
 *       the value, we could pass in a lamba expression that gets evaluated in
 *       context and is able to distinguish between a value that did not exist
 *       and one that does exist. Ideally we could offer three options at that
 *       point : test, set, and delete, but just test/set is fine for most
 *       purposes since remove(key) has the same effect.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ObjectIndex extends BTree implements IObjectIndex {

    /**
     * @param store
     * @param branchingFactor
     * @param hardReferenceQueue
     */
    public ObjectIndex(IRawStore store, int branchingFactor,
            HardReferenceQueue<PO> hardReferenceQueue) {

        super(store, branchingFactor,
                DefaultNodeSplitPolicy.INSTANCE,
                DefaultLeafSplitPolicy.INSTANCE,
                hardReferenceQueue, new IndexEntrySerializer(
                store.getSlotMath()));

    }

    /**
     * 
     * @param store
     * @param metadataId
     * @param leafQueue
     */
    public ObjectIndex(IRawStore store, long metadataId, HardReferenceQueue<PO> leafQueue) {
        
        super(store,metadataId,
                DefaultNodeSplitPolicy.INSTANCE,
                DefaultLeafSplitPolicy.INSTANCE,
                leafQueue,
                new IndexEntrySerializer(store.getSlotMath()));
        
    }


    /*
     * IObjectIndex.
     */

    /**
     * Add / update an entry in the object index.
     * 
     * @param id
     *            The persistent id.
     * @param slots
     *            The slots on which the current version is written.
     */
    public void put(int id, ISlotAllocation slots) {

        assert id > IBTree.NEGINF && id < IBTree.POSINF;

    }

    /**
     * Return the slots on which the current version of the object is
     * written.
     * 
     * @param id
     *            The persistent id.
     * @return The slots on which the current version is written.
     */
    public ISlotAllocation get(int id) {

        throw new UnsupportedOperationException();

    }

    /**
     * Mark the object as deleted.
     * 
     * @param id
     *            The persistent id.
     */
    public void delete(int id) {

        throw new UnsupportedOperationException();

    }

}
