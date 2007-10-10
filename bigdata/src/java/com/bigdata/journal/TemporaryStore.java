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
 * Created on Feb 21, 2007
 */

package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ByteArrayValueSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.rawstore.WormAddressManager;

/**
 * A temporary store that supports named indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TemporaryStore extends TemporaryRawStore implements IIndexManager {

    /**
     * 
     */
    public TemporaryStore() {

        this(WormAddressManager.DEFAULT_OFFSET_BITS,
                DEFAULT_INITIAL_IN_MEMORY_EXTENT,
                DEFAULT_MAXIMUM_IN_MEMORY_EXTENT, false);
        
    }

    /**
     * @param offsetBits
     *            This determines the capacity of the store file and the maximum
     *            length of a record.  The value is passed through to
     *            {@link WormAddressManager#WormAddressManager(int)}.
     * @param initialInMemoryExtent
     * @param maximumInMemoryExtent
     * @param useDirectBuffers
     */
    public TemporaryStore(int offsetBits, long initialInMemoryExtent,
            long maximumInMemoryExtent, boolean useDirectBuffers) {

        super(offsetBits, initialInMemoryExtent, maximumInMemoryExtent,
                useDirectBuffers);

        setupName2AddrBTree();

    }
    
    /**
     * BTree mapping index names to the last metadata record committed for the
     * named index. The keys are index names (unicode strings). The values are
     * the last known address of the named btree.
     * <p>
     * Note: This is a mutable {@link BTree} so it is NOT thread-safe. We always
     * synchronize on this object before accessing it.
     */
    private Name2Addr name2Addr;

    /**
     * Setup the btree that resolved named btrees.
     */
    private void setupName2AddrBTree() {

        assert name2Addr == null;
        
        name2Addr = new Name2Addr(this);

    }

    /**
     * Registers a {@link BTree} whose values are variable length byte[]s.
     */
    public IIndex registerIndex(String name) {
    
        return registerIndex(name, new BTree(this,
                BTree.DEFAULT_BRANCHING_FACTOR,
                UUID.randomUUID(),
                ByteArrayValueSerializer.INSTANCE));
        
    }
    
    public IIndex registerIndex(String name, IIndex btree) {

        synchronized (name2Addr) {

            // add to the persistent name map.
            name2Addr.add(name, btree);

            return btree;

        }
        
    }
    
    public void dropIndex(String name) {
        
        synchronized(name2Addr) {

            // drop from the persistent name map.
            name2Addr.dropIndex(name);
            
        }
        
    }

    /**
     * Return the named index (unisolated). Writes on the returned index will be
     * made restart-safe with the next {@link #commit()} regardless of the
     * success or failure of a transaction. Transactional writes must use the
     * same named method on the {@link Tx} in order to obtain an isolated
     * version of the named btree.
     */
    public IIndex getIndex(String name) {

        synchronized(name2Addr) {

            return name2Addr.get(name);
            
        }

    }

}
