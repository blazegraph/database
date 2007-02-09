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
 * Created on Feb 8, 2007
 */

package com.bigdata.journal;

import java.util.Properties;

import com.bigdata.objndx.BTree;
import com.bigdata.objndx.IIndex;
import com.bigdata.rawstore.IRawStore;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IJournal extends IRawStore, IAtomicStore, IStore {

    /**
     * A copy of the properties used to initialize this journal.
     */
    public Properties getProperties();
    
    /**
     * An overflow condition arises when the journal is within some declared
     * percentage of its maximum capacity during a {@link #commit()}. If this
     * event is not handled then the journal will automatically extent itself
     * until it either runs out of address space (int32) or other resources.
     */
    public void overflow();
    
    /**
     * Register a named index. Once registered the index will participate in
     * atomic commits.
     * <p>
     * Note: A named index must be registered outside of any transaction before
     * it may be used inside of a transaction.
     * <p>
     * Note: The return object MAY differ from the supplied {@link BTree}. For
     * example, when using partitioned indices the {@link BTree} is encapsulated
     * within an abstraction that knows how to managed index partitions.
     * 
     * @param name
     *            The name that can be used to recover the index.
     * 
     * @param btree
     *            The btree.
     * 
     * @return The object that would be returned by {@link #getIndex(String)}.
     * 
     * @see IStore#getIndex(String)
     * 
     * @todo The provided {@link BTree} must serve as a prototype so that it is
     * possible to retain additional metadata.
     */
    public IIndex registerIndex(String name, IIndex btree);
    
    /**
     * Return the named index which MAY may be invalidated by a
     * {@link IAtomicStore#commit()}.
     */
    public IIndex getIndex(String name);

}
