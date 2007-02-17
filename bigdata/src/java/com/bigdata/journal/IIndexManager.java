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
 * Created on Feb 17, 2007
 */

package com.bigdata.journal;

import com.bigdata.objndx.BTree;
import com.bigdata.objndx.IIndex;

/**
 * Interface for managing named indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IIndexManager extends IStore {

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
     * @todo The provided {@link BTree} must serve as a prototype so that it is
     *       possible to retain additional metadata.
     */
    public IIndex registerIndex(String name, IIndex btree);
    
    /**
     * Drops the named index (unisolated). The index is removed as a
     * {@link ICommitter} and all resources dedicated to that index are
     * reclaimed, including secondary index segment files, the metadata index,
     * etc.
     * 
     * @param name
     *            The name of the index to be dropped.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> does not identify a registered index.
     * 
     * @todo add a rename index method, but note that names in the file system
     *       would not change.
     * 
     * @todo declare a method that returns or visits the names of the registered
     *       indices.
     * 
     * @todo consider adding a delete method that releases all resources
     *       (including indices) and then closes the journal.
     */
    public void dropIndex(String name);
    
}
