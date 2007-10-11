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

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.service.IDataService;

/**
 * Interface for managing named indices.
 * 
 * FIXME Consider removing this interface for better alignment with the
 * {@link ConcurrentJournal}. Access to indices SHOULD only occur within
 * submitted {@link AbstractTask}s. The presence of this interface on the
 * {@link ConcurrentJournal} makes it possible to access a named index outside
 * of an executor service. {@link AbstractJournal} SHOULD NOT implement
 * {@link IIndexStore#getIndex(String)} since that returns the
 * <strong>unisolated</strong> (mutable) view of the named index and access to
 * that view MUST be single-threaded, which is enforced by the
 * {@link ConcurrentJournal} only within {@link AbstractTask}s.
 * <p>
 * Remove these methods from the {@link IJournal} heirarchy of implementations
 * but do retain them for {@link TemporaryStore} since it is (a) single-threaded
 * and (b) people need the ability to add/drop indices. The interface itself
 * could still go away.
 * <p>
 * Likewise {@link IIndexStore} needs to be available to
 * {@link AbstractTask}s, on the {@link Tx}, and on
 * {@link TemporaryStore} but MUST NOT be exposed elsewhere.
 * <p>
 * A bunch of test cases will have to be updated and the RDFS database will also
 * have to be updated (to use more concurrency as well as to refactor its index
 * access inside of {@link AbstractTask}s).
 * <p>
 * Get rid of {@link NoSuchIndexException} and {@link IndexExistsException} at
 * the same time since they are probably no longer.
 * <p>
 * Review {@link IDataService} methods for add/drop index.
 * <p>
 * Also drop {@link IAtomicStore#abort()} and {@link IAtomicStore#commit()}
 * since those operations are automatic, not something that can be directly
 * invoked by an application.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IIndexManager extends IIndexStore {

    /**
     * Register a named index (unisolated). Once registered the index will
     * participate in atomic commits.
     * <p>
     * Note: A named index must be registered outside of any transaction before
     * it may be used inside of a transaction.
     * 
     * @param name
     *            The name that can be used to recover the index.
     * 
     * @return The object that would be returned by {@link #getIndex(String)}.
     * 
     * @exception IllegalStateException
     *                if there is an index already registered under that name.
     */
    public IIndex registerIndex(String name);

    /**
     * Register a named index (unisolated). Once registered the index will
     * participate in atomic commits.
     * <p>
     * Note: A named index must be registered before it may be used inside of a
     * transaction.
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
     * @exception IndexExistsException
     *                if there is an index already registered under that name.
     *                Use {@link IIndexStore#getIndex(String)} to test whether
     *                there is an index registered under a given name.
     * 
     * @todo The provided {@link BTree} must serve as a prototype so that it is
     *       possible to retain additional metadata.
     */
    public IIndex registerIndex(String name, IIndex btree);

    /**
     * Drops the named index (unisolated). The index will no longer participate
     * in atomic commits.
     * <p>
     * Note: Whether or not and when index resources are reclaimed is dependent
     * on the store. For example, an immortal store will retain all historical
     * states for all indices. Likewise, a store that uses index partitions may
     * be able to delete index segments immediately.
     * 
     * @param name
     *            The name of the index to be dropped.
     * 
     * @exception NoSuchIndexException
     *                if <i>name</i> does not identify a registered index.
     * 
     * @todo add a rename index method, but note that names in the file system
     *       would not change.
     * 
     * @todo declare a method that returns or visits the names of the registered
     *       indices.
     */
    public void dropIndex(String name);
    
}
