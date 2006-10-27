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
 * Created on Oct 9, 2006
 */

package com.bigdata.istore;

/**
 * Object manager.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo The largest question facing an unisolated object manager is its
 *       behavior in a distributed database with other concurrent unisolated
 *       object managers and with concurrent transactions. There are also
 *       unresolved questions in concurrency control simply within the journal,
 *       which implements MVCC.  I need a better distinction here between what
 *       is possible and what is useful.
 * 
 * @todo Support named objects with transactional isolation. The notional design
 *       uses a B+Tree behind the scenes. The root of the btree is stored in a
 *       known location, e.g., the root block of segment0. There can also be
 *       segment local names. An embedded database would provide only global
 *       names, and they would be implemented as segment local names. Changes to
 *       the tree need to be validated. Other than having to store its root in a
 *       special slot, the tree can use the normal object api and the standard
 *       btree (vs the specialized object index variant). A short term solution
 *       can simply use a hash table to store the name : id mapping.
 * 
 * @todo Object caching, which is part of the OM contract (for reference testing
 *       for equality within a VM).
 * 
 * @todo Add operations for creating btree
 * 
 * @todo Add stream-based operations. Note that very large objects are going to
 *       scale out using an unisolated stream-based API. That API probably
 *       belongs to the {@link IStore} rather than the {@link IOM} since large
 *       streams can not be made transactional (well, they can we a journal
 *       designed for large writes and using a disk-only strategy, but do you
 *       want to bother?) Maybe we should put those methods on IOM so that
 *       people can elect for either isolated or unisolated streams. However,
 *       unisolated streams probably use a very different mechanism, e.g.,
 *       connecting to a stream service that writes directly to disk - but a
 *       disk-only journal would probably work fine for that purpose.
 * 
 * @todo Reconcile with the API for the pluggable backends for generic-native.
 * 
 * @todo Resolve on the behavior when an identifier is not found and when it is
 *       known to be deleted. The journal has one set of semantics here since it
 *       knows when something has been deleted - at least until the tx in which
 *       the delete occurs is GC'd. Those same semantics will probably be
 *       visible to bigdata since the journal's MVCC strategy is the basis for
 *       bigdata's concurrency control. The decision should apply equally to
 *       update() and delete() of a persistent identifier.
 */
public interface IOM {

    /**
     * Insert an object into the store.
     * 
     * @param obj The object (required).
     * 
     * @return The int64 persistent identifier.
     */
    public long insert(Object obj);

    /**
     * Read an object from the store.
     * 
     * @param id
     *            The int64 persistent identifier.
     * 
     * @return The object.
     * 
     * @throws NotFoundException
     *             if the identifier can not be resolved.
     */
    public Object read(long id);

    /**
     * Update an object on the store.
     * 
     * @param id
     *            The int64 persistent identifier.
     * 
     * @param obj
     *            The new object.
     * 
     * @throws NotFoundException
     *             if the identifier can not be resolved.
     */
    public void update(long id, Object obj);

    /**
     * Delete an object in the store.
     * 
     * @param id
     *            The int64 persistent identifier.
     * 
     * @throws NotFoundException
     *             if the identifier can not be resolved, including if the data
     *             has already been deleted.
     */
    public void delete(long id);

    /**
     * @todo Non-transactional, cached, immutable writes.
     * 
     * @return
     */
    public IOMExtensibleSerializer getExtensibleSerializer();
    
//    /**
//     * Write a stream onto the store.
//     * 
//     * @return A stream on which the data can be written.
//     */
//    public StoreOutputStream getOutputStream();
//
//    /**
//     * Read data from the store.
//     * 
//     * @param id
//     *            The persistent identifier.
//     * 
//     * @return An input stream from which you can read the data.
//     * 
//     * @throws NotFoundException
//     *             if the identifier can not be resolved.
//     */
//    public InputStream getInputStream(long id);
//
//    /**
//     * Update data in the store.
//     * 
//     * @param id
//     *            The persistent identifier.
//     *            
//     * @return A stream on which the new data can be written.
//     */
//    public StoreOutputStream getUpdateStream(long id);
//
//    /**
//     * <p>
//     * An output stream used to insert or update data in the store. The caller
//     * writes on the stream. When the stream is closed, the data is saved to the
//     * store. Internally, data is incrementally written against the store so
//     * that this mechanism may be used to write very large objects. The data is
//     * saved by invoking {@link #close()}.
//     * </p>
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     * 
//     * @todo Use factory to obtain pre-existing instances where possible.
//     * 
//     * @todo Support blobs transparently by writing on this stream.
//     * 
//     * @todo Consider protocol for aborting an operation. Certainly the
//     *       application can just thrown an exception forcing a transaction to
//     *       rollback.
//     */
//    
//    public abstract static class StoreOutputStream extends OutputStream {
//
//        /**
//         * The persistent identifier for the data. This is assigned by the store
//         * when inserting new data and provided by the application when updating
//         * existing data.
//         * 
//         * @return The persistent identifier.
//         */
//        abstract public long getId();
//
//    }

}
