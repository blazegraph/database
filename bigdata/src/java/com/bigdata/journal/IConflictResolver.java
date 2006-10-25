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
 * Created on Oct 23, 2006
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * An interface invoked during backward validation when a write-write conflict
 * has been detected. The interface must either resolve the write-write conflict
 * by returning a new version in which the conflict is resolved or report an
 * unresolvable conflict, in which case backward validation will force the
 * transaction to abort.
 * </p>
 * <p>
 * Note: You MUST provide a public constructor with the signature: <code>
 * <i>class</i>( Journal journal )</code>
 * </p>
 * 
 * @see http://www.cs.brown.edu/~mph/Herlihy90a/p96-herlihy.pdf for an excellent
 *      discussion of data type specific state-based conflict resolution,
 *      including examples for things such as bank accounts.
 * 
 * @todo Write tests in which we do state-based conflict resolution for both the
 *       bank account examplres in Herlihy and the examples that we will find in
 *       race conditions for the lexical terms and statements in an RDF model.
 * 
 * @todo Will object deserialization be buffered by a cache? Cache integrity is
 *       tricky here since the objects being reconciled belong to different
 *       transactional states. I can see a situation in which conflict
 *       resolution requires multiple changes in a graph structure, and could
 *       require simultaneous traversal of both a read-only view of the last
 *       committed state on the journal and a read-write view of the
 *       transaction. Resolution would then proceed by WRITE or DELETE
 *       operations on the transaction. The method signature would be changed
 *       to:
 * 
 * <pre>
 * resolveConflict(int32 id,Tx readOnly,Tx readWrite)
 * </pre>
 * 
 * This would have to allow READ,WRITE,and DELETE operations during PREPARE.<br>
 * 
 * This is nice, but is does require that we handle concurrent modification of
 * the object index during traversal (and concurrent traversal) since validation
 * is traversing the index while it is being used by the conflict resolver to
 * READ, WRITE, and DELETE objects. This can be handled with a non-persistence
 * capable object index simply by changing from {@link HashMap}
 * {@link ConcurrentHashMap}. Handling this for a persistence capable data
 * structure will require significantly more effort (but will provide a
 * worthwhile feature for btree support).<br>
 * 
 * Handling distributed conflicts could also require awareness of the int64
 * identifier. That is accessible to the caller if they have saved a reference
 * to the {@link Journal} in the constructor. Such resolution would have to be
 * network aware (i.e., aware that there was a distributed database). <br>
 * 
 * If we change to allowing object graph traverals during conflict resolution,
 * then we need to closely consider whether cycles can form and how they will
 * ground out.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IConflictResolver {

    /**
     * <p>
     * Resolve a write-write conflict between a committed version on the journal
     * and the current version within a transaction that is validating.
     * </p>
     * 
     * @param id
     *            The int32 within segment persistent identifier.
     * 
     * @param readOnlyTx
     *            A read-only transactional view from which the committed
     *            version may be read.
     * 
     * @param tx
     *            The transaction that is being validated.
     * 
     * @exception RuntimeException
     *                if you are not able to resolve the conflict.
     * 
     * @todo DELETE operations are a special case of WRITE operations. However,
     *       there is no way to overwrite a committed delete at this time using
     *       {@link Tx#write(int, ByteBuffer)} or
     *       {@link Journal#write(Tx, int, ByteBuffer)}
     */

    public void resolveConflict(int id, Tx readOnlyTx, Tx readWriteTx)
            throws RuntimeException;

}
