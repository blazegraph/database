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

package com.bigdata.isolation;

import java.io.Serializable;

import com.bigdata.journal.Journal;
import com.bigdata.journal.Tx;

/**
 * <p>
 * An interface invoked during backward validation when a write-write conflict
 * has been detected. The implementation must either resolve the write-write
 * conflict by returning a new version in which the conflict is resolved or
 * report an unresolvable conflict, in which case backward validation will force
 * the transaction to abort.
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
 * @todo How to handle cascading dependencies. For example, if there is a
 *       write-write conflict on the terms index of an RDF store then the
 *       reverse index and any statements written in the transaction for the
 *       term with which the write-write conflict exists must also be updated.
 *       This will require lots of application knowledge and access to the
 *       {@link Tx} object itself?
 * 
 * @todo The burden of deserialization is on the application and its
 *       implementation of this interface at present. Will object
 *       deserialization be buffered by a cache? Cache integrity is tricky here
 *       since the objects being reconciled belong to different transactional
 *       states. I can see a situation in which conflict resolution requires
 *       multiple changes in a graph structure, and could require simultaneous
 *       traversal of both a read-only view of the last committed state on the
 *       journal and a read-write view of the transaction. Resolution would then
 *       proceed by WRITE or DELETE operations on the transaction. The method
 *       signature would be changed to:
 * 
 * This would have to allow READ,WRITE,and DELETE operations during PREPARE.<br>
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
public interface IConflictResolver extends Serializable {

    /**
     * <p>
     * Resolve a write-write conflict between a committed version on the journal
     * and the current version within a transaction that is validating.
     * </p>
     * 
     * @param key
     *            The key for which the write-write conflict occurred.
     * 
     * @param comittedValue
     *            The historical value committed in the global state with which
     *            the write-write conflict exists.
     * 
     * @param txEntry
     *            The value written by the transaction that is being validated.
     * 
     * @return A {@link Value} in which the conflict has been resolved or
     *         <code>null</code> if the value could not be resolved. This may
     *         be either of the provided values or a new one constructed based
     *         on an examination of the provided values.
     */
    public Value resolveConflict(byte[] key, Value comittedValue, Value txEntry)
            throws RuntimeException;

}
