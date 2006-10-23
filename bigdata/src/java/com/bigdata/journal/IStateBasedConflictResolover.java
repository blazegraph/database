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

/**
 * <p>
 * An interface invoked during backward validation when a write-write conflict
 * has been detected. The interface must either resolve the write-write conflict
 * by returning a new version in which the conflict is resolved or report an
 * unresolvable conflict, in which case backward validation will force the
 * transaction to abort.
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
 * @todo How are write-delete conflicts represented? How are they handled?
 * 
 * @todo Does the resolver need any metadata about the transaction? It probably
 *       does require metadata for deserialization of objects.
 * 
 * @todo Will object deserialization be buffered by a cache? Cache integrity is
 *       tricky here since the objects being reconciled belong to different
 *       transactional states.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IStateBasedConflictResolover {

    /**
     * Resolve a conflict between a committed version and a proposed version.
     * 
     * @param committedVersion
     *            The committed version (the version found on the journal).
     * @param proposedVersion
     *            The proposed version (the version last written onto the
     *            transaction that is being validated).
     * @return A version in which the conflict is reconciled or
     *         <code>null</code>
     */

    public ByteBuffer resolve(ByteBuffer committedVersion,
            ByteBuffer proposedVersion);
        
}
