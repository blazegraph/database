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
 * Created on Mar 15, 2007
 */

package com.bigdata.journal;

import com.bigdata.service.IDataService;

/**
 * An interface implemented by an {@link IDataService} for the commit / abort of
 * the local write set for a transaction as directed by a centralized
 * {@link ITransactionManager} in response to client requests.
 * <p>
 * Clients DO NOT make direct calls against this API. Instead, they MUST locate
 * the {@link ITransactionManager} service and direct messages to that service.
 * <p>
 * Note: These methods should be invoked iff the transaction manager knows that
 * the {@link IDataService} is buffering writes for the transaction.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME in order to support 2-/3-phase commit, the [commitTime] from the
 * transaction manager service must be passed through to the journal rather than
 * being returned from {@link #commit(long)}. There also needs to be a distinct
 * "prepare" message that validates the write set of the transaction and makes
 * it restart safe. finally, i have to coordinate the serialization of the wait
 * for the "commit" message. (The write set of the transaction also needs to be
 * restart safe when it indicates that it has "prepared" so that a commit will
 * eventually succeed.)
 */
public interface ITxCommitProtocol {

    /**
     * Request commit of the transaction write set.
     */
    public long commit(long tx) throws ValidationError;

    /**
     * Request abort of the transaction write set.
     */
    public void abort(long tx);

}
