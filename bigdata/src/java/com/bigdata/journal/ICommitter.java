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
package com.bigdata.journal;

import com.bigdata.rawstore.Addr;

/**
 * An interface implemented by a persistence capable data structure such as a
 * btree so that it can participate in the commit protocol for the store.
 * <p>
 * This interface is invoked by {@link Journal#commit()} for each registered
 * {@link ICommitter}. The {@link Addr} returned by {@link #handleCommit()}
 * will be saved in the {@link ICommitRecord} under the index identified by the
 * {@link ICommitter} when it was registered.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IAtomicStore#setCommitter(int, ICommitter)
 */
public interface ICommitter {

    /**
     * Flush dirty state to the store in preparation for an atomic commit and
     * return the {@link Addr address} from which the persistence capable data
     * structure may be reloaded.
     * 
     * @return The {@link Addr address} of the record from which the persistence
     *         capable data structure may be reloaded. If no changes have been
     *         made then the previous address should be returned as it is still
     *         valid.
     */
    public long handleCommit();

}
