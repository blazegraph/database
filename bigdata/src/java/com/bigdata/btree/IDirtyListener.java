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
 * Created on Oct 8, 2007
 */

package com.bigdata.btree;

/**
 * An interface that may be used to learn when a {@link BTree} becomes
 * dirty.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IDirtyListener {

    /**
     * The btree has become dirty.
     * <p>
     * Note: This event is always generated for a new btree. Once a btree is
     * created it remains dirty until the root (and any dirty children) have
     * been flushed to the backing store. A btree that is read from the backing
     * store is always clean and consists of "immutable" nodes and/or leaves. A
     * btree remains clean until there is a write on some node or leaf. That
     * write triggers copy-on-write, which percolates from the point of the
     * write up to the root node and results in the reference to the root node
     * being replaced.  When that happens a dirty event is generated.
     * 
     * @param btree
     *            The btree.
     */
    public void dirtyEvent(BTree btree);
    
}
