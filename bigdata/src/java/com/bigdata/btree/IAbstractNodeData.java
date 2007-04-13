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
 * Created on Dec 19, 2006
 */

package com.bigdata.btree;

/**
 * Interface for low-level data access.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAbstractNodeData {

    /**
     * True iff this is a leaf node.
     */
    public boolean isLeaf();

    /**
     * The branching factor is maximum the #of children for a node or maximum
     * the #of values for a leaf.
     * 
     * @return The branching factor.
     */
    public int getBranchingFactor();
    
    /**
     * The #of entries (aka keys or values) spanned by this node or leaf.
     * 
     * @see INodeData#getChildEntryCounts()
     */
    public int getEntryCount();

    /**
     * The #of keys defined keys for the node or leaf. The maximum #of keys for
     * a node is one less than the {@link #getBranchingFactor()}. The maximum
     * #of keys for a leaf is the {@link #getBranchingFactor()}.
     * 
     * @return The #of defined keys.
     */
    public int getKeyCount();
    
    /**
     * The object used to contain and manage the keys.
     */
    public IKeyBuffer getKeys();
    
}
