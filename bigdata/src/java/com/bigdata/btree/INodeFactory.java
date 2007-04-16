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
package com.bigdata.btree;

/**
 * Interface for creating nodes or leaves.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface INodeFactory {

    /**
     * Create a node. The implementation is encouraged to steal the <i>keys</i>
     * and <i>childAddr</i> references rather than cloning them.
     * 
     * @param btree
     *            The owning btree.
     * @param addr
     *            The address from which the node was read.
     * @param branchingFactor
     *            The branching factor for the node.
     * @param nentries
     *            The #of entries spanned by this node.
     * @param keys
     *            A representation of the defined keys in the node.
     * @param childAddr
     *            An array of the persistent addresses for the children of this
     *            node.
     * @param childEntryCount
     *            An of the #of entries spanned by each direct child.
     * 
     * @return A node initialized from those data.
     */
    public INodeData allocNode(IIndex btree, long addr, int branchingFactor,
            int nentries, IKeyBuffer keys, long[] childAddr,
            int[] childEntryCount);

    /**
     * Create a leaf. The implementation is encouraged to steal the <i>keys</i>
     * and <i>values</i> references rather than cloning them.
     * 
     * @param btree
     *            The owning btree.
     * @param addr
     *            The address from which the leaf was read.
     * @param branchingFactor
     *            The branching factor for the leaf.
     * @param keys
     *            A representation of the defined keys in the node.
     * @param values
     *            An array containing the values found in the leaf.
     * 
     * @return A leaf initialized from those data.
     */
    public ILeafData allocLeaf(IIndex btree, long addr, int branchingFactor,
            IKeyBuffer keys, Object[] values);

}
