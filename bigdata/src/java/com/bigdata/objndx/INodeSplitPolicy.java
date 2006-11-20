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
 * Created on Nov 15, 2006
 */

package com.bigdata.objndx;

/**
 * Interface for computing the index at which to split a node.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface INodeSplitPolicy {

    /**
     * <p>
     * A node is split by choosing a split key. The node is split into
     * (node, rightSibling). The splitKey is not retained by either the
     * original node or its rightSibling. We then insert( splitKey,
     * rightSibling ) into the parent. The keys and child references to the
     * left of the key are retained by the original node while the keys and
     * child references to the right of the key are copied to a new right
     * sibling.
     * </p>
     * <p>
     * The most cited policy for splitting a node is to choose the key at
     * m/2-1, where m is the maximum #of child references for a node (aka
     * the branching factor or order of the nodes in the tree). Note that a
     * node always has one fewer keys than child references.
     * </p>
     * <p>
     * For example, when m := 3.
     * </p>
     * 
     * <pre>
     * keys : [ k0 k1 -- ]
     * refs : [ r0 r1 r2 ]
     * </pre>
     * 
     * <p>
     * where "--" denotes the unusable key index (#keys := #children-1).
     * </p>
     * If we choose m/2-1 equals zero(0) as the split index, then the key k0
     * is inserted into the parent and the node is split as follows:
     * </p>
     * 
     * <pre>
     * keys : [       -- ]   [ k1    -- ]
     * refs : [ r0       ]   [ r1 r2    ]
     * </pre>
     * 
     * <p>
     * On the other hand, if we choose m/2 equals one(1) as the split index,
     * then the key k1 is inserted into the parent and the node is split as
     * follows:
     * </p>
     * 
     * <pre>
     * keys : [ k0    -- ]   [       -- ]
     * refs : [ r0 r1    ]   [ r1       ]
     * </pre>
     * 
     * <p>
     * In general, you may choose to split on any key and the result is
     * always well-formed (the well-formedness constraint is #of child
     * references >= 1 and #of keys == #of child references - 1, note that
     * the #of keys in a node MAY be zero).
     * </p>
     * 
     * @param node
     *            The node to be split.
     * 
     * @return The index of the key to promote to the parent node.
     */
    public int splitNodeAt(Node node);
            
}
