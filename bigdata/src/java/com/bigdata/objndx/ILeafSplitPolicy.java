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
 * Interface for computing the index at which to split a leaf.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ILeafSplitPolicy {

    /**
     * <p>
     * A leaf is split by choosing the first key to copy to the new right
     * sibling. The leaf is split into (leaf, rightSibling) and we
     * insert(splitKey, rightSibling) into the parent. Since keys and values
     * are paired one-to-one in a leaf, all keys and values starting with
     * the split index are copied into the new right sibling.
     * </p>
     * <p>
     * The most cited policy for splitting a leaf is to choose the key at
     * m/2, where m is the maximum #of values for a leaf (aka the branching
     * factor or the order of the leaves in the tree).
     * </p>
     * <p>
     * For example, when m := 3.
     * </p>
     * 
     * <pre>
     * keys : [ k0 k1 k2 ]
     * vals : [ v0 v1 v2 ]
     * </pre>
     * 
     * <p>
     * If we choose m/2 equals one(1) as the split index, then the key k1 is
     * inserted into the parent and the leaf is split as follows:
     * </p>
     * 
     * <pre>
     * keys : [ k0 | k1 k2 ] =&gt; [ k0      ] [ k1 k2    ]
     * vals : [ v0 | v1 v2 ] =&gt; [ v0      ] [ v1 v2    ]
     * </pre>
     * 
     * <p>
     * The split index MUST partition the leaf into two non-empty leaves.
     * This means that the split index MUST be choosen from the range
     * [1:#keys-1].
     * </p>
     * <p>
     * For example, if we choose #keys-1 equals 3-1 equals two (2), then the
     * leaf is split as follows and we will insert the key k2 into the
     * parent.
     * </p>
     * 
     * <pre>
     * keys : [ k0 k1 | k2 ] =&gt; [ k0 k1   ] [ k2       ]
     * vals : [ v0 v1 | v2 ] =&gt; [ v0 v1   ] [ v2       ]
     * </pre>
     * 
     * @param leaf
     *            The leaf to be split.
     * @return The index of the first key to copy to the new right sibling.
     */
    public int splitLeafAt(Leaf leaf);

}
