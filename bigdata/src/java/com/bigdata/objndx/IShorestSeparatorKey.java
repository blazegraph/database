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
 * Created on Dec 18, 2006
 */

package com.bigdata.objndx;

/**
 * A notional interface that an application may implement in order to minimize
 * the length of the keys that are stored in the non-leaf nodes of a
 * {@link IBTree} having <em>variable length keys</em>.
 * 
 * This operation is invoked when a leaf is split or when keys must be
 * redistributed between two leaves that are siblings of some common parent
 * node. At those times the separatorKey in the parent node must be choosen.
 * When variable length keys are used, the choice can produce shorter keys in
 * nodes that are the immediate parents of leaves. For example, assuming that
 * you have an index with variable length character data as keys:
 * 
 * <pre>
 *       leaf:    [ motivate, movie, moving, mower ]
 * </pre>
 * 
 * This leaf will be split at index 2 as follows:
 * 
 * <pre>
 *       leaf:    [ motivate, movie, -, - ]
 *       sibling: [ moving, mower, -, - ]
 * </pre>
 * 
 * The separator key MUST be less than or equal to "moving" and strictly greater
 * than "movie". If we choose the shortest successor of "movie" that is not
 * greater than "moving" the separator key would be "movie\0" (the successor of
 * "movie") rather than "moving". This would save one (1) character in the
 * separatorKey.
 * 
 * Note that this trick can not be used at higher levels in the btree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated I rate this feature as not worth while. It was originally
 *             described by Bayer and Unterauer in "Prefix B-Trees". Their
 *             concern was to reduce the height of the tree by compacting more
 *             data into a page and for the index nodes. We are achieving
 *             similar means through other devices, notably allowing the page
 *             size to vary while fixing the branching factor and using large
 *             branching factors and compression in {@link IndexSegments}. If
 *             implemented, the effect would be to reduce the size on disk of a
 *             node in an index segment. However, those nodes are already
 *             compressed so it is uncertain if there would be any real savings.
 */
public interface IShorestSeparatorKey {

    /**
     * Choose the shortest separator key that is greater than the key at
     * <code>n1.keys[pos1]</code> and less than or equal to the key at
     * <code>n2.keys[pos2]</code>. This operation is type specific and is
     * only meaningful when the keys have variable length.
     * 
     * @param n1
     *            The node having the exclusive lower bound for the separator
     *            key at index <i>pos1</i>.
     * @param pos1
     *            The position of the key in <i>n1</i> that is the exclusive
     *            lower bound for the separator key to be choosen.
     * @param n2
     *            The node having the inclusive upper bound for the separator
     *            key at index <i>pos2</i.
     * @param pos2
     *            The position of the key in <i>n2</i> that is the inclusive
     *            upper bound for the separator key to be choosen.
     */
    public Object getShortestSeparatorKey(AbstractNode n1,int pos1,AbstractNode n2,int pos2);
    
}
