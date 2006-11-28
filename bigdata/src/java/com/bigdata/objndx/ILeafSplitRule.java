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
 * Created on Nov 27, 2006
 */
package com.bigdata.objndx;

/**
 * Interface for computing the splitIndex and separatorKey when splitting a
 * {@link Leaf}.  When a {@link Leaf} is split, the original {@link Leaf} is
 * retained, a new rightSibling of the leaf is created, and a separatorKey is
 * inserted into the parent.  The splitIndex is the index of the first key to
 * be moved to the rightSibling.  The separatorKey defines the lowest key value
 * that may enter into the rightSibling and may be either one of the keys from
 * the leaf that is being split or the insert key itself.
 * 
 * Note: This API does not permit a single global instance to be shared since
 * that would mean static private state for the instance and invocations by
 * concurrent but distinct btrees would result in corruptions. All leaves of a
 * btree need to have the same branching factor and that is constrant for the
 * life of the tree. This API should therefore be initialized once per btree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ILeafSplitRule {
    
    /**
     * Return the splitIndex (the index of the first key to move to the
     * rightSibling) and set the separatorKey as a side effect whose state is
     * accessible using {@link #getSeparatorKey()}.
     * 
     * @param keys
     * @param insertKey
     * @return
     */
    public int getSplitIndex(int[] keys, int insertKey );

    /**
     * The separatorKey to be inserted into the parent.
     * 
     * @return
     */
    public int getSeparatorKey();

}