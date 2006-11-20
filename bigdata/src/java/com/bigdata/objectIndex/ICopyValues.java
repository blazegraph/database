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
 * Created on Nov 20, 2006
 */

package com.bigdata.objectIndex;

/**
 * Interface encapsulates knowledge required to copy references or clone objects
 * when copy-on-write is invoked for a {@link Leaf}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated We can just steal the values instead.
 */
public interface ICopyValues {

    /**
     * This method is invoked when a {@link Leaf} is cloned and must a copy or
     * clone the values. If the values are immutable objects, then their
     * references may be copied. Otherwise, the value objects MUST be cloned,
     * deep-copied, or whatever so that changes to the value objects in the new
     * {@link Leaf} do NOT bleed back into the source {@link Leaf}.
     * 
     * @param src
     *            The array of objects on the source (immutable) leaf.
     * @param dst
     *            The array of objects on the new (mutable) leaf.
     * @param nkeys
     *            The #of valid keys in the source leaf. There is one object per
     *            key for a leaf. Values [0:nkeys-1] will be defined in the
     *            source array and must be copied to the same indices in the
     *            destination array.
     */
    public void copyValues(Object[] src, Object[] dst, int nkeys);
    
}
