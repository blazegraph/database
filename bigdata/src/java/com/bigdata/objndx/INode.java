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
 * Created on Dec 15, 2006
 */

package com.bigdata.objndx;

/**
 * Interface for the non-leaf nodes of a B+-Tree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface INode extends IAbstractNode {

    /**
     * The #of children of this node. Either all children will be {@link INode}s
     * or all children will be {@link ILeaf leaves}. The #of children of a node
     * MUST be <code>{@link #getKeyCount()}+1</code>
     * 
     * @return The #of children of this node.
     */
    public int getChildCount();

    /**
     * The backing array of the persistent addresses of the children. Only the
     * first {@link #getChildCount()} entries in the returned array are defined.
     * If an entry is zero(0L), then the corresponding child is not persistent.
     * The use of this array is dangerous since mutations are directly reflected
     * in the node, but it may be highly efficient. Callers MUST excercise are
     * to perform only read-only operations against the returned array.
     * 
     * @return The backing array of persistent child addresses.
     */
    public long[] getChildAddr();
        
}
