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
 * Created on Feb 1, 2007
 */

package com.bigdata.objndx;

/**
 * <p>
 * Interface for batch operations a B+-Tree mapping variable length unsigned
 * byte[] keys to arbitrary values. Batch operations can be very efficient if
 * the keys are presented in sorted order.
 * </p>
 * <p>
 * All mutation operations on a {@link BTree} are executed in a single threaded
 * context and are therefore atomic. A batch api operation that does NOT span
 * more than one index partition is therefore atomic. However, if an operation
 * spans multiple partitions of an index then NO GUARENTEE is made that the
 * operation is atomic over the set of index partitions.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see KeyBuilder, which may be used to encode one or more values into a
 *      variable length unsigned byte[] key.
 * 
 * @todo add batch api for rangeCount and rangeQuery.
 * 
 * @todo support batch api for indexOf(), keyAt(), valueAt()?
 * 
 * @todo add extensible operation defined by an vector
 *       {@link UserDefinedFunction}. Use this to move application logic to the
 *       indices as an alternative to scalar {@link UserDefinedFunction}s.  For
 *       example, the logic that tests an index for a key, increments a counter
 *       if the key is not found, and inserts the counter under the key.
 */
public interface IBatchBTree {

    /**
     * Apply a batch insert operation.
     */
    public void insert(BatchInsert op);

    /**
     * Apply a batch lookup operation.
     */
    public void lookup(BatchLookup op);

    /**
     * Apply a batch existence test operation.
     */
    public void contains(BatchContains op);

    /**
     * Apply a batch remove operation.
     */
    public void remove(BatchRemove op);

}
