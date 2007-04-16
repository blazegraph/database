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
 * Class with implementations supporting mutable and immutable variable length
 * byte[] keys.
 * 
 * @todo supporting a scalar int32 key natively would provide better performance
 *       for an object index. once it is up to int64, the byte[] approach could
 *       in fact be better on 32-bit hardware.
 * 
 * @todo explore the use of sparse buffers that minimize copying for more
 *       efficient management of large #s of keys - how would search work for
 *       such buffers?
 * 
 * @todo explore use of interpolated search. certainly we should be able to
 *       estimate the distribution of the keys when creating an immutable key
 *       buffer and choose binary vs interpolated vs linear search based on
 *       that.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractKeyBuffer implements IKeyBuffer {


    /**
     * Test the search key against the leading prefix shared by all bytes in the
     * key buffer.
     * 
     * @param prefixLength
     *            The length of the prefix shared by all keys in the buffer.
     * 
     * @param searchKey
     *            The search key.
     * 
     * @return Zero iff all bytes match and otherwise the insert position for
     *         the search key in the buffer. The insert position will be before
     *         the first key iff the search key is less than the prefix (-1) and
     *         will be after the last key iff the search key is greater than the
     *         prefix (-(nkeys)-1).
     */
    abstract protected int _prefixMatchLength(final int prefixLength,
            final byte[] searchKey);
    
    /**
     * Linear search.
     */
    abstract protected int _linearSearch(final int searchKeyOffset, final byte[] searchKey);

    /**
     * Binary search.
     */
    abstract protected int _binarySearch(final int searchKeyOffset, final byte[] searchKey);

}
