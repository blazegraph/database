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
 * Created on Nov 9, 2005
 */
package com.bigdata.objndx.ndx;

import java.util.Date;

/**
 * <p>
 * Interface for logic that returns the next possible key in the natural
 * ordering for the coerced keys index after the specified <i>coercedKey </i>.
 * </p>
 * <p>
 * For example, for {@link String} keys, the successor is formed by appending a
 * NUL character. For {@link Date} keys, the successor is formed by adding the
 * minimum unit of resolution, which is one millisecond for the Java
 * {@link Date} class. For integer keys ({@link Integer} or {@link Long}), the
 * successor is formed by adding ONE (1), but watch out for the maximum integer
 * value. For floating point keys ({@link Float} or {@link Double}), the
 * successor is the next value in the appropriate value set (float or double)
 * and is found by a bit of arcane floating point magic.
 * </p>
 * <p>
 * The successor of a <code>null</code> <i>coercedKey </i> is always the first
 * legal non- <code>null</code> value for that data type.
 * </p>
 * 
 * @see SuccessorUtil
 */

public interface Successor
{

    /**
     * Find the successor of the <i>coercedKey </i>. This is used during query
     * against the index to convert a half-open range into a closed range by
     * finding the next possible key after the end of the half-open range.
     * 
     * @param coercedKey
     *            The key value. Note that the successor of <code>null</code>
     *            is always the first value from the value space for the key
     *            since <code>null</code> s are sorted low by the index.
     * 
     * @return The successor of the <i>coercedKey </i>.
     * 
     * @exception NoSuccessorException
     *                Iff the successor of the <i>coercedKey </i> does not
     *                exist. Keys with finite value spaces, such as
     *                <code>int</code>, <code>long</code>,
     *                <code>float</code> or <code>double</code>, have a
     *                value whose successor is not defined. This exception is
     *                thrown for such keys.
     */

    public Object successor( Object coercedKey )
        throws NoSuccessorException;

    /**
     * Returns true iff the other object is a {@link Successor}
     * with the same semantics as this {@link Successor}.
     */

    public boolean equals( Object o );

}
