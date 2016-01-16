/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.rdf.internal;

import org.openrdf.model.URI;

import com.bigdata.rdf.internal.impl.AbstractIV;
import com.bigdata.rdf.internal.impl.literal.PackedLongIV;

/**
 * An extension of the intrinsic types defined by {@link DTE}.
 * 
 * @author bryan
 * 
 * @see BLZG-1507 (Implement support for DTE extension types for URIs)
 * 
 * TODO add a variable length nibble encoding of a byte[]?
 */
public enum DTEExtension {
	
	/**
	 * An IPV4 address. 
	 */
	IPV4((byte) 0, 0/* len */, IPv4Address.class, XSD.IPV4, DTEFlags.NOFLAGS),
    
    /**
     * A packed long value, restricted to the range [0;72057594037927935L].
     * Note that this is not the full range of long (negative values are not
     * supported and positive long values in [72057594037927936L;Long.MAX]
     * are not supported), the reason being that the compression technique
     * we're using is order preserving only for the valid range.
     */
    PACKED_LONG((byte) 1, 0/* len */, Long.class, PackedLongIV.PACKED_LONG, DTEFlags.NOFLAGS),
    
    /**
     * An array of inline IVs.
     */
    ARRAY((byte) 2, 0/* len */, Void.class, null/*datatypeURI*/, DTEFlags.NOFLAGS),

    /**
     * An mocked IV (used internally).
     */
    MOCKED_IV((byte) 3, 0/* len */, Void.class, null/*datatypeURI*/, DTEFlags.NOFLAGS),

    
	/**
	 * This is a place holder for extension of the intrinsic data types. Its
	 * code corresponds to 0xff, which is to say all four bits are on. When this
	 * code is used, the next byte(s) must be examined to determine the actual
	 * intrinsic data type.
	 * <p>
	 * Note: This is NOT the same as the {@link AbstractIV#isExtension()} bit.
	 * The latter <strong>always</strong> indicates that an {@link IV} follows
	 * the <code>flags</code> byte and indicates the actual datatype URI. In
	 * contrast, {@link #RESERVED} is a placeholder that could give us another
	 * byte to handle additional "intrinsic" types.
	 * 
	 * @see BLZG-1507 (Implement support for DTE extension types for URIs)
	 * 
	 *      TODO If we wind up extending things again, it would perhaps be nicer
	 *      to have an extension mechanism for non-intrinsic data types (ones
	 *      that can be registered without having to explicitly create them in
	 *      the code).
	 */
	RESERVED((byte) 255, 0/* len */, Void.class, null/*datatypeURI*/, DTEFlags.NOFLAGS);

    /**
     * @param v
     *            The code for the data type.
     * @param len
     *            The length of the inline value -or- ZERO (0) if the value has
     *            a variable length (xsd:integer, xsd:decimal).
     * @param cls
     *            The class of the Java object used to represent instances of
     *            the coded data type. (The inline object.)
     * @param datatype
     *            The well-known URI for the data type.
     * @param flags
     *            Some bit flags. See {@link #NUMERIC},
     *            {@link #UNSIGNED_NUMERIC}, etc.
     */
    private DTEExtension(final byte v, final int len, final Class<?> cls,
            final URI datatypeURI, final int flags) {
        this.v = v;
        this.len = len;
        this.cls = cls;
        this.datatypeURI = datatypeURI;
        this.flags = flags;
    }

    static final public DTEExtension valueOf(final byte b) {
        /*
         * Note: This switch MUST correspond to the declarations above (you can
         * not made the cases of the switch from [v] since it is not considered
         * a to be constant by the compiler).
         */
        switch (b) {
        case 0:
            return IPV4;
        case 1:
            return PACKED_LONG;            
        case 2:
            return ARRAY;
        case 3:
            return MOCKED_IV;
         default:
            throw new IllegalArgumentException(Byte.toString(b));
        }
    }

    /**
	 * Return the {@link DTEExtension} for the datatype {@link URI}.
	 * 
	 * @param datatype
	 *            The datatype {@link URI}.
	 * 
	 * @return The {@link DTEException} for that datatype -or- <code>null</code>
	 *         if the datatype {@link URI} is none of the datatypes for which
	 *         native support is provided.
	 */
    static final public DTEExtension valueOf(final URI datatype) {
        /*
         * Note: This switch MUST correspond to the declarations above (you can
         * not make the cases of the switch from [v] since it is not considered
         * a to be constant by the compiler).
         * 
         * TODO Optimize using trie, weighted frequency lookup tree, hash map,
         * etc. Also, the match will always be on the local name once we proof
         * the namespace.
         */
        
        if (datatype == null) {
            return null;
        }
        
	if (datatype.equals(IPV4.datatypeURI))
			return IPV4;
        if (datatype.equals(PACKED_LONG.datatypeURI))
            return PACKED_LONG;        

		/*
         * Not a known DTE datatype.
         */
        return null;
    }

    /**
     * The code for the data type.
     */
    final byte v;

    /**
     * The length of the inline value -or- ZERO (0) if the value has a variable
     * length (xsd:integer, xsd:decimal).
     */
    private final int len;

    /**
     * The class of the Java object used to represent instances of the coded
     * data type.  (The inline object.)
     */
    private final Class<?> cls;

    /**
     * The well-known URI for the data type.
     */
    private final URI datatypeURI;

    /**
     * Some bit flags.
     */
    private final int flags;

    /**
     * An <code>byte</code> value whose whose lower 6 bits code the
     * {@link DTE}.
     */
    final public byte v() {
        return v;
    }

    /**
     * The length of the data type value when represented as a component in an
     * unsigned byte[] key -or- ZERO iff the key component has a variable length
     * for that data type.
     */
    final public int len() {

        return len;
        
    }

    /**
     * The class of the Java object used to represent instances of the coded
     * data type.
     */
    final public Class<?> getCls() {
     
        return cls;
        
    }

    /**
     * The corresponding datatype {@link URI}.
     */
    final public URI getDatatypeURI() {

        return datatypeURI;
        
    }

}
