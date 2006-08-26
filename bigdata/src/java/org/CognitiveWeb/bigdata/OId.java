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
 * Created on Jun 19, 2006
 */
package org.CognitiveWeb.bigdata;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A flyweight immutable object identifier (OId). The object identifier is
 * broken down into four components: <em>P</em> artition, <em>S</em> egment,
 * <em>p</em> age, and <em>s</em> lot. Each component takes up 16 bits. The
 * components are aligned on 48, 32, 16, and 0 bit boundaries respectively. The
 * OId may be written as a dotted quad of unsigned hexadecimal short integers
 * formed from these components.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 * 
 * @todo Extract interface?
 */

final public class OId implements Externalizable {

    /**
     * The 64-bit long integer value.
     */
    private long _oid;

    /**
     * 65535 is the maximum value of an unsigned 16-bit integer and the maximum
     * value for each component of the object identifier.
     */
    public static final transient int MAX_USHORT = (1<<16) - 1;

    /**
     * Mask for the partition component of the object identifier.
     */
    public static final transient long PART_MASK = 0xffff000000000000L;
    
    /**
     * Mask for the segment component of the object identifier.
     */
    public static final transient long SEGM_MASK = 0x0000ffff00000000L;
    
    /**
     * Mask for the page component of the object identifier.
     */
    public static final transient long PAGE_MASK = 0x00000000ffff0000L;
    
    /**
     * Mask for the slot component of the object identifier.
     */
    public static final transient long SLOT_MASK = 0x000000000000ffffL;
    
    /**
     * An object identifier corresponding to a NULL reference and having the
     * long integer value of zero (0L).
     */

    public static final transient OId NULL = new OId(0L);
    
    /**
     * Break down the object identifier into an array of component identifiers
     * <code>P.S.p.s</code>.
     * 
     * @param oid
     *            The object identifier.
     * 
     * @return An array of <code>P.S.p.s</code> component identifiers.
     */
    
    static public short[] getComponents(final long oid) {
        return getComponents( oid, new short[4] );
    }

    /**
     * Break down the object identifier into an array of component identifiers.
     * 
     * @param oid
     *            The object identifier.
     * 
     * @param components
     *            The array into which the components will be stored.
     * 
     * @return The array of extracted component identifiers.
     */
    
    static public short[] getComponents(final long oid, final short[] components) {
//        final short[] components = new short[4];
        if( components == null || components.length != 4 ) {
            throw new IllegalArgumentException();
        }
        components[0] = (short) (oid >>> 48);
        components[1] = (short) ((SEGM_MASK & oid) >>> 32);
        components[2] = (short) ((PAGE_MASK & oid) >>> 16);
        components[3] = (short) (SLOT_MASK & oid);
        return components;
    }


    /**
     * Assemble an object identifier out of its component parts.
     * 
     * @param P
     *            The Partition identifier.
     * @param S
     *            The Segment identifier.
     * @param p
     *            The page identifier.
     * @param s
     *            The slot identifier.
     *            
     * @return The object identifier.
     */

    static public long getOid( short P, short S, short p, short s ) {
        /*
         * Note: sign extension when promoting from short to int or long must be
         * explicitly canceled out by masking off the unwanted bits.
         */
        return
            (((long) P) << 48) |
            ((((long) S) << 32) & SEGM_MASK) |
            ((((long) p) << 16) & PAGE_MASK) |
            (((long) s) & SLOT_MASK)
            ;
    }   

    /**
     * Assemble an object identifier out of its component parts.
     * 
     * @param P
     *            The Partition identifier.
     * @param S
     *            The Segment identifier.
     * @param p
     *            The page identifier.
     * @param s
     *            The slot identifier.
     * 
     * @return The object identifier.
     * 
     * @exception IllegalArgumentException
     *                if any component has a value less than zero or greater
     *                than {@link #MAX_USHORT}.
     */

    static public long getOid( int P, int S, int p, int s )
    {

        if (P < 0 || P > MAX_USHORT ) {
            
            throw new IllegalArgumentException("partition="+P);
            
        }

        if (S < 0 || S > MAX_USHORT ) {
            
            throw new IllegalArgumentException("segment="+S);
            
        }

        if (p < 0 || p > MAX_USHORT ) {
            
            throw new IllegalArgumentException("page="+p);
            
        }

        if (s < 0 || s > MAX_USHORT ) {
            
            throw new IllegalArgumentException("slot="+s);
            
        }

        return getOid((short) P, (short) S, (short) p, (short) s);

    }
    
    //
    // Relative OIds.
    //

    
    /**
     * Zeros the leading components in an object identifier that are shared by
     * the specified context. For example, if the <i>context </i> and the <i>oid
     * </i> are on the same page, then the returned value will have a zero (0)
     * for its P.S.p components and a non-zero s component.
     * 
     * @param context
     *            The context is the object identifier of the object being
     *            serialized
     * 
     * @param oid
     *            An absolute object identifier that is contained within the
     *            object being serialized.
     * 
     * @return A relative object identifier in which leading shared components
     *         of the <i>oid </i> have been zeroed. If the <i>oid </i> is zero,
     *         then the result is also zero.
     * 
     * @exception IllegalArgumentException
     *                if the context is zero (0L).
     * 
     * @exception IllegalArgumentException
     *                if any component of the <i>context </i> object identifier
     *                is zero.
     * 
     * @exception IllegalArgumentException
     *                if the <i>oid </i> is non-zero and any component of the
     *                <i>oid </i> object identifier is zero.
     */
    
    static public long getRelativeOid(final long context,final long oid) {
        if( context == 0L ) {
            throw new IllegalArgumentException("context is zero.");
        }
        if ((context & PART_MASK) == 0 || (context & SEGM_MASK) == 0
                || (context & PAGE_MASK) == 0 || (context & SLOT_MASK) == 0) {
            throw new IllegalArgumentException(
                    "object identifier contains zero component: context="
                            + Long.toHexString(context));
        }
        if( oid == 0L ) {
            return 0L;
        }
        if ((oid & PART_MASK) == 0 || (oid & SEGM_MASK) == 0 || (oid & PAGE_MASK) == 0
                || (oid & SLOT_MASK) == 0) {
            throw new IllegalArgumentException(
                    "object identifier contains zero component: oid="
                            + Long.toHexString(oid));
        }
        long oid2 = oid;
        if ((context & PART_MASK) == (oid & PART_MASK)) {
            oid2 &= ~PART_MASK;
            if ((context & SEGM_MASK) == (oid & SEGM_MASK)) {
                oid2 &= ~SEGM_MASK;
                if ((context & PAGE_MASK) == (oid & PAGE_MASK)) {
                    oid2 &= ~PAGE_MASK;
                    /* Note: We can not differentiate a null from a relative oid
                     * if we also make the slot identifier relative!
                     */
//                    if ((context & SLOT_MASK) == (oid & SLOT_MASK)) {
//                        oid2 &= ~SLOT_MASK;
//                    }
                }
            }
        }
        return oid2;
    }

    /**
     * Combines a relative object identifier with the context to obtain an
     * absolute object identifier.
     * 
     * @param contextOid
     *            The context is the object identifier of the object being
     *            deserialized
     * 
     * @param relativeOid
     *            A relative object identifier extracted as a packed long
     *            from the object being deserialized.
     * 
     * @return The absolute object identifier.
     */
    
    static public long getAbsoluteOid(final long contextOid, final long relativeOid) {
        if( relativeOid == 0L ) {
            // A zero(0L) relative oid always codes a null.
            return 0L;
        }
        long oid = relativeOid;
        if ((relativeOid & PART_MASK) == 0) {
            oid |= (contextOid & PART_MASK);
            if ((relativeOid & SEGM_MASK) == 0) {
                oid |= (contextOid & SEGM_MASK);
                if ((relativeOid & PAGE_MASK) == 0) {
                    oid |= (contextOid & PAGE_MASK);
                    /* Note: We can not differentiate a null from a relative oid
                     * if we also make the slot identifier relative!
                     */
//                    if ((relativeOid & SLOT_MASK) == 0) {
//                        oid |= (contextOid & SLOT_MASK);
//                    }
                }
            }
        }
        return oid;
    }

    //
    // Constructors.
    //
    
    /**
     * Create an {@link Oid} from a long integer.
     */
    
    public OId( long oid ) {
        
        _oid = oid;
        
    }

    /**
     * Creates an {@link OId}from its components.
     * 
     * @param P
     *            The partition identifier.
     * @param S
     *            The segment identifier.
     * @param p
     *            The page identifier.
     * @param s
     *            The slot identifier.
     * 
     * @exception IllegalArgumentException
     *                if any component has a value less than zero or greater
     *                than {@link #MAX_USHORT}.
     */
    
    public OId( int P, int S, int p, int s ) {
        
        this(getOid( P, S, p, s));
        
    }

    /**
     * Creates an object identifier from its dotted quad representation.
     * 
     * @param str
     *            An object identifier of the form P.S.p.s, where the partition,
     *            segment, page and slot are hexadecimal values.
     * 
     * @exception IllegalArgumentException
     *                if <i>str </i> is null.
     * @exception IllegalArgumentException
     *                if <i>str </i> does not contain four substrings delimited
     *                by a period ('.').
     * @exception NumberFormatException
     *                if the substrings in <i>str </i> can not be parsed as
     *                hexadecimal values.
     * @exception IllegalArgumentException
     *                if a parsed substring has a value larger than 65535.
     */
    
    public OId( String str ) {
        
        if( str == null ) {
            
            throw new IllegalArgumentException();
            
        }
        
        String[] vals = str.split("\\.",4);
        
        if( vals.length != 4 ) {
            
            throw new IllegalArgumentException("Not a dotted quad.");
            
        }
        
        int P = Integer.parseInt(vals[0],16);
        int S = Integer.parseInt(vals[1],16);
        int p = Integer.parseInt(vals[2],16);
        int s = Integer.parseInt(vals[3],16);

        _oid = getOid( P, S, p, s); // validates values in ushort range.
        
    }
    
    /**
     * Creates an {@link OId}from its components.
     * 
     * @param P
     *            The partition identifier.
     * @param S
     *            The segment identifier.
     * @param p
     *            The page identifier.
     * @param s
     *            The slot identifier.
     * 
     * @exception IllegalArgumentException
     */
    
    public OId( short P, short S, short p, short s ) {
        
        this(getOid(P, S, p, s));
        
    }
    
    public boolean equals( Object o ) {
        
        if( this == o ) return true;
        
        return _oid == ((OId) o)._oid;
        
    }

    /**
     * The hash code as if computed by {@link Long#hashCode()}.
     */
    public int hashCode() {
        return (int)(_oid ^ (_oid >>> 32));
    }
    
    /**
     * Return the object identifier as a 64-bit long integer.
     * 
     * @return The object identifier.
     */
    public long toLong() {
        return _oid;
    }
    
    public boolean isNull() {
        return _oid == 0L;
    }

    /**
     * The partition component of the object identifier as a non-negative
     * integer in (0:65535).
     */
    public int getPartition() {
        return (int) ((_oid & PART_MASK)>>>48);
    }
    
    /**
     * The segment component of the object identifier as a non-negative
     * integer in (0:65535).
     */
    public int getSegment() {
        return (int) ((_oid & SEGM_MASK)>>>32);
    }
    
    /**
     * The page component of the object identifier as a non-negative integer in
     * (0:65535).
     */
    public int getPage() {
        return (int) ((_oid & PAGE_MASK)>>>16);
    }

    /**
     * The page component of the object identifier as a non-negative integer in
     * (0:65535).
     */
    static public int getPage(long oid) {
        return (int) ((oid & PAGE_MASK)>>>16);
    }
    
    /**
     * The slot component of the object identifier as a non-negative integer in
     * (0:65535).
     */
    public int getSlot() {
        return (int) (_oid & SLOT_MASK);
    }
    
    /**
     * The slot component of the object identifier as a non-negative integer in
     * (0:65535).
     */
    public static int getSlot(long oid) {
        return (int) (oid & SLOT_MASK);
    }
    
    /**
     * Masks off the page and slot components to zero. This may be used to
     * compare two object identifiers to determine if they identify the same
     * partition and segment.
     * 
     * @return The partition and segment identifier as a long integer.
     */

    public long maskPageAndSlot() {
        
         return maskPageAndSlot(_oid);
         
    }

    /**
     * Masks off the page and slot components to zero. This may be used to
     * compare two object identifiers to determine if they identify the same
     * partition and segment.
     * 
     * @return The partition and segment identifier as a long integer.
     */

    public static long maskPageAndSlot(long oid) {
       
        long oid2 = oid & ~(PAGE_MASK|SLOT_MASK);
        
        return oid2;
        
    }
    
    /**
     * <p>
     * Represents the object identifier as a dotted quad of hexadecimal
     * components.
     * </p>
     * <p>
     * Note: We explictly mask off the 16 high order bits to remove the impact
     * of sign extension when promoting the short component to an int in order
     * to display its hex value string.
     * </p>
     */
    public String toString() {
        return Integer.toHexString(0x0000ffff & getPartition()) + "."
                + Integer.toHexString(0x0000ffff & getSegment()) + "."
                + Integer.toHexString(0x0000ffff & getPage()) + "."
                + Integer.toHexString(0x0000ffff & getSlot());
    }
    
    //
    // Externalizable
    //
    
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong( _oid );
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _oid = in.readLong();
    }
    
}
