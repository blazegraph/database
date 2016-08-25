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
/* Portions Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
/*
 * Created on May 3, 2010
 */

package com.bigdata.rdf.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.LongPacker;
import com.bigdata.rdf.internal.impl.AbstractIV;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.bnode.FullyInlineUnicodeBNodeIV;
import com.bigdata.rdf.internal.impl.bnode.NumericBNodeIV;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.internal.impl.bnode.UUIDBNodeIV;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.literal.IPv4AddrIV;
import com.bigdata.rdf.internal.impl.literal.LiteralArrayIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.MockedValueIV;
import com.bigdata.rdf.internal.impl.literal.PackedLongIV;
import com.bigdata.rdf.internal.impl.literal.PartlyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.literal.UUIDLiteralIV;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.internal.impl.literal.XSDDecimalIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedByteIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedIntIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedLongIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedShortIV;
import com.bigdata.rdf.internal.impl.uri.FullyInlineURIIV;
import com.bigdata.rdf.internal.impl.uri.PartlyInlineURIIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.internal.impl.uri.VocabURIByteIV;
import com.bigdata.rdf.internal.impl.uri.VocabURIShortIV;
import com.bigdata.rdf.lexicon.BlobsIndexHelper;
import com.bigdata.rdf.lexicon.ITermIndexCodes;
import com.bigdata.rdf.lexicon.TermIdEncoder;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOKeyOrder;

/**
 * Helper class for {@link IV}s.
 * 
 * @openrdf
 */
/*
 * Note: There are a huge number of warnings in this class, all of which are
 * related to related to IV type parameters. I've taken the liberty to suppress
 * them all.
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class IVUtility {

//    private static final transient Logger log = Logger.getLogger(IVUtility.class);

    /**
     * When <code>true</code>, we will pack term identifiers using
     * {@link LongPacker}.
     * <p>
     * Note: This option requires that term identifiers are non-negative. That
     * is not currently true for the cluster due to the {@link TermIdEncoder}.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-654">
     *      Pack TIDs</a>
     */
    public static final boolean PACK_TIDS = false;
    
    public static boolean equals(final IV iv1, final IV iv2) {
        
        // same IV or both null
        if (iv1 == iv2) {
            return true;
        }
        
        // one of them is null
        if (iv1 == null || iv2 == null) {
            return false;
        }
        
        // only possibility left if that neither are null
        return iv1.equals(iv2);
        
    }
    
    /**
     * This provides a dumb comparison across IVs.
     */
    public static int compare(final IV iv1, final IV iv2) {
        
        // same IV or both null
        if (iv1 == iv2)
            return 0;
        
        // one of them is null
        if (iv1 == null)
            return -1;
        
        if (iv2 == null)
            return 1;
        
        // only possibility left if that neither are null
        return iv1.compareTo(iv2);
        
    }
    
    /**
     * Encode an RDF value into a key for one of the statement indices.  Handles
     * null {@link IV} references gracefully.
     * 
     * @param keyBuilder
     *            The key builder.
     * @param iv
     *            The internal value (can be <code>null</code>).
     * 
     * @return The key builder.
     */
    public static IKeyBuilder encode(final IKeyBuilder keyBuilder, final IV iv) {

        if (iv == null) {

            TermId.NullIV.encode(keyBuilder);
            
        } else {
            
            iv.encode(keyBuilder);
            
        }

        return keyBuilder;
        
    }
    
    /**
     * Decode an {@link IV} from a byte[].
     * 
     * @param key
     *            The byte[].
     *            
     * @return The {@link IV}.
     */
    public static IV decode(final byte[] key) {

        return decodeFromOffset(key, 0);
        
    }
    
    /**
     * Decodes up to numTerms {@link IV}s from a byte[].
     * 
     * @param key
     *            The byte[].
     * @param numTerms
     *            The number of terms to decode.
     *            
     * @return The set of {@link IV}s.
     */
    public static IV[] decode(final byte[] key, final int numTerms) {

        return decode(key, 0 /* offset */, numTerms);
        
    }
    
    /**
     * Decodes up to numTerms {@link IV}s from a byte[].
     * 
     * @param key
     *            The byte[].
     * @param offset
     *            The offset into the byte[] key.
     * @param numTerms
     *            The number of terms to decode.
     *            
     * @return The set of {@link IV}s.
     */
    public static IV[] decode(final byte[] key, final int offset, 
            final int numTerms) {
            
        if (numTerms <= 0)
            return new IV[0];
        
        final IV[] ivs = new IV[numTerms];
        
        int o = offset;
        
        for (int i = 0; i < numTerms; i++) {

            if (o >= key.length)
                throw new IllegalArgumentException(
                        "key is not long enough to decode " 
                        + numTerms + " terms.");
            
            ivs[i] = decodeFromOffset(key, o);
            
            o += ivs[i] == null 
                    ? TermId.NullIV.byteLength() : ivs[i].byteLength();
            
        }

        return ivs;
        
    }
    
    /**
     * Decodes all {@link IV}s from a byte[].
     * 
     * @param key
     *            The byte[].
     *            
     * @return The set of {@link IV}s.
     */
    public static IV[] decodeAll(final byte[] key) {

        return decodeAll(key, 0/* off */, key.length/* len */);

    }

    /**
     * Decodes {@link IV}s from a slice of a byte[].
     * 
     * @param key
     *            The byte[].
     * @param off
     *            The offset of the first encoded {@link IV} in the byte[].
     * @param len
     *            The #of bytes of valid data to be decoded starting at that
     *            offset.
     * 
     * @return The {@link IV}s in the order in which they were decoded.
     */
    public static IV[] decodeAll(final byte[] key, int off, final int len) {

        if (key == null)
            throw new IllegalArgumentException();

        if (off < 0)
            throw new IllegalArgumentException();

        final int limit = off + len;

        if (len < 0 || limit > key.length)
            throw new IllegalArgumentException();

        final List<IV> ivs = new LinkedList<IV>();

        while (off < limit) {

            final IV iv = decodeFromOffset(key, off);

            ivs.add(iv);

            off += iv == null ? TermId.NullIV.byteLength() : iv.byteLength();

        }

        return ivs.toArray(new IV[ivs.size()]);

    }

    /**
     * Decode one {@link IV}.
     * 
     * @param key
     *            The unsigned byte[] key.
     * @param offset
     *            The offset.
     *            
     * @return The {@link IV} decoded from that offset.
     */
    public static IV decodeFromOffset(final byte[] key, final int offset) {
        
        return decodeFromOffset(key, offset, true/*nullIsNullRef*/);
        
    }
    
    /**
     * Decode one {@link IV}.
     * 
     * @param key
     *            The unsigned byte[] key.
     * @param offset
     *            The offset.
     * @param nullIsNullRef
     *            When <code>true</code> a <code>termId:=0L</code> {@link IV} is
     *            decoded as a <code>null</code> reference. Otherwise it is
     *            decoded using {@link TermId#mockIV(VTE)}.
     * 
     * @return The {@link IV} decoded from that offset.
     */
    public static IV decodeFromOffset(final byte[] key, final int offset,
            final boolean nullIsNullRef) {

        int o = offset;
        
        final byte flags = KeyBuilder.decodeByte(key[o++]);

        /*
         * Handle an IV which is not 100% inline.
         */
        if (!AbstractIV.isInline(flags)) {

            if (AbstractIV.isExtension(flags)) {

                /*
                 * Handle non-inline URI or Literal. 
                 */

                final byte extensionByte = KeyBuilder.decodeByte(key[o++]); 

                if (extensionByte < 0) {

                    // Decode the extension IV.
                    final IV extensionIV = IVUtility.decodeFromOffset(key, o);

                    // skip over the extension IV.
                    o += extensionIV.byteLength();

                    // Decode the inline component.
                    final AbstractLiteralIV delegate = (AbstractLiteralIV) 
                    		IVUtility.decodeFromOffset(key, o);

                    // TODO Should really be switch((int)extensionByte).
                    switch (AbstractIV.getInternalValueTypeEnum(flags)) {
                    case URI:
                        return new PartlyInlineURIIV<BigdataURI>(delegate,
                                extensionIV);
                    case LITERAL:
                        return new PartlyInlineTypedLiteralIV<BigdataLiteral>(delegate,
                                extensionIV);
                    default:
                        throw new AssertionError();
                    }
                    
                } else {
                    
                    /*
                     * Handle a BlobIV.
                     * 
                     * Note: This MUST be consistent with
                     * TermsIndexHelper#makeKey() and BlobIV.
                     */

                    final int hashCode = KeyBuilder.decodeInt(key, o);
                    
                    o += BlobsIndexHelper.SIZEOF_HASH;
                    
                    final short counter = KeyBuilder.decodeShort(key, o);
                    
                    o += BlobsIndexHelper.SIZEOF_COUNTER;
                    
                    final BlobIV<?> iv = new BlobIV(flags, hashCode, counter);

                    return iv;
                    
                }
                
            } else {

                /*
                 * Handle a TermId, including a NullIV.
                 */ 
                
                // decode the term identifier.
                final long termId;
                if(PACK_TIDS) {
                    termId = LongPacker.unpackLong(key, o);
                } else {
                    termId = KeyBuilder.decodeLong(key, o);
                }

                if (termId == TermId.NULL) {
                    if(nullIsNullRef) {
                        return null;
                    }
                    // Return a "mock" IV consistent with the VTE flags. 
                    // See BLZG-2051 SolutionSetStream incorrectly decodes VTE of MockIVs
                    return TermId.mockIV(AbstractIV.getInternalValueTypeEnum(flags));
//                    return TermId.mockIV(VTE.valueOf(flags));
                } else {
                    return new TermId(flags, termId);
                }
                
            }

        }

        /*
         * Handle an inline value. 
         */

        // The value type (URI, Literal, BNode, SID)
        final VTE vte = AbstractIV.getInternalValueTypeEnum(flags);

        switch (vte) {
        case STATEMENT: {
            /*
             * Handle inline sids.
             */
            // spo is directly decodable from key
            final ISPO spo = SPOKeyOrder.SPO.decodeKey(key, o);
            // all spos that have a sid are explicit
            spo.setStatementType(StatementEnum.Explicit);
//            spo.setStatementIdentifier(true);
            // create a sid iv and return it
            return new SidIV(spo);
        }
        case BNODE:
            return decodeInlineBNode(flags, key, o);
        case URI:
            return decodeInlineURI(flags, key, o);
        case LITERAL:
            return decodeInlineLiteral(flags, key, o);
        default:
            throw new AssertionError();
        }
        
    }

    /**
     * Decode an inline blank node from an offset.
     * 
     * @param flags
     *            The flags.
     * @param key
     *            The key.
     * @param o
     *            The offset.
     *            
     * @return The decoded {@link IV}.
     */
    static private IV decodeInlineBNode(final byte flags, final byte[] key,
            final int o) {
        
        // The data type
        final DTE dte = AbstractIV.getDTE(flags);
        switch (dte) {
        case XSDInt: {
            final int x = KeyBuilder.decodeInt(key, o);
            return new NumericBNodeIV<BigdataBNode>(x);
        }
        case UUID: {
            final UUID x = KeyBuilder.decodeUUID(key, o);
            return new UUIDBNodeIV<BigdataBNode>(x);
        }
        case XSDString: {
            // decode buffer.
            final StringBuilder sb = new StringBuilder();
            // inline string value
            final String str1;
            // #of bytes read.
            final int nbytes;
            try {
                nbytes = IVUnicode.decode(new ByteArrayInputStream(key, o,
                        key.length - o), sb);
                str1 = sb.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new FullyInlineUnicodeBNodeIV<BigdataBNode>(str1,
                    1/* flags */+ nbytes);
        }
        default:
            throw new UnsupportedOperationException("dte=" + dte);
        }
    }

    /**
     * Decode an inline URI from a byte offset.
     * 
     * @param flags
     *            The flags byte.
     * @param key
     *            The key.
     * @param o
     *            The offset.
     * 
     * @return The decoded {@link IV}.
     */
    static private IV decodeInlineURI(final byte flags, final byte[] key,
            int o) {

        if(AbstractIV.isExtension(flags)) {
            
            final IV namespaceIV = decodeFromOffset(key,o);
            
            o += namespaceIV.byteLength();
            
            final AbstractLiteralIV<BigdataLiteral, ?> localNameIV = 
                    (AbstractLiteralIV<BigdataLiteral, ?>) decodeFromOffset(
                            key, o);
            
            final IV iv = new URIExtensionIV<BigdataURI>(localNameIV,
                    namespaceIV);
            
            return iv;
            
        }
        
        // The data type
        final DTE dte = AbstractIV.getDTE(flags);
        switch (dte) {
// deprecated in favor of the extensible InlineURIFactory
//        case XSDBoolean: {
//        	/*
//        	 * TODO Using XSDBoolean so that we can know how to decode this thing
//           * as an IPAddrIV.  We need to fix the Extension mechanism for URIs.
//           * Extension is already used above. 
//        	 */
//        	final byte[] addr = new byte[5];
//        	System.arraycopy(key, o, addr, 0, 5);
//            final IPv4Address ip = new IPv4Address(addr);
//            return new IPv4AddrIV(ip);
//        }
        case XSDByte: {
            final byte x = key[o];//KeyBuilder.decodeByte(key[o]);
            return new VocabURIByteIV<BigdataURI>(x);
        }
        case XSDShort: {
            final short x = KeyBuilder.decodeShort(key, o);
            return new VocabURIShortIV<BigdataURI>(x);
        }
        case XSDString: {
            // decode buffer.
            final StringBuilder sb = new StringBuilder();
            // inline string value
            final String str1;
            // #of bytes read.
            final int nbytes;
            try {
                nbytes = IVUnicode.decode(new ByteArrayInputStream(key, o, key.length
                        - o), sb);
                str1 = sb.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new FullyInlineURIIV<BigdataURI>(new URIImpl(str1),
                    1/* flags */+ nbytes);
        }
        default:
            throw new UnsupportedOperationException("dte=" + dte);
        }

    }

    /**
     * Decode an inline literal from an offset.
     * 
     * @param flags
     *            The flags byte.
     * @param key
     *            The key.
     * @param o
     *            The offset.
     */
    static private IV decodeInlineLiteral(final byte flags, final byte[] key,
            int o) {

        // The data type
        final DTE dte = AbstractIV.getDTE(flags);
        final DTEExtension dtex;
        if (dte == DTE.Extension) {
            /*
             * @see BLZG-1507 (Implement support for DTE extension types for
             * URIs)
             * 
             * @see BLZG-1595 ( DTEExtension for compressed timestamp)
             */
            // The DTEExtension byte.
            dtex = DTEExtension.valueOf(key[o++]);
//            dtex = DTEExtension.valueOf(KeyBuilder.decodeByte(key[o++]));
        } else dtex = null;

        final boolean isExtension = AbstractIV.isExtension(flags);
        
        final IV datatype; 
        if (isExtension) {
            datatype = decodeFromOffset(key, o);
            o += datatype.byteLength();
        } else {
            datatype = null;
        }

        switch (dte) {
        case XSDBoolean: {
            final byte x = KeyBuilder.decodeByte(key[o]);
            final boolean isTrue = (x != 0);
            final AbstractLiteralIV iv = XSDBooleanIV.valueOf(isTrue);
//            final AbstractLiteralIV iv = (x == 0) ? 
//                    XSDBooleanIV.FALSE : XSDBooleanIV.TRUE;
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDByte: {
            final byte x = KeyBuilder.decodeByte(key[o]);
            final AbstractLiteralIV iv = new XSDNumericIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDShort: {
            final short x = KeyBuilder.decodeShort(key, o);
            final AbstractLiteralIV iv = new XSDNumericIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDInt: {
            final int x = KeyBuilder.decodeInt(key, o);
            final AbstractLiteralIV iv = new XSDNumericIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv;
        }
        case XSDLong: {
            final long x = KeyBuilder.decodeLong(key, o);
            final AbstractLiteralIV iv = new XSDNumericIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDFloat: {
            final float x = KeyBuilder.decodeFloat(key, o);
            final AbstractLiteralIV iv = new XSDNumericIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDDouble: {
            final double x = KeyBuilder.decodeDouble(key, o);
            final AbstractLiteralIV iv = new XSDNumericIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDInteger: {
            final BigInteger x = KeyBuilder.decodeBigInteger(o, key);
            final AbstractLiteralIV iv = new XSDIntegerIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDDecimal: {
            final BigDecimal x = KeyBuilder.decodeBigDecimal(o, key);
            final AbstractLiteralIV iv = new XSDDecimalIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case UUID: {
            final UUID x = KeyBuilder.decodeUUID(key, o);
            final AbstractLiteralIV iv = new UUIDLiteralIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv;
        }
        case XSDUnsignedByte: {
            final byte x = KeyBuilder.decodeByte(key[o]);
            final AbstractLiteralIV iv = new XSDUnsignedByteIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDUnsignedShort: {
            final short x = KeyBuilder.decodeShort(key, o);
            final AbstractLiteralIV iv = new XSDUnsignedShortIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDUnsignedInt: {
            final int x = KeyBuilder.decodeInt(key, o);
            final AbstractLiteralIV iv = new XSDUnsignedIntIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv;
        }
        case XSDUnsignedLong: {
            final long x = KeyBuilder.decodeLong(key, o);
            final AbstractLiteralIV iv = new XSDUnsignedLongIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDString: {
            if(isExtension) {
            // decode the termCode
            final byte termCode = key[o++];
            assert termCode == ITermIndexCodes.TERM_CODE_LIT : "termCode="
                    + termCode;
            // decode buffer.
            final StringBuilder sb = new StringBuilder();
            // inline string value
            final String str1;
            // #of bytes read.
            final int nread;
            try {
                nread = IVUnicode.decode(new ByteArrayInputStream(key, o, key.length
                        - o), sb);
                str1 = sb.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            // Note: The 'delegate' will be an InlineLiteralIV w/o a datatype.
            final FullyInlineTypedLiteralIV<BigdataLiteral> iv = new FullyInlineTypedLiteralIV<BigdataLiteral>(
                    str1, null/* languageCode */, null/* datatype */,
                    1/* flags */+ 1/* termCode */+ nread);
            return isExtension ? new LiteralExtensionIV<BigdataLiteral>(iv, datatype)
                    : iv;
            }
            return decodeInlineUnicodeLiteral(key,o);
        }
        case Extension: {
        /*
         * Handle an extension of the intrinsic data types.
         * 
         * @see BLZG-1507 (Implement support for DTE extension types for URIs)
         * @see BLZG-1595 (DTEExtension for compressed timestamp)
         * @see BLZG-611 (MockValue problems in hash join)
         */
		switch (dtex) {
    		case IPV4: {
    			final byte[] addr = new byte[5];
    			System.arraycopy(key, o, addr, 0, 5);
    			final IPv4Address ip = new IPv4Address(addr);
    			final AbstractLiteralIV iv = new IPv4AddrIV(ip);
    			return isExtension ? new LiteralExtensionIV(iv, datatype) : iv;
    		}
            case PACKED_LONG: {
                final AbstractLiteralIV iv = new PackedLongIV<>(LongPacker.unpackLong(key, o));
                return isExtension ? new LiteralExtensionIV<>(iv, datatype) : iv;
            }
            case MOCKED_IV: {
                return new MockedValueIV(decodeFromOffset(key,o));
            }
            case ARRAY: {
                // byte(0...255) --> int(1...256) 
                final int n = ((int) key[o++] & 0xFF) + 1;
                final IV[] ivs = decode(key, o, n);
                final InlineLiteralIV[] args = new InlineLiteralIV[n];
                for (int i = 0; i < n; i++) {
                    if (ivs[i] instanceof InlineLiteralIV) {
                        args[i] = (InlineLiteralIV) ivs[i];
                    } else {
                        throw new UnsupportedOperationException("InlineArrayIV only supports InlineLiteralIV delegates");
                    }
                }
                final LiteralArrayIV iv = new LiteralArrayIV(args);
                return isExtension ? new LiteralExtensionIV(iv, datatype) : iv;
            }
		default: {
			throw new UnsupportedOperationException("dte=" + dte);
		}
		}
//			return decodeInlineLiteralWithDTEExtension(flags, key, dte, isExtension, datatype, o);
        }
        default:
            throw new UnsupportedOperationException("dte=" + dte);
        }

	}

//	/**
//	 * Handle an extension of the intrinsic data types.
//	 * 
//	 * @see BLZG-1507 (Implement support for DTE extension types for URIs)
//	 */
//	private static IV decodeInlineLiteralWithDTEExtension(final byte flags, final byte[] key, final DTE dte,
//			final boolean isExtension, final IV datatype, int o) {
//
//		// The DTEExtension byte.
//		final DTEExtension dtex = DTEExtension.valueOf(key[o++]);
////		final DTEExtension dtex = DTEExtension.valueOf(KeyBuilder.decodeByte(key[o++]));
//
//		switch (dtex) {
//		case IPV4: {
//			final byte[] addr = new byte[5];
//			System.arraycopy(key, o, addr, 0, 5);
//			final IPv4Address ip = new IPv4Address(addr);
//			final AbstractLiteralIV iv = new IPv4AddrIV(ip);
//			return isExtension ? new LiteralExtensionIV(iv, datatype) : iv;
//		}
//		case PACKED_LONG: 
//		{
//		    final AbstractLiteralIV iv = 
//		        new PackedLongIV<>(LongPacker.unpackLong(key, 0));
//		        return isExtension ? new LiteralExtensionIV<>(iv, datatype) : iv;
//		}		
//		default: {
//			throw new UnsupportedOperationException("dte=" + dte);
//		}
//		}
//	}

	/**
     * Decode an inline literal which is represented as a one or two compressed
     * Unicode values.
     * 
     * @param key
     *            The key.
     * @param offset
     *            The offset into the key.
     *            
     * @return The decoded {@link IV}.
     */
    static private FullyInlineTypedLiteralIV<BigdataLiteral> decodeInlineUnicodeLiteral(
            final byte[] key, final int offset) {

        int o = offset;

        /*
         * Fully inline literal.
         */

        // decode the termCode
        final byte termCode = key[o++];
        // figure out the #of string values which were inlined.
        final int nstrings;
        final String str1, str2; 
        switch (termCode) {
        case ITermIndexCodes.TERM_CODE_LIT:
            nstrings = 1;
            break;
        case ITermIndexCodes.TERM_CODE_LCL:
            nstrings = 2;
            break;
        case ITermIndexCodes.TERM_CODE_DTL:
            nstrings = 2;
            break;
        default:
            throw new AssertionError("termCode=" + termCode);
        }
        // #of bytes read (not including the flags and termCode).
        int nread = 0;
        // decode buffer.
        final StringBuilder sb = new StringBuilder();
        // first inline string value
        try {
            final int nbytes = IVUnicode.decode(new ByteArrayInputStream(key, o, key.length
                    - o), sb);
            str1 = sb.toString();
            nread += nbytes;
            o += nbytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // second inline string value
        if (nstrings == 2) {
            sb.setLength(0); // reset buffer.
            try {
                final int nbytes = IVUnicode.decode(new ByteArrayInputStream(key,
                        o, key.length - o), sb);
                str2 = sb.toString();
                nread += nbytes;
                o += nbytes;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            str2 = null;
        }
        final int byteLength = 1/* flags */+ 1/* termCode */+ nread;
        final FullyInlineTypedLiteralIV<BigdataLiteral> iv;
        switch (termCode) {
        case ITermIndexCodes.TERM_CODE_LIT:
            iv = new FullyInlineTypedLiteralIV<BigdataLiteral>(//
                    str1,//
                    null, // language
                    null, // datatype
                    byteLength//
                    );
            break;
        case ITermIndexCodes.TERM_CODE_LCL:
            iv = new FullyInlineTypedLiteralIV<BigdataLiteral>(//
                    str2,//
                    str1, // language
                    null, // datatype
                    byteLength//
                    );
            break;
        case ITermIndexCodes.TERM_CODE_DTL:
            iv = new FullyInlineTypedLiteralIV<BigdataLiteral>(//
                    str2,//
                    null, // language
                    new URIImpl(str1), // datatype
                    byteLength//
                    );
            break;
        default:
            throw new AssertionError("termCode=" + termCode);
        }
        return iv;
    }

//    /**
//     * Decode an IV from its string representation as encoded by
//     * {@link BlobIV#toString()} and {@link AbstractInlineIV#toString()} (this
//     * is used by the prototype IRIS integration.)
//     * 
//     * @param s
//     *            the string representation
//     * @return the IV
//     */
//    public static final IV fromString(final String s) {
//        if (s.startsWith("TermIV")) {
//            return TermId.fromString(s);
//        } else if (s.startsWith("BlobIV")) {
//                return BlobIV.fromString(s);
//        } else {
//            final String type = s.substring(0, s.indexOf('(')); 
//            final String val = s.substring(s.indexOf('('), s.length()-1);
//            return decode(val, type); // Note, that decode() is moved
//				// into ASTDeferredIVResolutionInitializer
//				// as it is not used anywhere else
//        }
//    }

}
