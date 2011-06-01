/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
/*
 * Created July 10, 2010
 */

package com.bigdata.rdf.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.util.InnerCause;

/**
 * An object which describes which kinds of RDF Values are inlined into the
 * statement indices and how other RDF Values are coded into the lexicon.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Unit tests for inlining behaviors for all valid configurations,
 *          including verification that the {@link IV} was cached on the
 *          caller's {@link BigdataValue}
 */
public class LexiconConfiguration<V extends BigdataValue> 
        implements ILexiconConfiguration<V> {

    private static final Logger log = 
        Logger.getLogger(LexiconConfiguration.class);
    
    private final boolean inlineLiterals, inlineBNodes;

    /**
     * The maximum length of a Unicode string which may be inlined into the
     * statement indices. This applies to blank node IDs, literal labels
     * (including the {@link XSDStringExtension}), local names of {@link URI}s,
     * etc.
     */
    final private int maxInlineStringLength;

    private final IExtensionFactory xFactory;
    
    private final Map<TermId, IExtension<BigdataValue>> termIds;

    private final Map<String, IExtension<BigdataValue>> datatypes;

    /**
     * Return the maximum length of a Unicode string which may be inlined into
     * the statement indices. This applies to blank node IDs, literal labels
     * (including the {@link XSDStringExtension}), local names of {@link URI}s,
     * etc.
     */
    public int getMaxInlineStringLength() {

        return maxInlineStringLength;
        
    }
    
    public LexiconConfiguration(final boolean inlineLiterals,
            final int maxInlineStringLength, final boolean inlineBNodes,
            final boolean inlineDateTimes, final IExtensionFactory xFactory) {
        
        if (maxInlineStringLength < 0)
            throw new IllegalArgumentException();
        
        this.inlineLiterals = inlineLiterals;
        this.maxInlineStringLength = maxInlineStringLength;
        this.inlineBNodes = inlineBNodes;
//        this.inlineDateTimes = inlineDateTimes;
        this.xFactory = xFactory;
        
        termIds = new HashMap<TermId, IExtension<BigdataValue>>();
        datatypes = new HashMap<String, IExtension<BigdataValue>>();

    }

    public void initExtensions(final LexiconRelation lex) {

        xFactory.init(lex);

        for (IExtension<BigdataValue> extension : xFactory.getExtensions()) {

            final BigdataURI datatype = extension.getDatatype();

            if (datatype == null)
                continue;

            termIds.put((TermId) datatype.getIV(), extension);

            datatypes.put(datatype.stringValue(), extension);

        }

    }

    @SuppressWarnings("unchecked")
    public V asValue(final ExtensionIV iv, final BigdataValueFactory vf) {

        // The TermId for the ExtensionIV.
        final TermId datatypeIV = iv.getExtensionIV();

        // Find the IExtension from the datatype IV.
        final IExtension<BigdataValue> ext = termIds.get(datatypeIV);

        if (ext == null)
            throw new RuntimeException("Unknown extension: " + datatypeIV);

        return (V) ext.asValue(iv, vf);

    }

    @SuppressWarnings("unchecked")
    public IV createInlineIV(final Value value) {

        final IV iv;

        /*
         * Note: The decision to represent the Value as a TermId,
         * URINamespaceIV, or LiteralDatatypeIV is made at a higher level if
         * this method returns [null], indicating that the Value was not
         * inlined.
         */

        if (value instanceof URI) {

            iv = createInlineURIIV((URI) value);

        } else if (value instanceof Literal) {

            iv = createInlineLiteralIV((Literal) value);

        } else if (value instanceof BNode) {

            iv = createInlineBNodeIV((BNode) value);

        } else {
            
            // Note: SIDs are handled elsewhere.
            iv = null;
            
        }

        if (iv != null && value instanceof BigdataValue) {
         
            // Cache the IV on the BigdataValue.
            ((BigdataValue) value).setIV(iv);
            
        }
       
        return iv;

    }

    /**
     * If the {@link URI} can be inlined into the statement indices for this
     * {@link LexiconConfiguration}, then return its inline {@link IV}.
     * 
     * @param value
     *            The {@link URI}.
     *            
     * @return The inline {@link IV} -or- <code>null</code> if the {@link URI}
     *         can not be inlined into the statement indices.
     */
    private IV<BigdataURI, ?> createInlineURIIV(final URI value) {

        if (value.stringValue().length() <= maxInlineStringLength) {

            return new InlineURIIV<BigdataURI>(value);

        }

        // URI was not inlined.
        return null;

    }

    /**
     * If the {@link Literal} can be inlined into the statement indices for this
     * {@link LexiconConfiguration}, then return its inline {@link IV}.
     * 
     * @param value
     *            The {@link Literal}.
     * 
     * @return The inline {@link IV} -or- <code>null</code> if the
     *         {@link Literal} can not be inlined into the statement indices.
     */
    private IV<BigdataLiteral,?> createInlineLiteralIV(final Literal value) {

        final URI datatype = value.getDatatype();

        if (datatype != null && datatypes.containsKey(datatype.stringValue())) {

            /*
             * Check the registered extension factories first.
             * 
             * Note: optimized xsd:string support is provided via a registered
             * extension. See XSDStringExtension.
             * 
             * TODO Should we explicitly disallow extensions which override the
             * basic inlining behavior for xsd datatypes?
             */

            final IExtension<BigdataValue> xFactory = 
                datatypes.get(datatype.stringValue());

            try {

                @SuppressWarnings("unchecked")
                final IV<BigdataLiteral, ?> iv = xFactory.createIV(value);

                return iv;

            } catch (Throwable t) {

                if(InnerCause.isInnerCause(t, InterruptedException.class)) {

                    // Propagate interrupt.
                    throw new RuntimeException(t);
                    
                }
                
                if(InnerCause.isInnerCause(t, Error.class)) {

                    // Propagate error to preserve a stable encoding behavior.
                    throw new Error(t);

                }

                /*
                 * Some sort of parse error in the literal value most likely.
                 */
                
                log.error(t.getMessage() + ": value=" + value.stringValue(), t);

                // fall through.
                
            }

        }

        /*
         * Attempt to inline an xsd datatype corresponding to either a java
         * primitive (int, long, float, etc.) or to one of the special cases
         * (BigDecimal, BigInteger).
         * 
         * Note: Optimized xsd:string inlining is handled by the
         * XSDStringExtension (above).
         */
        IV<BigdataLiteral, ?> iv = createInlineDatatypeIV(value, datatype);

        if (iv != null)
            return iv;

        /*
         * Attempt to fully inline the literal.
         */

        final String label = value.getLabel();

        final int datatypeLength = value.getDatatype() == null ? 0 : value
                .getDatatype().stringValue().length();

        final int languageLength = value.getLanguage() == null ? 0 : value
                .getLanguage().length();

        final long totalLength = label.length() + datatypeLength
                + languageLength;

        if (totalLength <= maxInlineStringLength) {

            return new InlineLiteralIV<BigdataLiteral>(label, value
                    .getLanguage(), value.getDatatype());

        }

        // Literal was not inlined.
        return null;

    }

    /**
     * Attempt to inline an xsd datatype corresponding to either a java
     * primitive (int, long, float, etc.) or to one of the special cases
     * (BigDecimal, BigInteger).
     * 
     * @param value
     *            The RDF {@link Value}.
     * @param datatype
     *            The XSD datatype {@link URI}.
     *            
     * @return The {@link IV} -or- <code>null</code> if the value could not be
     *         inlined.
     */
    private AbstractInlineIV<BigdataLiteral, ?> createInlineDatatypeIV(
            final Literal value, final URI datatype) {

        // get the native DTE
        final DTE dte = DTE.valueOf(datatype);

        if (dte == DTE.Extension || dte == null) {
            /*
             * Either a registered IExtension datatype or a datatype for which
             * there is no native DTE support.
             */
            return null;
        }

        // check to see if we are inlining literals of this type
        if (!isInline(VTE.LITERAL, dte))
            return null;

        final String v = value.stringValue();

        try {

            switch (dte) {
            case XSDBoolean:
                return new XSDBooleanIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseBoolean(v));
            case XSDByte:
                return new XSDByteIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseByte(v));
            case XSDShort:
                return new XSDShortIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseShort(v));
            case XSDInt:
                return new XSDIntIV<BigdataLiteral>(XMLDatatypeUtil.parseInt(v));
            case XSDLong:
                return new XSDLongIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseLong(v));
            case XSDFloat:
                return new XSDFloatIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseFloat(v));
            case XSDDouble:
                return new XSDDoubleIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseDouble(v));
            case XSDInteger:
                return new XSDIntegerIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseInteger(v));
            case XSDDecimal:
                return new XSDDecimalIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseDecimal(v));
            case UUID:
                return new UUIDLiteralIV<BigdataLiteral>(UUID.fromString(v));
            default:
                // Not handled.
                return null;
            }

        } catch (NumberFormatException ex) {

            /*
             * Note: This winds up accepting the Value, but it gets handled as a
             * TermId instead of being inlined.
             * 
             * TODO Should we reject the Value instead since it does not
             * validate against the xsd schema datatype?
             */

            log.error(ex + ": value=" + v);

            return null;

        }

    }
    
    /**
     * If the {@link BNode} can be inlined into the statement indices for this
     * {@link LexiconConfiguration}, then return its inline {@link IV}.
     * 
     * @param value
     *            The {@link BNode}.
     * 
     * @return The inline {@link IV} -or- <code>null</code> if the {@link BNode}
     *         can not be inlined into the statement indices.
     */
    private IV<BigdataBNode, ?> createInlineBNodeIV(final BNode value) {

        final String id = value.getID();

        final char c = id.charAt(0);

        // Note: UUID is 16 bytes. If it has the 'u' prefix we can recognize it.
        if (c == 'u' && id.length() == 17 && isInline(VTE.BNODE, DTE.UUID)) {

            /*
             * Inline as [UUID].
             * 
             * Note: We cannot normalize IDs, they need to remain syntactically
             * identical.
             */

            try {

                final String subStr = id.substring(1);
                
                final UUID uuid = UUID.fromString(subStr);

                if (uuid.toString().equals(subStr)) {

                    return new UUIDBNodeIV<BigdataBNode>(uuid);
                    
                }

            } catch (Exception ex) {

                /*
                 * String id could not be converted to a UUID. Fall through.
                 */

            }
            
        } else if (c == 'i' && isInline(VTE.BNODE, DTE.XSDInt)) {

            /*
             * Inline as [int].
             * 
             * Note: We cannot normalize IDs, they need to remain syntactically
             * identical.
             */
            
            try {

                final String subStr = id.substring(1);

                final Integer i = Integer.valueOf(subStr);

                if (i.toString().equals(subStr)) {

                    return new NumericBNodeIV<BigdataBNode>(i);

                }

            } catch (Exception ex) {

                /*
                 * String id could not be converted to an Integer. Fall
                 * through.
                 */

            }
            
        }
        
        if (maxInlineStringLength > 0 && id.length() <= maxInlineStringLength) {

            /*
             * Inline as [Unicode].
             */

            return new UnicodeBNodeIV<BigdataBNode>(id);

        }

        // The blank node was not inlined.
        return null;
        
    }

    /**
     * Return <code>true</code> iff the {@link VTE} / {@link DTE} combination
     * will be inlined within the statement indices using native inlining
     * mechanims (not {@link IExtension} handlers) based solely on the
     * consideration of the {@link VTE} and {@link DTE} (the length of the
     * {@link Value} is not considered).
     */
    private boolean isInline(final VTE vte, final DTE dte) {

        switch (vte) {
	        case STATEMENT:
	            return true;
            case BNODE:
                return inlineBNodes && isSupported(dte);
            case LITERAL:
                return inlineLiterals && isSupported(dte);
            default:
                return false;
        }

    }

    /**
     * Hack for supported {@link DTE}s (this is here because we do not support
     * the unsigned variants yet).
     * 
     * @param dte
     *            The {@link DTE}.
     * 
     * @return <code>true</code> if the {@link DTE} has native inline support
     *         (versus support via an {@link IExtension} handler or inline
     *         support via a {@link InlineLiteralIV} (a catch all)).
     */
    private boolean isSupported(final DTE dte) {

        switch (dte) {
            case XSDBoolean:
            case XSDByte:
            case XSDShort:
            case XSDInt:
            case XSDLong:
            case XSDFloat:
            case XSDDouble:
            case XSDInteger:
            case XSDDecimal:
            case UUID:
                return true;
            case XSDString:
            /*
             * Note: xsd:string is handled as a registered extension by
             * XSDStringExtension. This method reports [false] for xsd:string
             * because this method returns [true] only for datatypes handled
             * outside of the extension mechanism.
             */
                return true;
            case XSDUnsignedByte:
            case XSDUnsignedShort: 
            case XSDUnsignedInt:
            case XSDUnsignedLong:
            /*
             * None of the unsigned datatypes are inlined yet.
             */
                return false;
            default:
                throw new AssertionError();
        }

    }

}
