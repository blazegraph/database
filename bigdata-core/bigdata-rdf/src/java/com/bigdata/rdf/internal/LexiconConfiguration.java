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
/*
 * Created July 10, 2010
 */

package com.bigdata.rdf.internal;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.internal.constraints.DateTimeUtility;
import com.bigdata.rdf.internal.constraints.IMathOpHandler;
import com.bigdata.rdf.internal.constraints.MathUtility;
import com.bigdata.rdf.internal.impl.AbstractInlineIV;
import com.bigdata.rdf.internal.impl.bnode.FullyInlineUnicodeBNodeIV;
import com.bigdata.rdf.internal.impl.bnode.NumericBNodeIV;
import com.bigdata.rdf.internal.impl.bnode.UUIDBNodeIV;
import com.bigdata.rdf.internal.impl.extensions.XSDStringExtension;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.literal.IPv4AddrIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.PackedLongIV;
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
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.lexicon.LexiconKeyOrder;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.util.InnerCause;

/**
 * An object which describes which kinds of RDF Values are inlined into the
 * statement indices and how other RDF Values are coded into the lexicon.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class LexiconConfiguration<V extends BigdataValue>
        implements ILexiconConfiguration<V> {

    private static final Logger log =
        Logger.getLogger(LexiconConfiguration.class);

    /**
	 * The maximum character length of an RDF {@link Value} before it will be
	 * inserted into the {@link LexiconKeyOrder#BLOBS} index rather than the
	 * {@link LexiconKeyOrder#TERM2ID} and {@link LexiconKeyOrder#ID2TERM}
	 * indices.
	 * <p>
	 * Note: {@link Integer#MAX_VALUE} disables the BLOBS index.
	 * 
	 * @see AbstractTripleStore.Options#BLOBS_THRESHOLD
	 * @see <a href="https://github.com/SYSTAP/bigdata-gpu/issues/25"> Disable
	 *      BLOBS indexing completely for GPU </a>
	 */
    private final int blobsThreshold;

    private final long MAX_UNSIGNED_BYTE = 1 << 9 - 1;
    private final long MAX_UNSIGNED_SHORT = 1 << 17 -1;
    private final long MAX_UNSIGNED_INT = 1L << 33 - 1;
    private final BigInteger MAX_UNSIGNED_LONG = BigInteger.valueOf(1L << 33)
    	.multiply(BigInteger.valueOf(1L << 33).subtract(BigInteger.valueOf(1)));

	/**
	 * <code>true</code> if xsd primitive and numeric xsd datatype literals will
	 * be inlined.
	 *
	 * @see AbstractTripleStore.Options#INLINE_XSD_DATATYPE_LITERALS
	 */
    private final boolean inlineXSDDatatypeLiterals;

    /**
     * <code>true</code> if geospatial support is enabled
     */
    private final boolean geoSpatial;
    


    /**
     * Optional configuration string for the geospatial facilities.
     */
    private final String geoSpatialConfig;

    
    /**
     * <code>true</code> if textual literals will be inlined.
     *
     * @see AbstractTripleStore.Options#INLINE_TEXT_LITERALS
     */
    final private boolean inlineTextLiterals;

    /**
     * The maximum length of a Unicode string which may be inlined into the
     * statement indices. This applies to blank node IDs, literal labels
     * (including the {@link XSDStringExtension}), local names of {@link URI}s,
     * etc.
     *
     * @see AbstractTripleStore.Options#MAX_INLINE_TEXT_LENGTH
     */
    final private int maxInlineTextLength;

    /**
     * <code>true</code> if (conforming) blank nodes will inlined.
     *
     * @see AbstractTripleStore.Options#INLINE_BNODES
     */
    private final boolean inlineBNodes;

	/**
	 * @see AbstractTripleStore.Options#INLINE_DATE_TIMES
	 */
	private final boolean inlineDateTimes;

    /**
     * @see AbstractTripleStore.Options#INLINE_DATE_TIMES_TIMEZONE
     */
	private final TimeZone inlineDateTimesTimeZone;

	/**
     * @see AbstractTripleStore.Options#REJECT_INVALID_XSD_VALUES
     */
    final boolean rejectInvalidXSDValues;

    /**
     * @see AbstractTripleStore.Options#EXTENSION_FACTORY_CLASS
     */
    private final IExtensionFactory xFactory;

    /**
     * @see AbstractTripleStore.Options#VOCABULARY_CLASS
     */
    private final Vocabulary vocab;

    /**
     * The value factory for the lexicon.
     */
    private final BigdataValueFactory valueFactory;

    /**
     * The inline URI factory for the lexicon.
     */
    private final IInlineURIFactory uriFactory;

    /**
     * Mapping from the {@link IV} for the datatype URI of a registered
     * extension to the {@link IExtension}.
     */
    @SuppressWarnings("rawtypes")
    private final Map<IV, IExtension<? extends BigdataValue>> iv2ext;

    /**
     * Mapping from the string value of the datatype URI for a registered
     * extension to the {@link IExtension}.
     */
    private final Map<String, IExtension<? extends BigdataValue>> datatype2ext;
    
    /**
     * The set of inline datatypes that should be included in the text index 
     * even though they are inline and not normally text indexed.
     */
    private final Set<URI> inlineDatatypesToTextIndex;

    /**
     * The set of registered {@link IMathOpHandler}s.
     * 
     * @see BLZG-1592 (ConcurrentModificationException in MathBOp when using expression in BIND)
     */
    private final ArrayList<IMathOpHandler> typeHandlers = new ArrayList<IMathOpHandler>();
    
    @Override
    public final BigdataValueFactory getValueFactory() {

        return valueFactory;

    }

    @Override
    public int getMaxInlineStringLength() {

        return maxInlineTextLength;

    }

    @Override
    public boolean isInlineTextLiterals() {

        return inlineTextLiterals;

    }

    @Override
    public boolean isInlineLiterals() {

        return inlineXSDDatatypeLiterals;

    }
    
    @Override
    public boolean isGeoSpatial() {
       
       return geoSpatial;
    }

    @Override
    public String getGeoSpatialConfig() {
       
       return geoSpatialConfig;
    }
    
    
    @Override
    public boolean isInlineDateTimes() {
        return inlineDateTimes;
    }

    @Override
    public TimeZone getInlineDateTimesTimeZone() {

        return inlineDateTimesTimeZone;

    }

    @Override
    public int getBlobsThreshold() {

        return blobsThreshold;

    }
    
	@Override
	public boolean isBlobsDisabled() {

		return blobsThreshold == Integer.MAX_VALUE;
    	
    }
    
    @Override
    public boolean isInlineDatatypeToTextIndex(final URI dt) {
        
        return dt != null && inlineDatatypesToTextIndex.contains(dt);
        
    }

    @Override
    public String toString() {

    	final StringBuilder sb = new StringBuilder();

    	sb.append(getClass().getName());

        sb.append("{ "
                + AbstractTripleStore.Options.BLOBS_THRESHOLD
                + "=" + blobsThreshold);

		sb.append(", "
				+ AbstractTripleStore.Options.INLINE_XSD_DATATYPE_LITERALS
				+ "=" + inlineXSDDatatypeLiterals);

		sb.append(", " + AbstractTripleStore.Options.INLINE_TEXT_LITERALS + "="
				+ inlineTextLiterals);

		sb.append(", " + AbstractTripleStore.Options.MAX_INLINE_TEXT_LENGTH+ "="
				+ maxInlineTextLength);

		sb.append(", " + AbstractTripleStore.Options.INLINE_BNODES + "="
				+ inlineBNodes);

		sb.append(", " + AbstractTripleStore.Options.INLINE_DATE_TIMES + "="
				+ inlineDateTimes);

		sb.append(", " + AbstractTripleStore.Options.REJECT_INVALID_XSD_VALUES + "="
				+ rejectInvalidXSDValues);

		sb.append(", " + AbstractTripleStore.Options.EXTENSION_FACTORY_CLASS+ "="
				+ xFactory.getClass().getName());

		sb.append(", " + AbstractTripleStore.Options.VOCABULARY_CLASS + "="
				+ vocab.getClass().getName());

        sb.append(", " + AbstractTripleStore.Options.INLINE_URI_FACTORY_CLASS + "="
                + uriFactory.getClass().getName());

        sb.append(", " + LexiconConfiguration.class.getName() + ".inlineDatatypesToTextIndex="
                + inlineDatatypesToTextIndex);

		sb.append("}");

    	return sb.toString();

    }

    @SuppressWarnings("rawtypes")
    public LexiconConfiguration(//
            final int blobsThreshold,
            final boolean inlineXSDDatatypeLiterals,//
            final boolean inlineTextLiterals,//
            final int maxInlineTextLength,//
            final boolean inlineBNodes,//
            final boolean inlineDateTimes,//
            final TimeZone inlineDateTimesTimeZone,
            final boolean rejectInvalidXSDValues,
            final IExtensionFactory xFactory,//
            final Vocabulary vocab,
            final BigdataValueFactory valueFactory,//
            final IInlineURIFactory uriFactory,//
            final boolean geoSpatial,
            final String geoSpatialConfig
            ) {

        if (blobsThreshold < 0)
            throw new IllegalArgumentException();

        if (maxInlineTextLength < 0)
            throw new IllegalArgumentException();

        if (vocab == null)
            throw new IllegalArgumentException();

        if (valueFactory == null)
            throw new IllegalArgumentException();

        this.blobsThreshold = blobsThreshold;
        this.inlineXSDDatatypeLiterals = inlineXSDDatatypeLiterals;
        this.inlineTextLiterals = inlineTextLiterals;
        this.maxInlineTextLength = maxInlineTextLength;
        this.inlineBNodes = inlineBNodes;
        this.inlineDateTimes = inlineDateTimes;
        this.inlineDateTimesTimeZone = inlineDateTimesTimeZone;
        this.rejectInvalidXSDValues = rejectInvalidXSDValues;
        this.xFactory = xFactory;
        this.vocab = vocab;
        this.valueFactory = valueFactory;
        this.uriFactory = uriFactory;
        this.geoSpatial = geoSpatial;
        this.geoSpatialConfig = geoSpatialConfig;
        
        /*
         * TODO Make this configurable.
         */
        this.inlineDatatypesToTextIndex =
                new LinkedHashSet<URI>(Arrays.asList(new URI[] {
                        XSD.IPV4
                }));

		/*
		 * Note: These collections are read-only so we do NOT need additional
		 * synchronization.
		 */

        iv2ext = new LinkedHashMap<IV, IExtension<? extends BigdataValue>>();

        datatype2ext = new LinkedHashMap<String, IExtension<? extends BigdataValue>>();

    }

    @Override
    @SuppressWarnings("unchecked")
    public void initExtensions(final IDatatypeURIResolver resolver) {

        xFactory.init(resolver, (ILexiconConfiguration<BigdataValue>) this/* config */);

        @SuppressWarnings("rawtypes")
        final Iterator<IExtension<? extends BigdataValue>> itr = xFactory.getExtensions();

        while(itr.hasNext()) {
            
            final IExtension<?> extension = itr.next();
            
//            final BigdataURI datatype = extension.getDatatype();
        	for (BigdataURI datatype : extension.getDatatypes()) {

	            if (datatype == null)
	                continue;

	            if (log.isDebugEnabled()) {
	            	log.debug("adding extension for: " + datatype);
	            }

	            if (iv2ext.containsKey(datatype.getIV())) {
	            	log.warn("multiple IExtension implementations for: " + datatype);
	            }

	            iv2ext.put(datatype.getIV(), extension);

	            datatype2ext.put(datatype.stringValue(), extension);

        	}

        	if (extension instanceof IMathOpHandler) {
        	    typeHandlers.add((IMathOpHandler)extension);
        	}

        }

        typeHandlers.add(new DateTimeUtility());
        typeHandlers.add(new MathUtility());
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public V asValue(final LiteralExtensionIV<?> iv
//            ,final BigdataValueFactory vf
            ) {

        // The datatypeIV for the ExtensionIV.
        final IV datatypeIV = iv.getExtensionIV();

        // Find the IExtension from the datatype IV.
        final IExtension<? extends BigdataValue> ext = iv2ext.get(datatypeIV);

        if (ext == null)
            throw new RuntimeException("Unknown extension: " + datatypeIV);

        return (V) ext.asValue(iv, valueFactory);

    }

    @Override
    @SuppressWarnings("unchecked")
    public V asValueFromVocab(final IV<?, ?> iv) {

        return (V) vocab.asValue(iv);

    }

    @Override
    @SuppressWarnings("rawtypes")
    public IV createInlineIV(final Value value) {

        final IV iv;

        /*
         * Note: The decision to represent the Value as a TermId,
         * URINamespaceIV, or LiteralDatatypeIV is made at a higher level if
         * this method returns [null], indicating that the Value was not
         * inlined.
         */

        if (value instanceof URI) {

            /*
             * Test the Vocabulary to see if this URI was pre-declared. If so,
             * then return the IV from the Vocabulary. It will be either an
             * URIByteIV or an URIShortIV.
             */

            final IV tmp = vocab.get(value);

            if (tmp != null) {

                iv = tmp;

            } else {

                iv = createInlineURIIV((URI) value);

            }

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
    @SuppressWarnings("unchecked")
    private IV<BigdataURI, ?> createInlineURIIV(final URI value) {

// deprecated in favor of the extensible InlineURIFactory mechanism
//    	try {
//
//    		final String s = value.stringValue();
//
//	    	if (s.startsWith("ip:")) {
//	    		return new IPAddrIV(s.substring(3));
//	    	}
//
//    	} catch (UnknownHostException ex) {
//
//    		log.warn("unknown host exception, will not inline: " + value);
//
//    	}

        /*
         * See if there is a handler for inline URIs for this namespace.
         */
        
    	@SuppressWarnings("rawtypes")
        final URIExtensionIV inline = uriFactory.createInlineURIIV(value);
        
        if (inline != null) {

            return inline;

        }

        if (maxInlineTextLength == 0) {

            return null;

        }

        if (value.stringValue().length() <= maxInlineTextLength) {

            return new FullyInlineURIIV<BigdataURI>(value);

        }

        final String localName = ((URI) value).getLocalName();

        if (localName.length() < maxInlineTextLength) {

            final String namespace = ((URI) value).getNamespace();

            final IV<BigdataURI, ?> namespaceIV = vocab.get(new URIImpl(namespace));

            if (namespaceIV != null) {

                final FullyInlineTypedLiteralIV<BigdataLiteral> localNameIV =
                        new FullyInlineTypedLiteralIV<BigdataLiteral>(
                                localName);

                return new URIExtensionIV<BigdataURI>(localNameIV, namespaceIV);

            }

        }

        // URI was not inlined.
        return null;

    }

    /**
     * Inflate the localName portion of an inline URI using its storage delegate.
     * @param namespace the uris's prefix
     * @param delegate the storage delegate
     * @return the inflated localName
     */
    @Override
    public String getInlineURILocalNameFromDelegate(final URI namespace,
            final AbstractLiteralIV<BigdataLiteral, ?> delegate) {
        return uriFactory.getLocalNameFromDelegate(namespace, delegate);
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

        IV<BigdataLiteral, ?> iv = null;

        if (datatype != null && datatype2ext.containsKey(datatype.stringValue())) {

            /*
             * Check the registered extension factories first.
             *
             * Note: optimized xsd:string support is provided via a registered
             * extension. See XSDStringExtension.
             */

            if ((iv = createExtensionIV(value, datatype)) != null)
                return iv;

        }

        /*
         * Attempt to inline an xsd datatype corresponding to either a java
         * primitive (int, long, float, etc.) or to one of the special cases
         * (BigDecimal, BigInteger).
         *
         * Note: Optimized xsd:string inlining is handled by the
         * XSDStringExtension (above).
         */
        if ((iv = createInlineDatatypeIV(value, datatype)) != null)
            return iv;

        if (inlineTextLiterals && maxInlineTextLength > 0) {

            /*
             * Attempt to fully inline the literal.
             */

            if ((iv = createInlineUnicodeLiteral(value)) != null)
                return iv;

        }

        // Literal was not inlined.
        return null;

    }

    /**
     * Test the registered {@link IExtension} handlers. If one handles this
     * datatype, then delegate to that {@link IExtension}.
     *
     * @return The {@link LiteralExtensionIV} -or- <code>null</code> if no
     *         {@link IExtension} was registered for that datatype.
     *
     *         TODO Should we explicitly disallow extensions which override the
     *         basic inlining behavior for xsd datatypes?
     */
    private AbstractInlineIV<BigdataLiteral, ?> createExtensionIV(
            final Literal value, final URI datatype) {

        final IExtension<? extends BigdataValue> xFactory =
            datatype2ext.get(datatype.stringValue());

        try {

            @SuppressWarnings("unchecked")
            final AbstractInlineIV<BigdataLiteral, ?> iv = xFactory
                    .createIV(value);

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
			 * Some sort of parse error in the literal value most likely. By
			 * returning [null], it will be inserted into the lexicon indices
			 * instead of being inlined.
			 */

            log.error(t.getMessage() + ": value=" + value.stringValue());// t);

            // fall through.
            return null;

        }

    }

    /**
     * If the total length of the Unicode components of the {@link Literal} is
     * less than {@link #maxInlineTextLength} then return an fully inline
     * version of the {@link Literal}.
     *
     * @param value
     *            The literal.
     *
     * @return The fully inline IV -or- <code>null</code> if the {@link Literal}
     *         could not be inlined within the configured constraints.
     */
    private AbstractInlineIV<BigdataLiteral, ?> createInlineUnicodeLiteral(
            final Literal value) {

        if (maxInlineTextLength > 0) {

            /*
             * Only check the string length if we are willing to turn this into
             * a fully inline IV.
             */

            final long totalLength = BigdataValueSerializer.getStringLength(value);

            if (totalLength <= maxInlineTextLength) {

                return new FullyInlineTypedLiteralIV<BigdataLiteral>(
                        value.getLabel(), value.getLanguage(),
                        value.getDatatype());

            }

        }

        // Will not inline as Unicode.
        return null;

    }

    /**
	 * Attempt to inline an xsd datatype corresponding to either an intrinsic
	 * datatype, including a java primitive (int, long, float, etc.) or of the
	 * special cases (BigDecimal, BigInteger). Both the {@link DTE} and the
	 * {@link DTEExtension} enums will be checked to see if either one
	 * recognizes the datatype as an intrinsic.
	 *
	 * @param value
	 *            The RDF {@link Value}.
	 * @param datatype
	 *            The XSD datatype {@link URI}.
	 *
	 * @return The {@link IV} -or- <code>null</code> if the value could not be
	 *         inlined.
	 *         
	 * @see BLZG-1507 (Implement support for DTE extension types for URIs)
	 */
    private AbstractInlineIV<BigdataLiteral, ?> createInlineDatatypeIV(
            final Literal value, final URI datatype) {

        // Get the native (intrinsic) DTE.
        final DTE dte = DTE.valueOf(datatype);

// DTE.Extension being used for IPv4 now
//        if (dte == DTE.Extension || dte == null) {
//            /*
//             * Either a registered IExtension datatype or a datatype for which
//             * there is no native DTE support.
//             */
//            return null;
//        }

		final DTEExtension dtex;
		if (dte == null) {
			// Check for an extended intrinsic datatype.
			dtex = DTEExtension.valueOf(datatype);
		} else {
			dtex = null;
		}
        
        if (dte == null && dtex == null) {
			// Not an intrinsic data type. 
            return null;
        }

        // check to see if we are inlining literals of this type
        if (!isInline(VTE.LITERAL, dte))
            return null;

        final String v = value.stringValue();

        try {

			if (dtex != null) {
				/*
				 * Handle an extended intrinsic datatype.
				 */
				switch (dtex) {
				case IPV4:
					/*
					 * Extension for IPv4. Throws UnknownHostException if not
					 * parseable as an IPv4.
					 */
					return new IPv4AddrIV<BigdataLiteral>(v);
                case PACKED_LONG:
				    /*
				     * Extension for packed long value in the range [0;72057594037927935L].
				     */
				    return new PackedLongIV<BigdataLiteral>(v);					
				default:
					// Not handled.
					return null;
				}
        	}

			/*
			 * Handle an intrinsic datatype (but not a DTEExtended intrinsic).
			 */
            switch (dte) {
            case XSDBoolean:
                return new XSDBooleanIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseBoolean(v));
            case XSDByte:
                return new XSDNumericIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseByte(v));
            case XSDShort:
                return new XSDNumericIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseShort(v));
            case XSDInt:
                return new XSDNumericIV<BigdataLiteral>(XMLDatatypeUtil.parseInt(v));
            case XSDLong:
                return new XSDNumericIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseLong(v));
            case XSDFloat:
                return new XSDNumericIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseFloat(v));
            case XSDDouble:
                return new XSDNumericIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseDouble(v));
            case XSDInteger:
                return new XSDIntegerIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseInteger(v));
            case XSDDecimal:
                return new XSDDecimalIV<BigdataLiteral>(XMLDatatypeUtil
                        .parseDecimal(v));
            case UUID:
                return new UUIDLiteralIV<BigdataLiteral>(UUID.fromString(v));
            case XSDUnsignedByte:
                return new XSDUnsignedByteIV<BigdataLiteral>(parseUnsignedByte(v));
            case XSDUnsignedShort:
                return new XSDUnsignedShortIV<BigdataLiteral>(parseUnsignedShort(v));
           case XSDUnsignedInt:
                return new XSDUnsignedIntIV<BigdataLiteral>(parseUnsignedInt(v));
            case XSDUnsignedLong:
				return new XSDUnsignedLongIV<BigdataLiteral>(parseUnsignedLong(v));
			case Extension: 
				// Fall through. Should have been handled above.
            default:
                // Not handled.
                return null;
            }

        } catch (NumberFormatException ex) {

            if (rejectInvalidXSDValues) {

                throw new RuntimeException(ex + ": value=" + v, ex);

            }

            /*
             * Note: By falling through here, we wind up accepting the Value,
             * but it gets handled as a TermId instead of being inlined.
             */

            if (log.isInfoEnabled())
                log.warn("Value does not validate against datatype: " + value);

            return null;

        } catch (UnknownHostException ex) {

            if (rejectInvalidXSDValues) {

                throw new RuntimeException(ex + ": value=" + v, ex);

            }

            /*
             * Note: By falling through here, we wind up accepting the Value,
             * but it gets handled as a TermId instead of being inlined.
             */

            if (log.isInfoEnabled())
                log.warn("Value does not validate against datatype: " + value);

            return null;

        }


    }

    /**
     * The unsigned parse must range check since it uses a parser for a higher value type
     * before casting.
     */
    private byte parseUnsignedByte(final String v) {
    	short pv = XMLDatatypeUtil.parseShort(v);

    	if (pv < 0 || pv > MAX_UNSIGNED_BYTE) {
    		throw new NumberFormatException("Value out of range for unsigned byte");
    	}
    	pv += Byte.MIN_VALUE;

    	return (byte) pv;
    }

    private short parseUnsignedShort(final String v) {
    	int pv = XMLDatatypeUtil.parseInt(v);

    	if (pv < 0 || pv > MAX_UNSIGNED_SHORT) {
    		throw new NumberFormatException("Value out of range for unsigned short");
    	}

    	pv += Short.MIN_VALUE;

    	return (short) pv;
    }

    private int parseUnsignedInt(final String v) {
    	long pv = XMLDatatypeUtil.parseLong(v);

    	if (pv < 0 || pv > MAX_UNSIGNED_INT) {
    		throw new NumberFormatException("Value out of range for unsigned int");
    	}

    	pv += Integer.MIN_VALUE;

    	return (int) pv;
    }

    private long parseUnsignedLong(final String v) {
    	final BigInteger pv = XMLDatatypeUtil.parseInteger(v);

    	if (pv.signum() == -1 || pv.compareTo(MAX_UNSIGNED_LONG) > 0) {
    		throw new NumberFormatException("Value out of range for unsigned long");
    	}

    	return pv.subtract(BigInteger.valueOf(Long.MIN_VALUE)).longValue();
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

        /*
         * Note: UUID.toString() is 36 bytes. If it has the 'u' prefix we can
         * recognize it, so that is a total of 37 bytes.
         */
        if (c == 'u' &&
                id.length() == 37 && isInline(VTE.BNODE, DTE.UUID)) {

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

        if (maxInlineTextLength > 0 && id.length() <= maxInlineTextLength) {

            /*
             * Inline as [Unicode].
             */

            return new FullyInlineUnicodeBNodeIV<BigdataBNode>(id);

        }

        // The blank node was not inlined.
        return null;

    }

    /**
     * Return <code>true</code> iff the {@link VTE} / {@link DTE} combination
     * will be inlined within the statement indices using native inlining
     * mechanisms (not {@link IExtension} handlers) based solely on the
     * consideration of the {@link VTE} and {@link DTE} (the length of the
     * {@link Value} is not considered).
     */
    private boolean isInline(final VTE vte, final DTE dte) {

        switch (vte) {
	        case STATEMENT:
	            return true;
            case BNODE:
                return inlineBNodes;// && isSupported(dte);
            case LITERAL:
                return inlineXSDDatatypeLiterals;// && isSupported(dte);
            default:
                return false;
        }

    }

//    /**
//     * Hack for supported {@link DTE}s (this is here because we do not support
//     * the unsigned variants yet).
//     *
//     * @param dte
//     *            The {@link DTE}.
//     *
//     * @return <code>true</code> if the {@link DTE} has native inline support
//     *         (versus support via an {@link IExtension} handler or inline
//     *         support via a {@link FullyInlineTypedLiteralIV} (a catch all)).
//     */
//    private boolean isSupported(final DTE dte) {
//
//        switch (dte) {
//            case XSDBoolean:
//            case XSDByte:
//            case XSDShort:
//            case XSDInt:
//            case XSDLong:
//            case XSDFloat:
//            case XSDDouble:
//            case XSDInteger:
//            case XSDDecimal:
//            case UUID:
//            case Extension:  
//                return true;
//            case XSDString:
//            /*
//             * Note: xsd:string is handled as a registered extension by
//             * XSDStringExtension. This method reports [false] for xsd:string
//             * because this method returns [true] only for datatypes handled
//             * outside of the extension mechanism.
//             */
//                return true;
//            case XSDUnsignedByte:
//            case XSDUnsignedShort:
//            case XSDUnsignedInt:
//            case XSDUnsignedLong:
//                return true; // enable supporting XSD unsigned types!
//            default:
//                throw new AssertionError();
//        }
//
//    }

    @Override
    public Iterable<IMathOpHandler> getTypeHandlers() {

    	return Collections.unmodifiableList(typeHandlers);
    	
    }

}
