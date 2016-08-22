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
/* Portions of this code are:
 *
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
/*
 * Created on Aug 24, 2011
 */

package com.bigdata.rdf.sail.sparql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.MalformedQueryException;

import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.LexiconConfiguration;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.literal.UUIDLiteralIV;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.internal.impl.literal.XSDDecimalIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedByteIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedIntIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedLongIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedShortIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.sail.BigdataValueReplacer;
import com.bigdata.rdf.sail.sparql.ast.ASTBlankNode;
import com.bigdata.rdf.sail.sparql.ast.ASTDatasetClause;
import com.bigdata.rdf.sail.sparql.ast.ASTFalse;
import com.bigdata.rdf.sail.sparql.ast.ASTIRI;
import com.bigdata.rdf.sail.sparql.ast.ASTNumericLiteral;
import com.bigdata.rdf.sail.sparql.ast.ASTOperationContainer;
import com.bigdata.rdf.sail.sparql.ast.ASTQName;
import com.bigdata.rdf.sail.sparql.ast.ASTRDFLiteral;
import com.bigdata.rdf.sail.sparql.ast.ASTRDFValue;
import com.bigdata.rdf.sail.sparql.ast.ASTString;
import com.bigdata.rdf.sail.sparql.ast.ASTTrue;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;
import com.bigdata.rdf.store.BD;

/**
 * Visits the AST model and builds a map from each RDF {@link Value} to
 * {@link BigdataValue} objects that have mock IVs assigned to them.
 * <p>
 * Note: The {@link PrefixDeclProcessor} will rewrite {@link ASTQName} nodes as
 * {@link ASTIRI} nodes. It MUST run before this processor.
 * <p>
 * Note: Any {@link ASTRDFLiteral} or {@link ASTIRI} nodes are annotated by this
 * processor using {@link ASTRDFValue#setRDFValue(Value)}. This includes IRIrefs
 * in the {@link ASTDatasetClause}, which are matched as either {@link ASTIRI}
 * or {@link ASTQName}.
 * <p>
 * Note: This is a part of deferred IV batch resolution, which is intended to
 * replace the functionality of the {@link BigdataValueReplacer}.
 * <p>
 * Note: {@link IValueExpression} nodes used in {@link SPARQLConstraint}s are
 * allowed to use values not actually in the database. MP
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @openrdf
 * 
 * @see https://jira.blazegraph.com/browse/BLZG-1176 (decouple SPARQL parser
 *      from DB)
 * @see https://jira.blazegraph.com/browse/BLZG-1519 (Refactor test suite to
 *      remove tight coupling with IVs while checking up parsed queries)
 */
public class ASTDeferredIVResolutionInitializer extends ASTVisitorBase {

    private final static Logger log = Logger
            .getLogger(ASTDeferredIVResolutionInitializer.class);

    private final static boolean INFO = log.isInfoEnabled();

    private final static List<URI> RDF_VOCAB = Arrays.asList(RDF.FIRST, RDF.REST, RDF.NIL, BD.VIRTUAL_GRAPH);

    private final Map<Value, BigdataValue> vocab;

    private final BigdataValueFactory valueFactory;

    private final LinkedHashMap<ASTRDFValue, BigdataValue> nodes;
    
    /**
	 * Return a map from openrdf {@link Value} objects to the corresponding
	 * {@link BigdataValue} objects for all {@link Value}s that appear in the
	 * parse tree.
	 */
    public Map<Value, BigdataValue> getValues() {

    	return vocab;
    	
    }

    public ASTDeferredIVResolutionInitializer() {

        // Unnamed BigdataValueFactory is used to provide instances
        // of BigdataValue, which are required by existing test suite.
        // See also task https://jira.blazegraph.com/browse/BLZG-1519
//        this.valueFactory = BigdataValueFactoryImpl.getInstance("parser"+UUID.randomUUID().toString().replaceAll("-", ""));
        this.valueFactory = new BigdataValueFactoryImpl();
        
        this.nodes = new LinkedHashMap<>();
        
        this.vocab = new LinkedHashMap<>();

    }

    /**
     * Visit the parse tree, locating and collecting references to all
     * {@link ASTRDFValue} nodes (including blank nodes iff we are in a told
     * bnodes mode). The {@link ASTRDFValue}s are collected in a {@link Map}
     * which associates each one with a {@link BigdataValue} object which is set
     * using {@link ASTRDFValue#setRDFValue(org.openrdf.model.Value)}. The
     * {@link BigdataValue}s will be resolved later (in ASTDeferredIVResolution)
     * in a batch against the database, obtaining their {@link IVs}.
     * Until then {@link BigdataValue}s in the parse tree have unresolved
     * {@link IV}s (TermID(0)).  
     * 
     * @param qc
     * 
     * @throws MalformedQueryException
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void process(final ASTOperationContainer qc)
            throws MalformedQueryException {
        
        try {

            /*
             * Collect all ASTRDFValue nodes into a map, paired with
             * BigdataValue objects.
             */
            qc.jjtAccept(new RDFValueResolver(), null);
            
        } catch (final VisitorException e) {
            
            // Turn the exception into a Query exception.
            throw new MalformedQueryException(e);
            
        }

        {
            
            /*
             * RDF Values actually appearing in the parse tree.
             */
            final Iterator<Entry<ASTRDFValue, BigdataValue>> itr = nodes.entrySet().iterator();

            while (itr.hasNext()) {
            
                final Entry<ASTRDFValue, BigdataValue> entry = itr.next();

                final ASTRDFValue value = entry.getKey();
                
                IV iv = null;
                BigdataValue bigdataValue = null;
                if (value.getRDFValue()!=null && ((BigdataValue)value.getRDFValue()).getIV() != null) {
                    bigdataValue = (BigdataValue) value.getRDFValue();
                    iv = bigdataValue.getIV();
                } else if (value instanceof ASTIRI) {
                    iv = new TermId<BigdataValue>(VTE.URI,0);
                    bigdataValue = valueFactory.createURI(((ASTIRI)value).getValue());
                    if (!bigdataValue.isRealIV()) {
                    	bigdataValue.clearInternalValue();
                    	bigdataValue.setIV(iv);
                    }
                    iv.setValue(bigdataValue);
                } else if (value instanceof ASTRDFLiteral) {
                    final ASTRDFLiteral rdfNode = (ASTRDFLiteral) value;
                    final String lang = rdfNode.getLang();
                    final ASTIRI dataTypeIri = rdfNode.getDatatype();
                    URIImpl dataTypeUri = null;
                    DTE dte = null;
                    if (dataTypeIri!=null && dataTypeIri.getValue()!=null) {
                        dataTypeUri = new URIImpl(dataTypeIri.getValue());
                        dte = DTE.valueOf(dataTypeUri);
                    }
                    if (dte!=null) {
                        bigdataValue = getBigdataValue(rdfNode.getLabel().getValue(), dte);
                        if (!bigdataValue.stringValue().equals(rdfNode.getLabel().getValue())) {
                            // Data loss could occur if inline IV will be used, as string representation of original value differ from decoded value
                            bigdataValue = valueFactory.createLiteral(rdfNode.getLabel().getValue(), dataTypeUri);
                            iv = TermId.mockIV(VTE.valueOf(bigdataValue));
                            bigdataValue.setIV(iv);
                            iv.setValue(bigdataValue);
                        }
                    } else { 
                        iv = new TermId<BigdataValue>(VTE.LITERAL,0);
                        if (lang!=null) {
                            bigdataValue = valueFactory.createLiteral(rdfNode.getLabel().getValue(), lang);
                        } else {
                            bigdataValue = valueFactory.createLiteral(rdfNode.getLabel().getValue(), dataTypeUri);
                        }
                        iv.setValue(bigdataValue);
                        bigdataValue.setIV(iv);
                    }
                } else if (value instanceof ASTNumericLiteral) {
                    final ASTNumericLiteral rdfNode = (ASTNumericLiteral) value;
                    final URI dataTypeUri = rdfNode.getDatatype();
                    final DTE dte = DTE.valueOf(dataTypeUri);
                    bigdataValue = getBigdataValue(rdfNode.getValue(), dte);
                    if (!bigdataValue.stringValue().equals(rdfNode.getValue())) {
                        // Data loss could occur if inline IV will be used, as string representation of original value differ from decoded value
//                        iv = bigdataValue.getIV();
                        bigdataValue = valueFactory.createLiteral(rdfNode.getValue(), dataTypeUri);
//                        bigdataValue.setIV(iv);
                    }
                } else if (value instanceof ASTTrue) {
                    bigdataValue = valueFactory.createLiteral(true);
                    if (bigdataValue.isRealIV()) {
                        iv = bigdataValue.getIV();
                    } else {
                        iv = TermId.mockIV(VTE.valueOf(bigdataValue));
                        iv.setValue(bigdataValue);
                        bigdataValue.setIV(iv);
                    }
                } else if (value instanceof ASTFalse) {
                    bigdataValue = valueFactory.createLiteral(false);
                    if (bigdataValue.isRealIV()) {
                        iv = bigdataValue.getIV();
                    } else {
                        iv = TermId.mockIV(VTE.valueOf(bigdataValue));
                        iv.setValue(bigdataValue);
                        bigdataValue.setIV(iv);
                    }
                } else {
                    iv = new FullyInlineTypedLiteralIV<BigdataLiteral>(value.toString(), true);
                    bigdataValue = iv.getValue();
                }

                if (bigdataValue!=null) {
                    value.setRDFValue(bigdataValue);
                    // filling in a dummy IV for BigdataExprBuilder
                    // @see https://jira.blazegraph.com/browse/BLZG-1717 (IV not resolved)
                    fillInDummyIV(bigdataValue);
                    vocab.put(bigdataValue, bigdataValue);
                }
                
            }
            
        }

        /*
         * FIXME Why is this [vocab] still here? And why the IV assignment logic
         * if we are not doing any batch resolution?
         */
        
        // RDF Collection syntactic sugar vocabulary items.
        for (Value value: RDF_VOCAB) {
            BigdataValue bigdataValue = valueFactory.asValue(value);
            fillInDummyIV(bigdataValue);
            vocab.put(value, bigdataValue);
        }

    }

    /*
     * Note: Batch resolution the BigdataValue objects against the database
     * DOES NOT happen here. It will be done in ASTDeferredIVResolution.
     * Mock IVs used until then.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void fillInDummyIV(BigdataValue value) {
        final IV iv = value.getIV();

        if (iv == null) {

            /*
             * Since the term identifier is NULL this value is not known
             * to the kb.
             */

            if (INFO)
                log.info("Not in knowledge base: " + value);

            /*
             * Create a dummy iv and cache the unknown value on it so
             * that it can be used during query evaluation.
             */
            final IV dummyIV = TermId.mockIV(VTE.valueOf(value));

            value.setIV(dummyIV);

            dummyIV.setValue(value);

        } else {

            iv.setValue(value);

        }
    }

    /**
     * Reconstructs BigdataValue out of IV, creating literals if needed
     * <p>
     * {@link IVUtility#decode(String, String)} is used by
     * {@link ASTDeferredIVResolutionInitializer} to convert parsed AST
     * objects (ASTRDFLiteral and ASTNumericalLiteral) to IVs wrapped up as
     * BigdataValues, which are required on later stages of processing.
     * <p>
     * There's no LexiconRelation available at this point, so all values
     * converted in inlined mode. {@link ASTDeferredIVResolution} converts these
     * inlined IVs to term IV by getLexiconRelation().addTerms in case if triple
     * store configured to not use inlined values.
     * 
     * @param iv
     *            the IV
     * 
     * @param dte
     *            data type of IV
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private BigdataValue getBigdataValue(final String value, final DTE dte) {
    	// Check if lexical form is empty, and provide bigdata value
    	// with FullyInlineTypedLiteralIV holding corresponding data type
    	// @see https://jira.blazegraph.com/browse/BLZG-1716 (SPARQL Update parser fails on invalid numeric literals)
    	if (value.isEmpty()) {
    		BigdataLiteral bigdataValue = valueFactory.createLiteral(value, dte.getDatatypeURI());
    		IV iv = new FullyInlineTypedLiteralIV<BigdataLiteral>("", null, dte.getDatatypeURI(), true);
			bigdataValue.setIV(iv);
			iv.setValue(bigdataValue);
			return bigdataValue;
    	}
        final IV iv = decode(value, dte.name());
        BigdataValue bigdataValue;
        if (!iv.hasValue() && iv instanceof AbstractLiteralIV) {
            switch(dte) {
            case XSDByte:
                bigdataValue = valueFactory.createLiteral(((AbstractLiteralIV)iv).byteValue());
                break;
            case XSDShort:
                bigdataValue = valueFactory.createLiteral(((AbstractLiteralIV)iv).shortValue());
                break;
            case XSDInt:
                bigdataValue = valueFactory.createLiteral(((AbstractLiteralIV)iv).intValue());
                break;
            case XSDLong:
                bigdataValue = valueFactory.createLiteral(((AbstractLiteralIV)iv).longValue());
                break;
            case XSDFloat:
                bigdataValue = valueFactory.createLiteral(((AbstractLiteralIV)iv).floatValue());
                break;
            case XSDDouble:
                bigdataValue = valueFactory.createLiteral(((AbstractLiteralIV)iv).doubleValue());
                break;
            case XSDBoolean:
                bigdataValue = valueFactory.createLiteral(((AbstractLiteralIV)iv).booleanValue());
                break;
            case XSDString:
                bigdataValue = valueFactory.createLiteral(((AbstractLiteralIV)iv).stringValue(), dte.getDatatypeURI());
                break;
            case XSDInteger:
                bigdataValue = valueFactory.createLiteral(((AbstractLiteralIV)iv).stringValue(), XMLSchema.INTEGER);
                break;
            case XSDDecimal:
                bigdataValue = valueFactory.createLiteral(iv.stringValue(), DTE.XSDDecimal.getDatatypeURI());
                break;
            case XSDUnsignedShort:
                bigdataValue = valueFactory.createLiteral(iv.stringValue(), DTE.XSDUnsignedShort.getDatatypeURI());
                break;
            case XSDUnsignedInt:
                bigdataValue = valueFactory.createLiteral(iv.stringValue(), DTE.XSDUnsignedInt.getDatatypeURI());
                break;
            case XSDUnsignedByte:
                bigdataValue = valueFactory.createLiteral(iv.stringValue(), DTE.XSDUnsignedByte.getDatatypeURI());
                break;
            case XSDUnsignedLong:
                bigdataValue = valueFactory.createLiteral(iv.stringValue(), DTE.XSDUnsignedLong.getDatatypeURI());
                break;
            default:
                throw new RuntimeException("unknown DTE " + dte);
            }
            bigdataValue.setIV(iv);
            iv.setValue(bigdataValue);
        } else {
            bigdataValue = iv.getValue();
        }
        return bigdataValue;
    }

    /**
     * FIXME Should this be using the {@link LexiconConfiguration} to create
     * appropriate inline {@link IV}s when and where appropriate?
     */
    private class RDFValueResolver extends ASTVisitorBase {

        @Override
        public Object visit(final ASTQName node, final Object data)
                throws VisitorException {

            throw new VisitorException(
                    "QNames must be resolved before resolving RDF Values");

        }

        /**
         * Note: Blank nodes within a QUERY are treated as anonymous variables,
         * even when we are in a told bnodes mode.
         */
        @Override
        public Object visit(final ASTBlankNode node, final Object data)
                throws VisitorException {
            
            throw new VisitorException(
                    "Blank nodes must be replaced with variables before resolving RDF Values");
            
        }

        @Override
        public Void visit(final ASTIRI node, final Object data)
                throws VisitorException {

            try {

                nodes.put(node, valueFactory.createURI(node.getValue()));

                return null;

            } catch (final IllegalArgumentException e) {

                // invalid URI
                throw new VisitorException(e.getMessage());

            }

        }

        @Override
        public Void visit(final ASTRDFLiteral node, final Object data)
                throws VisitorException {

            // Note: This is handled by this ASTVisitor (see below in this
            // class).
            final String label = (String) node.getLabel().jjtAccept(this, null);

            final String lang = node.getLang();

            final ASTIRI datatypeNode = node.getDatatype();

            final BigdataLiteral literal;

            if (datatypeNode != null) {

                final URI datatype;

                try {

                    datatype = valueFactory.createURI(datatypeNode.getValue());

                } catch (final IllegalArgumentException e) {

                    // invalid URI
                    throw new VisitorException(e);

                }

                literal = valueFactory.createLiteral(label, datatype);

            } else if (lang != null) {

                literal = valueFactory.createLiteral(label, lang);

            } else {

                literal = valueFactory.createLiteral(label);

            }

            nodes.put(node, literal);

            return null;

        }

        @Override
        public Void visit(final ASTNumericLiteral node, final Object data)
                throws VisitorException {

            nodes.put(
                    node,
                    valueFactory.createLiteral(node.getValue(),
                            node.getDatatype()));

            return null;

        }

        @Override
        public Void visit(final ASTTrue node, final Object data)
                throws VisitorException {

            nodes.put(node, valueFactory.createLiteral(true));

            return null;

        }

        @Override
        public Void visit(final ASTFalse node, final Object data)
                throws VisitorException {

            nodes.put(node, valueFactory.createLiteral(false));

            return null;

        }

        /**
         * Note: This supports the visitor method for a Literal.
         */
        @Override
        public String visit(final ASTString node, final Object data)
                throws VisitorException {

            return node.getValue();

        }

    }

    /**
     * Decode an IV from its string representation and type, provided in as
     * ASTRDFLiteral node in AST model.
     * <p>
     * Note: This is a very special case method. Normally logic should go
     * through the ILexiconRelation to resolve inline IVs. This always uses
     * inline IVs, and thus defeats the ILexiconConfiguration for the namespace.
     * 
     * @param val
     *            the string representation
     * @param type
     *            value type
     * @return the IV
     * 
     * @see https://jira.blazegraph.com/browse/BLZG-1176 (SPARQL QUERY/UPDATE should not use db connection)
     * 
     * This method was moved from IVUtility class, as it is not used anywhere except
     * AST Deferred resolution
     */
    @SuppressWarnings("rawtypes")
	public static IV decode(final String val, final String type) {
        final DTE dte = Enum.valueOf(DTE.class, type);
        switch (dte) {
        case XSDBoolean: {
            return XSDBooleanIV.valueOf((Boolean.valueOf(val)));
        }
        case XSDByte: {
            final byte x = Byte.valueOf(val);
            return new XSDNumericIV<BigdataLiteral>(x);
        }
        case XSDShort: {
            final short x = Short.valueOf(val);
            return new XSDNumericIV<BigdataLiteral>(x);
        }
        case XSDInt: {
            final int x = Integer.valueOf(val);
            return new XSDNumericIV<BigdataLiteral>(x);
        }
        case XSDLong: {
            final long x = Long.valueOf(val);
            return new XSDNumericIV<BigdataLiteral>(x);
        }
        case XSDFloat: {
            final float x = Float.valueOf(val);
            return new XSDNumericIV<BigdataLiteral>(x);
        }
        case XSDDouble: {
            final double x = Double.valueOf(val);
            return new XSDNumericIV<BigdataLiteral>(x);
        }
        case UUID: {
            final UUID x = UUID.fromString(val);
            return new UUIDLiteralIV<BigdataLiteral>(x);
        }
        case XSDInteger: {
            final BigInteger x = new BigInteger(val);
            return new XSDIntegerIV<BigdataLiteral>(x);
        }
        case XSDDecimal: {
            final BigDecimal x = new BigDecimal(val);
            return new XSDDecimalIV<BigdataLiteral>(x);
        }
        case XSDString: {
            return new FullyInlineTypedLiteralIV(val, null, XMLSchema.STRING, true);
        }
        case XSDUnsignedByte: {
            return new XSDUnsignedByteIV<>((byte) (Byte.valueOf(val) + Byte.MIN_VALUE));
        }
        case XSDUnsignedShort: {
            return new XSDUnsignedShortIV<>((short) (Short.valueOf(val) + Short.MIN_VALUE));
        }
        case XSDUnsignedInt: {
            return new XSDUnsignedIntIV((int) (Integer.valueOf(val) + Integer.MIN_VALUE));
        }
        case XSDUnsignedLong: {
            return new XSDUnsignedLongIV<>(Long.valueOf(val) + Long.MIN_VALUE);
        }
        default:
            throw new UnsupportedOperationException("dte=" + dte);
        }
    }
}
