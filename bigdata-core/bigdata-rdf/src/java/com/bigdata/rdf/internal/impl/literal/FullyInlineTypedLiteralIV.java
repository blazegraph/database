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

package com.bigdata.rdf.internal.impl.literal;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IInlineUnicode;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUnicode;
import com.bigdata.rdf.internal.impl.extensions.XSDStringExtension;
import com.bigdata.rdf.lexicon.ITermIndexCodes;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataLiteralImpl;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Implementation for inline {@link Literal}s. Literals may be plain (just a
 * label), have a datatype URI, or have a language code. In each case, all
 * information is inlined. This class is mainly targeted at inlining small plain
 * literals and language code literals, but may also be used to fully inline
 * literals in scale-out (which can be an attractive option).
 * <p>
 * Note: Optimized support for <code>xsd:string</code> inlining is provided by
 * the {@link XSDStringExtension}.
 * 
 * TODO Validate methods on this class against {@link Literal} and
 * {@link BigdataLiteralImpl} (API compliance).
 */
public class FullyInlineTypedLiteralIV<V extends BigdataLiteral> extends
        AbstractLiteralIV<V, String> implements IInlineUnicode, Literal {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /** The Literal's label. */
    private final String label;

    /**
     * The Literal's language code (optional but exclusive with the
     * {@link #datatype}).
     */
    private final String language;

    /**
     * The Literal's language datatype URI (optional but exclusive with the
     * {@link #language}).
     */
    private final URI datatype;

    /**
     * Indicates what "type" of literal this is.
     * 
     * @see ITermIndexCodes
     */
    private final byte termCode;
    
    /**
     * Indicates what literal is temporary and would need additional resolution.
	/**
	 * <code>true</code> iff the {@link IV} represents a <em>temporary</em>
	 * {@link IV} reference. <code>Temporary</code> {@link IV}s are somewhat special.
	 * They are used while preparing query/update without access to the triple store.
	 * <code>Temporary</code> {@link IV}s have to be recreated while executing update,
	 * as new terms may need adding to Term2ID and ID2Term indexes.
	 * @see https://jira.blazegraph.com/browse/BLZG-1176 (SPARQL Parsers should not be db mode aware)
	 * Introduced while fixing issue
	 * @see https://jira.blazegraph.com/browse/BLZG-1893 (Problems with Fulltext Index)
	 */
    private final boolean temp;
    
    /** The cached byte length of this {@link IV}. */
    private transient int byteLength = 0;

    public IV<V, String> clone(final boolean clearCache) {

        final FullyInlineTypedLiteralIV<V> tmp = new FullyInlineTypedLiteralIV<V>(
                label, language, datatype);

        // propagate transient state if available.
        tmp.byteLength = byteLength;

        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }
    
    public FullyInlineTypedLiteralIV(final String label) {
     
        this(label, null/* languageCode */, null/* datatype */, false/* temp */);
        
    }
    
    public FullyInlineTypedLiteralIV(final String label, final boolean temp) {
        
        this(label, null/* languageCode */, null/* datatype */, temp);
        
    }
    
    public FullyInlineTypedLiteralIV(final String label, final String languageCode,
            final URI datatypeURI) {

        this(label, languageCode, datatypeURI, 0/* byteLength */, false/* temp */);

    }

    public FullyInlineTypedLiteralIV(final String label, final String languageCode,
            final URI datatypeURI, final boolean temp) {

        this(label, languageCode, datatypeURI, 0/* byteLength */, temp);

    }

    public FullyInlineTypedLiteralIV(final String label, final String languageCode,
            final URI datatypeURI, final int byteLength) {
    	this(label, languageCode, datatypeURI, byteLength, false/* temp */);
    }

    public FullyInlineTypedLiteralIV(final String label, final String languageCode,
            final URI datatypeURI, final int byteLength, final boolean temp) {

        super(DTE.XSDString);

        if (label == null)
            throw new IllegalArgumentException();

        if (languageCode != null && datatypeURI != null)
            throw new IllegalArgumentException();

        this.label = label;

        this.language = languageCode;

        this.datatype = datatypeURI;

        if (datatypeURI != null) {
            this.termCode = ITermIndexCodes.TERM_CODE_DTL;
        } else if (languageCode != null) {
            this.termCode = ITermIndexCodes.TERM_CODE_LCL;
        } else {
            this.termCode = ITermIndexCodes.TERM_CODE_LIT;
        }

        this.byteLength = byteLength;
        
        this.temp = temp;
        
    }

    final public String getInlineValue() {

        return label;
        
    }
    
    @Override
    public boolean isNullIV() {
    	return temp || super.isNullIV();
    }

    /**
     * Overrides {@link AbstractLiteralIV#getLabel()}.
     */
    @Override
    final public String getLabel() {
        
        return label;
        
    }
    
    /**
     * Overrides {@link AbstractLiteralIV#getLanguage()}.
     */
    @Override
    final public String getLanguage() {
        
        return language;
        
    }

    /**
     * Overrides {@link AbstractLiteralIV#getDatatype()}.
     */
    @Override
    final public URI getDatatype() {
        
        return datatype;
        
    }

    /**
     * Indicates what "type" of literal this is.
     * 
     * @see ITermIndexCodes
     */
    public final byte getTermCode() {

        return termCode;
        
    }
    
    @SuppressWarnings("unchecked")
    public V asValue(final LexiconRelation lex) {
		V v = getValueCache();
		if (v == null) {
            final BigdataValueFactory f = lex.getValueFactory();
            if (datatype != null) {
                v = (V) f.createLiteral(label, datatype);
            } else if (language != null) {
                v = (V) f.createLiteral(label, language);
            } else {
                v = (V) f.createLiteral(label);
            }
			v.setIV(this);
			setValue(v);
		}
		return v;
    }

    public boolean equals(final Object o) {
        if (this == o)
            return true;

        if (!(o instanceof FullyInlineTypedLiteralIV<?>))
            return false;

        final FullyInlineTypedLiteralIV<?> t = (FullyInlineTypedLiteralIV<?>) o;

        if (!label.equals(t.getLabel()))
            return false;

        if (language != null) {

            // the language code is case insensitive.
            return language.equalsIgnoreCase(t.getLanguage());

        } else if (t.getLanguage() != null) {

            return false;

        }

        if (datatype != null) {

            return datatype.equals(t.getDatatype());

        } else if (t.getDatatype() != null) {

            return false;

        }

        return true;
        
    }

    /**
     * Return the hash code of the label (per {@link Literal#hashCode()})
     */
    public int hashCode() {

        return label.hashCode();
        
    }

    public int byteLength() {
        
        if (byteLength == 0) {

            /*
             * Cache the byteLength if not yet set. This needs to include the
             * termCode, the label, and the language or datatype URI iff
             * present.
             */
            byteLength = 1 // flags
                    + 1 // termCode
                    + ((termCode == ITermIndexCodes.TERM_CODE_LCL) //
                    ? IVUnicode.byteLengthUnicode(language)//
                            : (termCode == ITermIndexCodes.TERM_CODE_DTL) //
                            ? IVUnicode.byteLengthUnicode(datatype
                                    .stringValue()) //
                                    : 0) + //
                    IVUnicode.byteLengthUnicode(label)//
            ;

        }

        return byteLength;
        
    }

    final public void setByteLength(final int byteLength) {

        if (byteLength < 0)
            throw new IllegalArgumentException();
        
        if (this.byteLength != 0 && this.byteLength != byteLength)
            throw new IllegalStateException();
        
        this.byteLength = byteLength;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: The ordering here is defined over the datatype or language code
     * followed by the label. The Unicode compression scheme (BOCU) preserves
     * the code point order, so we can compare the {@link String} values of the
     * datatype URI and/or language code and the labels to determine the
     * ordering. The ordering provided is disjoint across distinct datatypes and
     * language codes. It is the same order which is present for the keys in the
     * index. The [termCode] is used to partition the space into plain literals,
     * language code literals, and datatype literals. Orderings are defined
     * within those partitions.
     */
    @Override
    public int _compareTo(final IV o) {

        final FullyInlineTypedLiteralIV<?> t = ((FullyInlineTypedLiteralIV<?>) o);

        int ret = termCode - t.termCode;
        
        if (ret < 0)
            return -1;
        
        if (ret > 0)
            return 1;

        switch (termCode) {
        case ITermIndexCodes.TERM_CODE_LIT:
            return IVUnicode.IVUnicodeComparator.INSTANCE.compare(label,t.label);
        case ITermIndexCodes.TERM_CODE_LCL:
            ret = IVUnicode.IVUnicodeComparator.INSTANCE.compare(language,t.language);
            if (ret < 0)
                return -1;
            if (ret > 0)
                return 1;
            return IVUnicode.IVUnicodeComparator.INSTANCE.compare(label,t.label);
        case ITermIndexCodes.TERM_CODE_DTL:
            ret = IVUnicode.IVUnicodeComparator.INSTANCE.compare(datatype.stringValue(),t.datatype.stringValue());
//            ret = datatype.stringValue().compareTo(t.datatype.stringValue());
            if (ret < 0)
                return -1;
            if (ret > 0)
                return 1;
            return IVUnicode.IVUnicodeComparator.INSTANCE.compare(label,t.label);
        default:
            throw new AssertionError();
        }

    }
    
}
