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

import java.util.TimeZone;

import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.internal.constraints.IMathOpHandler;
import com.bigdata.rdf.internal.impl.AbstractInlineExtensionIV;
import com.bigdata.rdf.internal.impl.extensions.XSDStringExtension;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.lexicon.LexiconKeyOrder;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * Configuration determines which RDF Values are inlined into the statement
 * indices rather than being assigned term identifiers by the lexicon.
 */
public interface ILexiconConfiguration<V extends BigdataValue> {

    /**
     * Return the maximum length of a Unicode string which may be inlined into
     * the statement indices. This applies to blank node IDs, literal labels
     * (including the {@link XSDStringExtension}), local names of {@link URI}s,
     * etc.
     *
     * @see AbstractTripleStore.Options#MAX_INLINE_TEXT_LENGTH
     */
    public int getMaxInlineStringLength();

    /**
     *
     * @see AbstractTripleStore.Options#INLINE_TEXT_LITERALS
     */
    public boolean isInlineTextLiterals();

    /**
     * Return <code>true</code> if datatype literals are being inlined into
     * the statement indices.
     */
    public boolean isInlineLiterals();

    /**
     * Return <code>true</code> if xsd:datetime literals are being inlined into
     * the statement indices.
     */
    public boolean isInlineDateTimes();
    
    /**
     * Return <code>true</code> if GeoSpatial support is enabled.
     */
    public boolean isGeoSpatial();
    
    
    /**
     * Return the configuration string for the GeoSpatial service
     */
    public String getGeoSpatialConfig();
    

    /**
     * Return the default time zone to be used for inlining.
     */
    public TimeZone getInlineDateTimesTimeZone();

    /**
     * Return the threshold at which a literal would be stored in the
     * {@link LexiconKeyOrder#BLOBS} index.
     *
     * @see AbstractTripleStore.Options#BLOBS_THRESHOLD
     */
    public int getBlobsThreshold();

    /**
	 * Return true iff the BLOBS index has been disabled.
	 * 
	 * @see <a href="https://github.com/SYSTAP/bigdata-gpu/issues/25"> Disable
	 * BLOBS indexing completely for GPU </a>
	 */
    public boolean isBlobsDisabled();
    
    /**
     * Create an inline {@link IV} for the supplied RDF value if inlining is
     * supported for the supplied RDF value.
     * <p>
     * Note: If the supplied RDF value is a {@link BigdataValue} then <em>the
     * {@link IV} will be set as a side-effect</em> and will be available from
     * {@link BigdataValue#getIV()}.
     *
     * @param value
     *            The RDF value
     *
     * @return The inline {@link IV} -or- <code>null</code> if the {@link Value}
     *         can not be represented by an {@link IV}.
     */
    IV createInlineIV(final Value value);

    /**
     * Inflate the localName portion of an inline URI using its storage delegate.
     * @param namespace the uris's prefix
     * @param delegate the storage delegate
     * @return the inflated localName
     */
    String getInlineURILocalNameFromDelegate(final URI namespace,
            final AbstractLiteralIV<BigdataLiteral, ?> delegate);

    /**
     * Create an RDF value from an {@link AbstractInlineExtensionIV}. The
     * "extension" {@link IV} MUST be registered with the {@link Vocabulary}.
     * <p>
     * For {@link LiteralExtensionIV}, this through an internal catalog of
     * {@link IExtension}s to find one that knows how to handle the extension
     * datatype from the supplied {@link LiteralExtensionIV}. This is the
     * historical use case.
     *
     * @param iv
     *            the extension IV
     *
     * @return The RDF {@link Value}
     */
    V asValue(final LiteralExtensionIV<?> iv);

    /**
     * Return the {@link Value} for that {@link IV} iff the {@link IV} is
     * declared in the {@link Vocabulary}.
     *
     * @param iv
     *            The {@link IV}.
     *
     * @return The {@link Value} -or- <code>null</code> if the {@link IV} was
     *         not declared in the {@link Vocabulary}.
     */
    V asValueFromVocab(final IV<?,?> iv);

    /**
     * Initialize the extensions, which need to resolve their datatype URIs into
     * term ids.
     */
    void initExtensions(final IDatatypeURIResolver resolver);

    /**
     * Return the value factory for the lexicon.
     */
    BigdataValueFactory getValueFactory();
    
    /**
     * Should the specified datatype be included in the text index (even though
     * it is an inline datatype, for example IPv4).
     */
    boolean isInlineDatatypeToTextIndex(URI datatype);

    /**
     * Get iterator over registered type handlers.
     */
    Iterable<IMathOpHandler> getTypeHandlers();

}
