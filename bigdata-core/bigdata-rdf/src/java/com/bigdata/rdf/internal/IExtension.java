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

import java.util.Set;

import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * {@link IExtension}s are responsible for round-tripping between an RDF
 * {@link Value} and an {@link LiteralExtensionIV} for a particular datatype.
 * Because of how {@link LiteralExtensionIV}s are encoded and decoded, the
 * {@link IExtension} will need to have on hand the {@link TermId} for its
 * datatype. This is accomplished via the {@link IDatatypeURIResolver} - the
 * {@link IExtension} will give the resolver the datatype {@link URI} it needs
 * resolved and the resolver will lookup (or create) the {@link TermId}. This
 * relies on the declaration of that {@link URI} as part of the
 * {@link Vocabulary}.
 */
public interface IExtension<V extends BigdataValue> {

    /**
     * Return the fully resolved datatype(s) handled by this interface in the
     * form of a {@link BigdataURI} with the {@link TermId} already set.
     * 
     * @return the datatype
     */
    Set<BigdataURI> getDatatypes();
    
    /**
     * Create an {@link LiteralExtensionIV} from an RDF value.
     * 
     * @param value
     *            The RDF {@link Value}
     *            
     * @return The extension {@link IV} -or- <code>null</code> if the
     *         {@link Value} can not be inlined using this {@link IExtension}.
     */
    LiteralExtensionIV createIV(final Value value);
    
    /**
     * Create an RDF value from an {@link LiteralExtensionIV}.
     * 
     * @param iv
     *          The extension {@link IV}
     * @param vf
     *          The bigdata value factory
     * @return
     *          The RDF {@link Value}
     */
    V asValue(final LiteralExtensionIV iv, final BigdataValueFactory vf);
    
}
