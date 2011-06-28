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

package com.bigdata.rdf.internal;

import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * {@link IExtension}s are responsible for round-tripping between an RDF
 * {@link Value} and an {@link ExtensionIV} for a particular datatype. Because
 * of how {@link ExtensionIV}s are encoded and decoded, the {@link IExtension}
 * will need to have on hand the {@link TermId} for its datatype. This is
 * accomplished via the {@link IDatatypeURIResolver} - the {@link IExtension}
 * will give the resolver the datatype {@link URI} it needs resolved and the
 * resolver will lookup (or create) the {@link TermId}. This is done when the
 * {@link LexiconRelation} is created since the operation must write on the
 * lexicon.
 */
public interface IExtension<V extends BigdataValue> {

    /**
     * Return the fully resolved datatype in the form of a {@link BigdataURI}
     * with the {@link TermId} already set.
     * 
     * @return
     *          the datatype
     */
    BigdataURI getDatatype();

    /**
     * Create an {@link ExtensionIV} from an RDF value.
     * 
     * @param value
     *            The RDF {@link Value}
     *            
     * @return The extension {@link IV} -or- <code>null</code> if the
     *         {@link Value} can not be inlined using this {@link IExtension}.
     */
    ExtensionIV createIV(final Value value);
    
    /**
     * Create an RDF value from an {@link ExtensionIV}.
     * 
     * @param iv
     *          The extension {@link IV}
     * @param vf
     *          The bigdata value factory
     * @return
     *          The RDF {@link Value}
     */
    V asValue(final ExtensionIV iv, final BigdataValueFactory vf);
    
}
