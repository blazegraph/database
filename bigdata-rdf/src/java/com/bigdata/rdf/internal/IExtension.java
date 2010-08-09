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

import org.openrdf.model.Value;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * IExtensions are responsible for roundtripping between an RDF value and an
 * {@link ExtensionIV} for a particular datatype.  Because of how
 * {@link ExtensionIV}s are encoded and decoded, the IExtension will need to
 * have on hand the {@link TermId} for its datatype.  This is accomplished
 * via the {@link IDatatypeURIResolver} - the IExtension will give the resolver
 * the datatype URI it needs resolved and the resolver will lookup (or create)
 * the {@link TermId}.  
 */
public interface IExtension<V extends BigdataValue> {

//    /**
//     * This will be called very early in the IExtension lifecycle so that the
//     * {@link TermId} for the datatype URI will be on hand when needed.
//     * 
//     * @param resolver
//     *          the datatype URI resolver
//     */
//    void resolveDatatype(final IDatatypeURIResolver resolver);
    
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
     *          the RDF value
     * @return
     *          the extension IV
     */
    ExtensionIV createIV(final Value value);
    
    /**
     * Create an RDF value from an {@link ExtensionIV}.
     * 
     * @param iv
     *          the extension IV
     * @param vf
     *          the bigdata value factory
     * @return
     *          the RDF value
     */
    V asValue(final ExtensionIV iv, final BigdataValueFactory vf);
    
}
