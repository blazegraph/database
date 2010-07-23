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
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;

public interface IDatatypeURIResolver {

    /**
     * Returns a fully resolved datatype URI with the {@link TermId} set.
     * {@link IExtension}s will handle encoding and decoding of inline literals
     * for custom datatypes, however to do so they will need the term identifier
     * for the custom datatype.  By passing an instance of this interface into
     * the constructor for the {@link IExtension}, it will be able to resolve
     * its datatype URI and cache it for future use.
     * <p>
     * If the datatype URI is not already in the lexicon this method MUST add
     * it to the lexicon so that it has an assigned term identifier. 
     * <p>
     * This is implemented by the {@link LexiconRelation}. 
     * 
     * @param uri   
     *          the term to resolve
     * @return
     *          the fully resolved term
     */
    BigdataURI resolve(final URI datatypeURI);
    
}
