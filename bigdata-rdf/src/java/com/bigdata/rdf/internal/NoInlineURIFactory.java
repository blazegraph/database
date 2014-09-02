/**

Copyright (C) SYSTAP, LLC 2006-Infinity.  All rights reserved.

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

import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * Do-nothing inline URI factory used in the case where there is no vocabulary
 * defined.
 */
public class NoInlineURIFactory implements IInlineURIFactory {

    public NoInlineURIFactory() {
    }

    public void init(final Vocabulary vocab) {
    }
    
    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public URIExtensionIV createInlineURIIV(URI uri) {
        return null;
    }

}
