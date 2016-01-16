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

package com.bigdata.rdf.vocab;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

/**
 * Basic {@link VocabularyDecl} implementation.
 */
public class BaseVocabularyDecl implements VocabularyDecl {

    private final URI[] uris;

    /**
     * Construct a decl from an array of URIs or URI Strings.  Any object types
     * other than URI or String will cause an IllegalArgumentException.
     */
    public BaseVocabularyDecl(final Object... uris) {
        if (uris == null) {
            this.uris = new URI[0];
        } else {
            this.uris = new URI[uris.length];
            for (int i = 0; i < uris.length; i++) {
                if (uris[i] instanceof URI) {
                    this.uris[i] = (URI) uris[i];
                } else if (uris[i] instanceof String) {
                    this.uris[i] = new URIImpl((String) uris[i]);
                } else {
                    throw new IllegalArgumentException("URIs or Strings only: " + uris[i]);
                }
            }
        }
    }
    
    /**
     * Return an iterator which will visit the declared values. The iterator
     * must not support removal.
     */
    public Iterator<URI> values() {
        return Collections.unmodifiableList(Arrays.asList(uris)).iterator();
    }

}
