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
 * Created on Jun 4, 2011
 */

package com.bigdata.rdf.vocab;

import java.util.Iterator;

import org.openrdf.model.URI;

import com.bigdata.rdf.internal.impl.uri.VocabURIShortIV;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * A class for declaring a {@link Vocabulary}.
 * <p>
 * Note: Implementations of a {@link VocabularyDecl} MUST be stable across the
 * life cycle of an {@link AbstractTripleStore}. For this reason it is strongly
 * recommended that new versions of a vocabulary are defined in a new class.
 * <p>
 * Each class SHOULD provide a declaration for the namespace(s) used by the
 * vocabulary. It is convention to expose this {@link String} value as a public
 * static field named <code>NAMESPACE</code>. Declaring the namespace used by
 * provides for compact encoding of URIs within that namespace (e.g., using a
 * {@link VocabURIShortIV} for the namespace and a compressed unicode representation
 * of the localName of the URI) even if those URIs were not part of the original
 * vocabulary declaration. Thus, it provides a compact fallback encoding if the
 * vocabulary evolves after the {@link AbstractTripleStore} has been
 * provisioned.
 * 
 * @version $Id$
 */
public interface VocabularyDecl {

    /**
     * Return an iterator which will visit the declared values. The iterator
     * must not support removal.
     */
    public Iterator<URI> values();

}
