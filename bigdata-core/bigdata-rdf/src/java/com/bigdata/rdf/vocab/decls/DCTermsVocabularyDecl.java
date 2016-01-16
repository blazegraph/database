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
 * Created on Jun 6, 2011
 */

package com.bigdata.rdf.vocab.decls;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

/**
 * Dublin Core terms.
 * 
 * @see http://purl.org/dc/terms/
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DCTermsVocabularyDecl {

    public static final String NAMESPACE = "http://purl.org/dc/terms/";
    public static final URI description = new URIImpl(NAMESPACE + "description");
    public static final URI hasVersion = new URIImpl(NAMESPACE + "hasVersion");
    public static final URI issued = new URIImpl(NAMESPACE + "issued");
    public static final URI modified = new URIImpl(NAMESPACE + "modified");
    public static final URI publisher = new URIImpl(NAMESPACE + "publisher");
    public static final URI reviewer = new URIImpl(NAMESPACE + "reviewer");
    public static final URI Review = new URIImpl(NAMESPACE + "Review");
    public static final URI text = new URIImpl(NAMESPACE + "text");
    public static final URI title = new URIImpl(NAMESPACE + "title");

    static private final URI[] uris = new URI[]{
            new URIImpl(NAMESPACE),//
            description, hasVersion, issued, modified, publisher, reviewer,
            Review, text, title, };

    public DCTermsVocabularyDecl() {
    }

    public Iterator<URI> values() {

        return Collections.unmodifiableList(Arrays.asList(uris)).iterator();

    }

}
