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

package com.bigdata.rdf.vocab.decls;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.vocab.VocabularyDecl;

/**
 * Vocabulary and namespace for the Dublin Core "elements".
 * 
 * @see http://purl.org/dc/elements/1.1/
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DCElementsVocabularyDecl implements VocabularyDecl {

    public static final String NAMESPACE = "http://purl.org/dc/elements/1.1/";
    public static final URI title = new URIImpl(NAMESPACE + "title");
    public static final URI creator = new URIImpl(NAMESPACE + "creator");
    public static final URI subject = new URIImpl(NAMESPACE + "subject");
    public static final URI description = new URIImpl(NAMESPACE + "description");
    public static final URI publisher = new URIImpl(NAMESPACE + "publisher");
    public static final URI contributor = new URIImpl(NAMESPACE + "contributor");
    public static final URI date = new URIImpl(NAMESPACE + "date");
    public static final URI type = new URIImpl(NAMESPACE + "type");
    public static final URI format = new URIImpl(NAMESPACE + "format");
    public static final URI identifier = new URIImpl(NAMESPACE + "identifier");
    public static final URI source = new URIImpl(NAMESPACE + "source");
    public static final URI language = new URIImpl(NAMESPACE + "language");
    public static final URI relation = new URIImpl(NAMESPACE + "relation");
    public static final URI coverage = new URIImpl(NAMESPACE + "coverage");
    public static final URI rights = new URIImpl(NAMESPACE + "rights");
        
    static private final URI[] uris = new URI[]{
            new URIImpl(NAMESPACE),//
            title, creator, subject, description, publisher, contributor, date,
            type, format, identifier, source, language, relation, coverage,
            rights
    };

    public DCElementsVocabularyDecl() {
    }
    
    public Iterator<URI> values() {

        return Collections.unmodifiableList(Arrays.asList(uris)).iterator();
        
    }

}
