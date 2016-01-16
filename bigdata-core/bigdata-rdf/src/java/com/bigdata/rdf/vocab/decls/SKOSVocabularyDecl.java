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
 * Vocabulary and namespace for SKOS.
 * 
 * @see http://www.w3.org/2004/02/skos/core#
 * @see http://www.w3.org/TR/skos-reference/skos.html
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SKOSVocabularyDecl implements VocabularyDecl {

    public static final String NAMESPACE = "http://www.w3.org/2004/02/skos/core#";
    public static final URI Collection = new URIImpl(NAMESPACE+"Collection");
    public static final URI Concept = new URIImpl(NAMESPACE+"Concept");
    public static final URI ConceptScheme = new URIImpl(NAMESPACE+"ConceptScheme");
    public static final URI OrderedCollection = new URIImpl(NAMESPACE+"OrderedCollection");
    public static final URI altLabel = new URIImpl(NAMESPACE+"altLabel");
    public static final URI broadMatch = new URIImpl(NAMESPACE+"broadMatch");
    public static final URI broader = new URIImpl(NAMESPACE+"broader");
    public static final URI broaderTransitive = new URIImpl(NAMESPACE+"broaderTransitive");
    public static final URI changeNote = new URIImpl(NAMESPACE+"changeNote");
    public static final URI closeMatch = new URIImpl(NAMESPACE+"closeMatch");
    public static final URI definition = new URIImpl(NAMESPACE+"definition");
    public static final URI editorialNote = new URIImpl(NAMESPACE+"editorialNote");
    public static final URI exactMatch = new URIImpl(NAMESPACE+"exactMatch");
    public static final URI example = new URIImpl(NAMESPACE+"example");
    public static final URI hasTopConcept = new URIImpl(NAMESPACE+"hasTopConcept");
    public static final URI hiddenLabel = new URIImpl(NAMESPACE+"hiddenLabel");
    public static final URI historyNote = new URIImpl(NAMESPACE+"historyNote");
    public static final URI inScheme = new URIImpl(NAMESPACE+"inScheme");
    public static final URI mappingRelation = new URIImpl(NAMESPACE+"mappingRelation");
    public static final URI member = new URIImpl(NAMESPACE+"member");
    public static final URI memberList = new URIImpl(NAMESPACE+"memberList");
    public static final URI narrowMatch = new URIImpl(NAMESPACE+"narrowMatch");
    public static final URI narrow = new URIImpl(NAMESPACE+"narrow");
    public static final URI narrowTransitive = new URIImpl(NAMESPACE+"narrowTransitive");
    public static final URI notation = new URIImpl(NAMESPACE+"notation");
    public static final URI note = new URIImpl(NAMESPACE+"note");
    public static final URI prefLabel = new URIImpl(NAMESPACE+"prefLabel");
    public static final URI related = new URIImpl(NAMESPACE+"related");
    public static final URI relatedMatch = new URIImpl(NAMESPACE+"relatedMatch");
    public static final URI scopeNote = new URIImpl(NAMESPACE+"scopeNote");
    public static final URI semanticRelation= new URIImpl(NAMESPACE+"semanticRelation");
    public static final URI topConceptOf = new URIImpl(NAMESPACE+"topConceptOf");
    
    static private final URI[] uris = new URI[] {
        new URIImpl(NAMESPACE),//
        Collection, Concept, ConceptScheme, OrderedCollection, altLabel,
        broadMatch, broader, broaderTransitive, changeNote, closeMatch,
        definition, editorialNote, exactMatch, example, hasTopConcept,
        hiddenLabel, historyNote, inScheme, mappingRelation, member,
        memberList, narrowMatch, narrow, narrowTransitive, notation, note,
        prefLabel, related, relatedMatch, scopeNote, semanticRelation,
        topConceptOf//
    };

    public SKOSVocabularyDecl() {
    }
    
    public Iterator<URI> values() {

        return Collections.unmodifiableList(Arrays.asList(uris)).iterator();
        
    }

}
