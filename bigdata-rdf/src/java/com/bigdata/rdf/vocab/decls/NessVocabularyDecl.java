/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Vocabulary declaration for Ness / TSG LI use case. The following queries
 * and query patterns were used for extraction of the vocabulary (manually):
 * 
 * 1. Get distinct predicates
 * <code>
 * SELECT DISTINCT ?p WHERE { ?s ?p ?o }
 * </code>
 * 
 * 2. Get instance prefixes (query shows the idea, FILTERs added step by step
 *    to obtain the prefixes used in the dataset), namely
 *    
 * 2.1 for subject position
 * <code>
 * SELECT ?s WHERE { 
    ?s ?p ?o 
    FILTER(!(STRSTARTS(STR(?s),"http://ness.org/storage/area/Area@@")))
    FILTER(!(STRSTARTS(STR(?s),"http://ness.org/storage/connected-phones/CP@@")))
    FILTER(!(STRSTARTS(STR(?s),"http://ness.org/storage/phone-id/Pstn@@")))
    FILTER(!(STRSTARTS(STR(?s),"http://ness.org/storage/phone/Phone@@")))
    FILTER(!(STRSTARTS(STR(?s),"http://ness.org/storage/gang/Cell@@")))
  } LIMIT 1
 * </code>
 * 
 * 2.2 for object position
 * <code>
 * SELECT ?o WHERE { 
    ?s ?p ?o 
    FILTER(isURI(?o) && !(STRSTARTS(STR(?o),"http://ness.org/storage/area/Area@@")))
    FILTER(isURI(?o) && !(STRSTARTS(STR(?o),"http://ness.org/storage/connected-phones/CP@@")))
    FILTER(isURI(?o) && !(STRSTARTS(STR(?o),"http://ness.org/storage/phone-id/Pstn@@")))
    FILTER(isURI(?o) && !(STRSTARTS(STR(?o),"http://ness.org/storage/phone/Phone@@")))
    FILTER(isURI(?o) && !(STRSTARTS(STR(?o),"http://ness.org/storage/gang/Cell@@")))
    FILTER(isURI(?o) && !(STRSTARTS(STR(?o),"http://ness.org/storage/gang/Family@@")))
    FILTER(isURI(?o) && !(STRSTARTS(STR(?o),"http://ness.org/storage/gang/Gang@@")))
    FILTER(isURI(?o) && !(STRSTARTS(STR(?o),"http://ness.org/storage/person/Person@@")))   
    FILTER(isURI(?o) && !(STRSTARTS(STR(?o),"http://ness.org/storage/phone-id/Imei@@")))
    FILTER(?o!=<http://ness.org/input-stream#gang>)
  } LIMIT 1
 * </code>
 * 
 * 2.3 for context position (assuming the original data scheme)
 * <code>
 * SELECT ?g WHERE { 
     GRAPH ?g { ?s ?p ?o }
     FILTER(isURI(?g) && !(STRSTARTS(STR(?g),"http://ness.org/storage/area/Area@@")))
     FILTER(isURI(?g) && !(STRSTARTS(STR(?g),"http://ness.org/storage/connected-phones/CP@@")))
     FILTER(isURI(?g) && !(STRSTARTS(STR(?g),"http://ness.org/storage/phone-id/Pstn@@")))
     FILTER(isURI(?g) && !(STRSTARTS(STR(?g),"http://ness.org/storage/phone/Phone@@")))
     FILTER(isURI(?g) && !(STRSTARTS(STR(?g),"http://ness.org/storage/gang/Cell@@")))
     FILTER(isURI(?g) && !(STRSTARTS(STR(?g),"http://ness.org/storage/gang/Family@@")))
     FILTER(isURI(?g) && !(STRSTARTS(STR(?g),"http://ness.org/storage/gang/Gang@@")))
     FILTER(isURI(?g) && !(STRSTARTS(STR(?g),"http://ness.org/storage/person/Person@@")))   
     FILTER(isURI(?g) && !(STRSTARTS(STR(?g),"http://ness.org/storage/phone-id/Imei@@")))
     FILTER(isURI(?g) && !(STRSTARTS(STR(?g),"http://ness.org/storage/transaction/Trans@@")))    
     FILTER(?g!=<http://default.graph>)
   } LIMIT 1
 * </code>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class NessVocabularyDecl implements VocabularyDecl {

    public static final String VOCABULARY = "http://ness.org/input-stream#";

    public static final String INSTANCES = "http://ness.org/storage/";
    
    static private final URI[] uris = new URI[]{//
        // namespace for instance data.
        
        // subject position instance prefixes
        new URIImpl(INSTANCES + "area/Area@@"),//
        new URIImpl(INSTANCES + "connected-phones/CP@@"),//
        new URIImpl(INSTANCES + "phone-id/Pstn@@"),//
        new URIImpl(INSTANCES + "phone/Phone@@"),//
        new URIImpl(INSTANCES + "gang/Cell@@"),//
        new URIImpl(INSTANCES + "gang/Family@@"),//
        new URIImpl(INSTANCES + "gang/Gang@@"),//
        new URIImpl(INSTANCES + "/person/Person@@"),//
        
        // object position instance prefixes
        new URIImpl(INSTANCES + "phone-id/Imei@@"),//
        
        // graph position instance prefixes
        new URIImpl(INSTANCES + "transaction/Trans@@"),//
        
        // default instance namespace prefix (default for missed instances)
        new URIImpl(INSTANCES),
        
        // predicate constants
        new URIImpl(VOCABULARY),//
        new URIImpl(VOCABULARY + "area-of-interest"),//
        new URIImpl(VOCABULARY + "call-duration"),//
        new URIImpl(VOCABULARY + "call-in-progress"),//
        new URIImpl(VOCABULARY + "cell-crossing"),//
        new URIImpl(VOCABULARY + "dev-close"),//
        new URIImpl(VOCABULARY + "dev-open"),//
        new URIImpl(VOCABULARY + "location-time"),//
        new URIImpl(VOCABULARY + "new-phone-id"),//
        new URIImpl(VOCABULARY + "phone-call"),//
        new URIImpl(VOCABULARY + "reconnect"),//
        new URIImpl(VOCABULARY + "repeating-message"),//
        new URIImpl(VOCABULARY + "sms"),//
        new URIImpl(VOCABULARY + "unsuccesful-call"),//
        new URIImpl(VOCABULARY + "called-link"),//
        new URIImpl(VOCABULARY + "international-indication"),//
        new URIImpl(VOCABULARY + "leader-gang-link"),//
        new URIImpl(VOCABULARY + "person-gang-link"),//
        new URIImpl(VOCABULARY + "person-phone-identity"),//
        new URIImpl(VOCABULARY + "soldier-gang-link"),//
        new URIImpl(VOCABULARY + "subordinate-gang-link"),//
        new URIImpl(VOCABULARY + "phone-identity"),//
        new URIImpl(VOCABULARY + "pstn-imei-link"),//
        
        // object constants (types)
        new URIImpl(VOCABULARY + "gang"),
        new URIImpl(VOCABULARY + "person"),
        
        // named graph constants
        new URIImpl("http://default.graph") // default graph URI for loading

    };

    public NessVocabularyDecl() {
    }
    
    public Iterator<URI> values() {

        return Collections.unmodifiableList(Arrays.asList(uris)).iterator();
        
    }

}
