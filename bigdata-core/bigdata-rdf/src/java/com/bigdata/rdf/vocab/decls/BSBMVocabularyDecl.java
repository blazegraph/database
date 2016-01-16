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
 * Vocabulary and namespace for BSBM.
 * <p>
 * Note: the benchmark also makes use of some dublin core vocabulary, including:
 * <ul>
 * <li>http://purl.org/stuff/rev#text</li>
 * <li>http://purl.org/stuff/rev#reviewer</li>
 * <li>http://purl.org/stuff/rev#Review</li>
 * </ul>
 * 
 * @see http://www4.wiwiss.fu-berlin.de/bizer/BerlinSPARQLBenchmark/
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: SesameVocabularyDecl.java 4628 2011-06-04 22:01:57Z thompsonbry
 *          $
 */
public class BSBMVocabularyDecl implements VocabularyDecl {

    public static final String VOCABULARY = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/";

    public static final String INSTANCES = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/";

    public static final URI USD = new URIImpl(VOCABULARY + "USD");
    
    static private final URI[] uris = new URI[]{//
        // namespace for instance data.
        new URIImpl(INSTANCES),//
        // namespace for vocabulary
        new URIImpl(VOCABULARY),//
        new URIImpl(VOCABULARY + "reviewDate"),//
        new URIImpl(VOCABULARY + "reviewFor"),//
        new URIImpl(VOCABULARY + "ProductType"),//
        new URIImpl(VOCABULARY + "rating1"),//
        new URIImpl(VOCABULARY + "rating2"),//
        new URIImpl(VOCABULARY + "rating3"),//
        new URIImpl(VOCABULARY + "rating4"),//
        USD,//
    };

    public BSBMVocabularyDecl() {
    }
    
    public Iterator<URI> values() {

        return Collections.unmodifiableList(Arrays.asList(uris)).iterator();
        
    }

}
