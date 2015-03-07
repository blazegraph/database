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
package com.bigdata.rdf.rules;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.RDFSVocabulary;
import com.bigdata.rdf.vocab.VocabularyDecl;

public class SampleVocab extends RDFSVocabulary {

	private static final long serialVersionUID = -6646094201371729113L;
	
    /**
     * De-serialization ctor.
     */
    public SampleVocab() {
        
        super();
        
    }
    
    /**
     * Used by {@link AbstractTripleStore#create()}.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     */
    public SampleVocab(final String namespace) {

        super( namespace );
        
    }

    /**
     * Add any values used by custom inference rules.
     */
    @Override
    protected void addValues() {

    	super.addValues();
    	
        addDecl(new SampleDecl());
        
    }

    public static class SampleDecl implements VocabularyDecl {

        static private final URI[] uris = new URI[]{
                new URIImpl(SAMPLE.NAMESPACE),
                SAMPLE.SIMILAR_TO, //
        };

        public SampleDecl() {
        }
        
        public Iterator<URI> values() {

            return Collections.unmodifiableList(Arrays.asList(uris)).iterator();
            
        }

    }
    
}
