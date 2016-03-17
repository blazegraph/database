package com.bigdata.rdf.vocab;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

public class TestVocabularyDecl_BLZG1591 implements VocabularyDecl {

    static private final URI[] uris = new URI[]{//
    	// two named graphs
        new URIImpl("http://test.com/P")
    };

    public TestVocabularyDecl_BLZG1591() {
    }
    
    public Iterator<URI> values() {

        return Collections.unmodifiableList(Arrays.asList(uris)).iterator();
        
    }

}
