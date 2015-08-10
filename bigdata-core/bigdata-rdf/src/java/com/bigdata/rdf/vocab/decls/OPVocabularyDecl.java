package com.bigdata.rdf.vocab.decls;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.vocab.VocabularyDecl;

public class OPVocabularyDecl implements VocabularyDecl {

    static private final URI[] uris = new URI[]{//
    	// two named graphs
        new URIImpl("file:///home/OPS/develop/openphacts/datasets/chem2bio2rdf/chembl.nt"),//
        new URIImpl("http://linkedlifedata.com/resource/drugbank"),//
    };

    public OPVocabularyDecl() {
    }
    
    public Iterator<URI> values() {

        return Collections.unmodifiableList(Arrays.asList(uris)).iterator();
        
    }

}
