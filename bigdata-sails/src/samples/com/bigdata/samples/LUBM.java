package com.bigdata.samples;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

public class LUBM {
    
    final static String NS =
        "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#";

    final static URI FULL_PROFESSOR = new URIImpl(NS + 
        "FullProfessor"
        );
    
    final static URI PROFESSOR = new URIImpl(NS + 
            "Professor"
            );
        
    final static URI WORKS_FOR = new URIImpl(NS + 
            "worksFor"
            );
        
    final static URI NAME = new URIImpl(NS + 
            "name"
            );
        
    final static URI EMAIL_ADDRESS = new URIImpl(NS + 
            "emailAddress"
            );
        
    final static URI TELEPHONE = new URIImpl(NS + 
            "telephone"
            );
        
}
