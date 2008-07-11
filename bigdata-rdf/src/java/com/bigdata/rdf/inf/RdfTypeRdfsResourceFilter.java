package com.bigdata.rdf.inf;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.rdf.rules.RDFSVocabulary;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.relation.accesspath.IElementFilter;

/**
 * Filter matches <code>(x rdf:type rdfs:Resource).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RdfTypeRdfsResourceFilter implements IElementFilter<SPO>, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = -2157234197316632000L;
    
    private long rdfType;
    private long rdfsResource;
    
    /**
     * De-serialization ctor.
     */
    public RdfTypeRdfsResourceFilter() {
        
    }
    
    /**
     * 
     * @param vocab
     */
    public RdfTypeRdfsResourceFilter(RDFSVocabulary vocab) {
        
        this.rdfType = vocab.rdfType.get();
        
        this.rdfsResource = vocab.rdfsResource.get();
        
    }

    public boolean accept(SPO spo) {

        if (spo.p == rdfType && spo.o == rdfsResource) {
            
            // reject (?x, rdf:type, rdfs:Resource )
            
            return true;
            
        }
        
        // Accept everything else.
        
        return false;
        
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        rdfType = in.readLong();

        rdfsResource = in.readLong();
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeLong(rdfType);

        out.writeLong(rdfsResource);
        
    }
    
}
