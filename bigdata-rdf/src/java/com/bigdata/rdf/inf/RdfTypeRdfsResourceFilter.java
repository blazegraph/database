package com.bigdata.rdf.inf;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.CognitiveWeb.extser.LongPacker;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.accesspath.IElementFilter;

/**
 * Filter matches <code>(x rdf:type rdfs:Resource).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RdfTypeRdfsResourceFilter implements IElementFilter<ISPO>, Externalizable {

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
    public RdfTypeRdfsResourceFilter(Vocabulary vocab) {
        
        this.rdfType = vocab.get(RDF.TYPE);
        
        this.rdfsResource = vocab.get(RDFS.RESOURCE);
        
    }

    public boolean accept(ISPO spo) {

        if (spo.p() == rdfType && spo.o() == rdfsResource) {
            
            // reject (?x, rdf:type, rdfs:Resource )
            
            return true;
            
        }
        
        // Accept everything else.
        
        return false;
        
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
//        rdfType = LongPacker.unpackLong(in);
//
//        rdfsResource = LongPacker.unpackLong(in);

        rdfType = in.readLong();

        rdfsResource = in.readLong();
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

//        LongPacker.packLong(out,rdfType);
//
//        LongPacker.packLong(out,rdfsResource);

        out.writeLong(rdfType);

        out.writeLong(rdfsResource);
        
    }
    
}
