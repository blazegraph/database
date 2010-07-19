package com.bigdata.rdf.inf;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOFilter;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * Filter matches <code>(x rdf:type rdfs:Resource).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RdfTypeRdfsResourceFilter extends SPOFilter implements Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = -2157234197316632000L;
    
    private IV rdfType;
    private IV rdfsResource;
    
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

    public boolean accept(final Object o) {
        
        if (!canAccept(o)) {
            
            return true;
            
        }
        
        final ISPO spo = (ISPO) o;
        
        if (spo.p().equals(rdfType) && spo.o().equals(rdfsResource)) {
            
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

        rdfType = (IV) in.readObject();

        rdfsResource = (IV) in.readObject();
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

//        LongPacker.packLong(out,rdfType);
//
//        LongPacker.packLong(out,rdfsResource);

        out.writeObject(rdfType);

        out.writeObject(rdfsResource);
        
    }
    
}
