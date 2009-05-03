package com.bigdata.btree.compression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.log4j.Logger;


/**
 * Wrap two serializers, one for small # of keys and one for large.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class WrapSerializer implements IDataSerializer, Externalizable {

    protected final static Logger log = Logger.getLogger(WrapSerializer.class);
    
    protected final static boolean INFO = log.isInfoEnabled();
    
    /**
     * 
     */
    private static final long serialVersionUID = -653468541332465324L;

    private IDataSerializer small, big;
    
    private int numkeys;
    
    /**
     * Sole constructor (handles de-serialization also).
     */
    public WrapSerializer(IDataSerializer small, IDataSerializer big, int numkeys) {

        this.small = small;
        
        this.big = big;
        
        this.numkeys = numkeys;
        
    }
    
    public void read(DataInput in, IRandomAccessByteArray raba) throws IOException {

        // true if the small serializer was used
        if (in.readBoolean()) {
            
            if (INFO) log.info("reading using small serializer"); 
            
            small.read(in, raba);
            
        } else {
            
            if (INFO) log.info("reading using big serializer"); 
            
            big.read(in, raba);
            
        }
    }

    public void write(DataOutput out, IRandomAccessByteArray raba)
            throws IOException {
        
        final int n = raba.getKeyCount();
        
        if (n < numkeys) {
            
            if (INFO) log.info("writing using small serializer"); 
            
            out.writeBoolean(true /*small*/);
            
            small.write(out, raba);
            
        } else {
            
            if (INFO) log.info("writing using big serializer"); 
            
            out.writeBoolean(false /*small*/);
            
            big.write(out, raba);
            
        }
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        // NOP
        
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        // NOP
        
    }

}
