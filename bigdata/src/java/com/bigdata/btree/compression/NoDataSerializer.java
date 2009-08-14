package com.bigdata.btree.compression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.raba.IRaba;


/**
 * Useful when no data will be written.
 */
public class NoDataSerializer implements IDataSerializer {

    /**
     * 
     */
    private static final long serialVersionUID = 6683355231100666183L;

    public transient static NoDataSerializer INSTANCE = new NoDataSerializer();

    public void write(DataOutput out, IRaba raba) throws IOException {

        // NOP
        
    }

    public void read(DataInput in, IRaba raba) throws IOException {

        // NOP
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        // NOP

    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        // NOP

    }

}