package com.bigdata.btree.compression;

import it.unimi.dsi.fastutil.bytes.Byte2IntOpenHashMap;
import it.unimi.dsi.mg4j.compression.Coder;
import it.unimi.dsi.mg4j.compression.Decoder;
import it.unimi.dsi.mg4j.compression.HuffmanCodec;
import it.unimi.dsi.mg4j.io.InputBitStream;
import it.unimi.dsi.mg4j.io.OutputBitStream;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.CognitiveWeb.extser.LongPacker;
import org.apache.log4j.Logger;

/**
 * Huffman compression.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class HuffmanSerializer implements IDataSerializer, Externalizable {
    
    protected static final transient Logger log = Logger.getLogger(HuffmanSerializer.class);
    
    protected static final transient boolean INFO = log.isInfoEnabled();
    
    /**
     * 
     */
    private static final long serialVersionUID = -24523165741358551L;
    
    public final transient static HuffmanSerializer INSTANCE = new HuffmanSerializer();

    public HuffmanSerializer() {
        
    }
    
    /**
     * Use a Huffman compression algorithm to write keys to a byte stream. 
     */
    public void write(final DataOutput out, final IRandomAccessByteArray raba)
            throws IOException {

        // write number of keys
        // write that many key lengths
        // write the number of symbols
        // write each symbol's byte values
        // write each symbol's frequency
        // write the encoded byte array
        // example: [ mike ], [ personick ]
        // 2 4 9 10 c e i k m n o p r s 1 2 2 2 1 1 1 1 1 1 <num compressed bytes> <compressed bytes> 
        
        final StringBuilder info = new StringBuilder();
        
        final int n = raba.getKeyCount();

        LongPacker.packLong(out, n);
        
        if (INFO) {
            
            info.append(n).append(" ");
            
        }
        
        if (n == 0) {
            
            // no keys.
            
            return;

        }

        // concatenate all the bytes into one byte[], makes life easier
        
        for (byte[] bytes : raba) {

            LongPacker.packLong(out, bytes.length);
            
            if (INFO) {
                
                info.append(bytes.length).append(" ");
                
            }

        }
        
        // create a frequency table for every possible value of a byte
        // 256 possible values
        
        final int[] frequency = new int[Byte.MAX_VALUE - Byte.MIN_VALUE + 1];
        
        for (byte[] bytes : raba) {

            for (byte b : bytes) {

                frequency[b - Byte.MIN_VALUE]++;
                
            }
            
        }
        
        // Then, we compute the number of actually used bytes
        int count = 0;
        
        for( int i = frequency.length; i-- != 0; ) {
            
            if ( frequency[ i ] != 0 ) {
                
                count++;
                
            }
            
        }
        
        /* Now we remap used bytes, building at the same time maps from 
         * symbol to bytes and from bytes to symbols. */
        
        final int[] packedFrequency = new int[count];
        
        final byte[] symbol2byte = new byte[count];
        
        final Byte2IntOpenHashMap byte2symbol = new Byte2IntOpenHashMap(count);

        byte2symbol.defaultReturnValue(-1);

        for (int i = frequency.length, k = count; i-- != 0;) {

            if (frequency[i] != 0) {

                packedFrequency[--k] = frequency[i];

                final byte b = (byte) (i + Byte.MIN_VALUE);

                symbol2byte[k] = b;

                byte2symbol.put(b, k);

            }

        }

        byte2symbol.trim();
        
        // write the number of symbols
        
        LongPacker.packLong(out, symbol2byte.length);
        
        if (INFO) {
            
            info.append(symbol2byte.length).append(" ");
            
        }
        
        // for each symbol, write the byte
        
        out.write(symbol2byte);
        
        if (INFO) {

            for (byte b : symbol2byte) {
                
                info.append(b).append(" ");
                
            }
            
        }
            
        // for each symbol, write the frequency
        
        for (int i = 0; i < packedFrequency.length; i++) {
            
            LongPacker.packLong(out, packedFrequency[i]);
            
            if (INFO) {
                
                info.append(packedFrequency[i]).append(" ");
                
            }
            
        }
        
        // We now build the coder used to code the bytes
        
        final HuffmanCodec codec = new HuffmanCodec(packedFrequency);
        
        final Coder coder = codec.getCoder();

        final ByteArrayOutputStream data = new ByteArrayOutputStream();
        
        final OutputBitStream obs = new OutputBitStream(data);
        // Use this ctor variant directly on the [out] arg - it is much faster.
//        new OutputBitStream((OutputStream) out,
//                0/* unbuffered */, false/*reflectionTest*/);
        
        for (byte[] bytes : raba) {

            for (byte b : bytes) {

                coder.encode(byte2symbol.get(b), obs);
                
            }

        }

        obs.close();

        // write the size of the compressed byte[]

        LongPacker.packLong(out, data.size());

        if (INFO) {
            
            info.append(data.size()); 
            
        }
        
        // write out the compressed bytes
        
        out.write(data.toByteArray());
        
        if (INFO) {
            
            log.info(info.toString());
            
        }
        
    }

    /**
     * Use a Huffman compression algorithm to read keys from a byte stream. 
     */
    public void read(final DataInput in, final IRandomAccessByteArray raba)
            throws IOException {

        // read the # of keys
        
        final int nkeys = (int) LongPacker.unpackLong(in);

        if (nkeys == 0) {

            /*
             * No keys.
             */

            return;

        }

        final StringBuilder info = new StringBuilder();
        
        if (INFO) {
            
            info.append(nkeys).append(" ");
            
        }
        
        // read the key lengths for the keys
        
        final int[] keyLens = new int[nkeys];
        
        for (int i = 0; i < nkeys; i++) {
            
            keyLens[i] = (int) LongPacker.unpackLong(in);
            
            if (INFO) {
                
                info.append(keyLens[i]).append(" ");
                
            }
            
        }
        
        // read the # of symbols
        
        final int numSymbols = (int) LongPacker.unpackLong(in);
        
        if (INFO) {
            
            info.append(numSymbols).append(" ");
            
        }
        
        // for each symbol, read the byte value
        
        final byte[] symbol2byte = new byte[numSymbols];
        
        in.readFully(symbol2byte);
        
        if (INFO) {

            for (byte b : symbol2byte) {
                
                info.append(b).append(" ");
                
            }
            
        }
            
        // for each symbol, read the frequency
        
        final int[] frequency = new int[numSymbols];
        
        for (int i = 0; i < numSymbols; i++) {
            
            frequency[i] = (int) LongPacker.unpackLong(in);
            
            if (INFO) {
                
                info.append(frequency[i]).append(" ");
                
            }
            
        }
        
        // read the size of the compressed data
        
        final int dataLen = (int) LongPacker.unpackLong(in);
        
        if (INFO) {
            
            info.append(dataLen).append(" ");
            
        }
        
        // read the compressed data
        
        final byte[] data = new byte[dataLen];
        
        in.readFully(data);
        
        // decode the compressed data using the serialized frequency and 
        // symbol dictionary
        
        final HuffmanCodec codec = new HuffmanCodec(frequency);

        final Decoder decoder = codec.getDecoder();
        
        final InputBitStream ibs = new InputBitStream(data);
        
        for (int i = 0; i < nkeys; i++) {
            
            final byte[] key = new byte[keyLens[i]];
            
            for (int j = 0; j < keyLens[i]; j++) {
            
                final int symbol = decoder.decode(ibs);

                key[j] = (byte) symbol2byte[symbol]; 
                
            }

            raba.add(key);
            
        }
        
        if (INFO) {
            
            log.info(info.toString());
            
        }
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        // NOP - this class has no state.

    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        // NOP - this class has no state.

    }

}
