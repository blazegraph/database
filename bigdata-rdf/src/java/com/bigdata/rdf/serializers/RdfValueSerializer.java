/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
package com.bigdata.rdf.serializers;

import java.io.DataInput;
import java.io.IOException;

import org.CognitiveWeb.extser.LongPacker;
import org.openrdf.model.Value;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IValueSerializer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.util.RdfKeyBuilder;
import com.ibm.icu.text.UnicodeCompressor;
import com.ibm.icu.text.UnicodeDecompressor;

/**
 * Serializes the RDF {@link Value} using custom logic.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Optimize ICU UTF compression using the incremental APIs. This will
 *       reduce heap allocation as well during (de-)serialization. Without
 *       optimization, UTF compression is just slightly slower on write.
 * 
 * @todo use a per-leaf dictionary to factor out common strings as codes, e.g.,
 *       Hamming codes. This should be done in the basic btree package.
 * 
 * @todo pass reference to the {@link IIndex} object so that we can version the
 *       per value serialization?
 */
public class RdfValueSerializer implements IValueSerializer {

    private static final long serialVersionUID = 6950535691724083790L;

    public static final int VERSION0 = 0x0;
    
    public static transient final IValueSerializer INSTANCE = new RdfValueSerializer();
    
    public static final boolean utfCompression = false;
    
    public RdfValueSerializer() {}
    
    protected void writeUTF(DataOutputBuffer os,String s) throws IOException {
        
        if (utfCompression) {

            byte[] data = UnicodeCompressor.compress(s);

//            LongPacker.packLong(os, data.length);
            os.packLong(data.length);

            os.write(data);

        } else {

            os.writeUTF(s);

        }
        
    }
    
    protected String readUTF(DataInput is) throws IOException {
        
        if(utfCompression) {
        
            int len = (int)LongPacker.unpackLong(is);
            
            byte[] data = new byte[len];
            
            is.readFully(data);
            
            return UnicodeDecompressor.decompress(data);
        
        } else {
            
            return is.readUTF();
            
        }
        
    }
    
    public void getValues(DataInput is, Object[] vals, int n)
            throws IOException {

        final int version = (int)LongPacker.unpackLong(is);
        
        if (version != VERSION0)
            throw new RuntimeException("Unknown version: " + version);
        
        for (int i = 0; i < n; i++) {
            
            final _Value val;
    
            final byte code = is.readByte();

            final String term = readUTF(is); 
            
            switch(code) {
            case RdfKeyBuilder.TERM_CODE_URI:
                val = new _URI(term);
                break;
            case RdfKeyBuilder.TERM_CODE_LIT:
                val = new _Literal(term);
                break;
            case RdfKeyBuilder.TERM_CODE_LCL:
                val = new _Literal(term,readUTF(is));
                break;
            case RdfKeyBuilder.TERM_CODE_DTL:
                val = new _Literal(term,new _URI(readUTF(is)));
                break;
            case RdfKeyBuilder.TERM_CODE_BND:
                val = new _BNode(term);
                break;
            default: throw new AssertionError("Unknown code="+code);
            }

            vals[i] = val;
            
        }
        
    }

    public void putValues(DataOutputBuffer os, Object[] vals, int n)
            throws IOException {

        LongPacker.packLong(os, VERSION0);
        
        for (int i = 0; i < n; i++) {

            _Value value = (_Value)vals[i];
            
            byte code = value.getTermCode();
            
            os.writeByte(code);
            
            writeUTF(os,value.term);
            
            switch(code) {
            case RdfKeyBuilder.TERM_CODE_URI: break;
            case RdfKeyBuilder.TERM_CODE_LIT: break;
            case RdfKeyBuilder.TERM_CODE_LCL:
                writeUTF(os,((_Literal)value).language);
                break;
            case RdfKeyBuilder.TERM_CODE_DTL:
                writeUTF(os,((_Literal)value).datatype.term);
                break;
            case RdfKeyBuilder.TERM_CODE_BND: break;
            default: throw new AssertionError("Unknown code="+code);
            }

        }

    }
    
}
