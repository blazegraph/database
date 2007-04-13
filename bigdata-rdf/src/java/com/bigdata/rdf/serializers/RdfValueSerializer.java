/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
package com.bigdata.rdf.serializers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.CognitiveWeb.extser.LongPacker;
import org.openrdf.model.Value;

import com.bigdata.btree.DataOutputBuffer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IValueSerializer;
import com.bigdata.rdf.RdfKeyBuilder;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
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
            case RdfKeyBuilder.CODE_URI:
                val = new _URI(term);
                break;
            case RdfKeyBuilder.CODE_LIT:
                val = new _Literal(term);
                break;
            case RdfKeyBuilder.CODE_LCL:
                val = new _Literal(term,readUTF(is));
                break;
            case RdfKeyBuilder.CODE_DTL:
                val = new _Literal(term,new _URI(readUTF(is)));
                break;
            case RdfKeyBuilder.CODE_BND:
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
            case RdfKeyBuilder.CODE_URI: break;
            case RdfKeyBuilder.CODE_LIT: break;
            case RdfKeyBuilder.CODE_LCL:
                writeUTF(os,((_Literal)value).language);
                break;
            case RdfKeyBuilder.CODE_DTL:
                writeUTF(os,((_Literal)value).datatype.term);
                break;
            case RdfKeyBuilder.CODE_BND: break;
            default: throw new AssertionError("Unknown code="+code);
            }

        }

    }
    
}