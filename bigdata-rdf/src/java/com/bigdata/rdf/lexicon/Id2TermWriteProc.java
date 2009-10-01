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
/*
 * Created on May 21, 2007
 */
package com.bigdata.rdf.lexicon;

import org.openrdf.model.impl.ValueFactoryImpl;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.compression.IDataSerializer;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.IMutableRelationIndexWriteProcedure;

/**
 * Unisolated write operation makes consistent assertions on the
 * <em>id:term</em> index based on the data developed by the {@link Term2IdWriteProc}
 * operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Id2TermWriteProc extends AbstractKeyArrayIndexProcedure implements
        IParallelizableIndexProcedure, IMutableRelationIndexWriteProcedure {

    /**
     * 
     */
    private static final long serialVersionUID = -5480378815444534653L;

    /**
     * Enables validation that a pre-assigned term identifier is being
     * consistently mapped onto the same term. Errors are reported if, for
     * example, the index has a record that a term identifier is mapped onto one
     * URL but the procedure was invoked with a different URI paired to that
     * term identifiers. When such errors are reported, they generally indicate
     * a problem with the TERM2ID index where it is failing to maintain a
     * consistent mapping.
     * <p>
     * Validation may be disabled for releases, however it is not really that
     * much overhead since the operation is on the in-memory representation.
     *
     * @deprecated Validation can not be reasonably applied it the
     * Unicode collation is less than Identical.  */
    static protected transient final boolean validate = false;
    
    public final boolean isReadOnly() {
        
        return false;
        
    }
    
    /**
     * De-serialization constructor.
     */
    public Id2TermWriteProc() {
        
    }
    
    protected Id2TermWriteProc(IDataSerializer keySer, IDataSerializer valSer,
            int fromIndex, int toIndex, byte[][] keys, byte[][] vals) {

        super(keySer, valSer, fromIndex, toIndex, keys, vals);
        
        assert vals != null;
        
    }
    
    public static class Id2TermWriteProcConstructor extends
            AbstractKeyArrayIndexProcedureConstructor<Id2TermWriteProc> {

        public static Id2TermWriteProcConstructor INSTANCE = new Id2TermWriteProcConstructor();

        /**
         * Values are required.
         */
        public final boolean sendValues() {
            
            return true;
            
        }

        private Id2TermWriteProcConstructor() {}
        
        public Id2TermWriteProc newInstance(IDataSerializer keySer,
                IDataSerializer valSer,int fromIndex, int toIndex,
                byte[][] keys, byte[][] vals) {

            return new Id2TermWriteProc(keySer,valSer,fromIndex, toIndex, keys, vals);

        }

    }

    /**
     * Conditionally inserts each key-value pair into the index. The keys are
     * the term identifiers. The values are the terms as serialized by
     * {@link BigdataValueSerializer}. Since a conditional insert is used, the
     * operation does not cause terms that are already known to the ids index to
     * be re-inserted, thereby reducing writes of dirty index nodes.
     * 
     * @param ndx
     *            The index.
     * 
     * @return <code>null</code>.
     */
    public Object apply(IIndex ndx) {
        
        final int n = getKeyCount();
        
        for (int i = 0; i < n; i++) {

            // Note: the key is the term identifier.
            final byte[] key = getKey(i);
            
            // Note: the value is the serialized term (and never a BNode).
            final byte[] val;

            if (validate) {

                // The term identifier.
                final long id = KeyBuilder.decodeLong(key, 0);

                assert id != IRawTripleStore.NULL;
                
                // Note: BNodes are not allowed in the reverse index.
                assert ! AbstractTripleStore.isBNode(id);
                
                // Note: SIDS are not allowed in the reverse index.
                assert ! AbstractTripleStore.isStatement(id);
                
                /*
                 * When the term identifier is found in the reverse mapping
                 * this code path validates that the serialized term is the
                 * same.
                 */
                final byte[] oldval = ndx.lookup(key);
                
                val = getValue(i);
                
                if( oldval == null ) {
                    
                    if (ndx.insert(key, val) != null) {

                        throw new AssertionError();

                    }
                    
                } else {

                    /*
                     * Note: This would fail if the serialization of the term
                     * was changed for an existing database instance. In order
                     * to validate when different serialization formats might be
                     * in use you have to actually deserialize the terms.
                     * However, I have the validation logic here just as a
                     * sanity check while getting the basic system running - it
                     * is not meant to be deployed.
                     */

                    if (! BytesUtil.bytesEqual(val, oldval)) {

                        final char suffix;
                        if (AbstractTripleStore.isLiteral(id))
                            suffix = 'L';
                        else if (AbstractTripleStore.isURI(id))
                            suffix = 'U';
                        else if (AbstractTripleStore.isBNode(id))
                            suffix = 'B';
                        else if (AbstractTripleStore.isStatement(id))
                            suffix = 'S';
                        else
                            suffix = '?';

                        // note: solely for de-serialization of values for error logging.
                        final BigdataValueSerializer valSer = new BigdataValueSerializer(
                                new ValueFactoryImpl());
                        log.error("id=" + id + suffix);
                        log.error("val=" + BytesUtil.toString(val));
                        log.error("oldval=" + BytesUtil.toString(oldval));
                        log.error("val=" + valSer.deserialize(val));
                        log.error("oldval=" + valSer.deserialize(oldval));
                        if(ndx.getIndexMetadata().getPartitionMetadata()!=null)
                            log.error(ndx.getIndexMetadata().getPartitionMetadata().toString());
                        
                        throw new RuntimeException("Consistency problem: id="+ id);
                        
                        
                    }
                    
                }
                
            } else {
                
                /*
                 * This code path does not validate that the term identifier
                 * is mapped to the same term. This is the code path that
                 * you SHOULD use.
                 */

                if (!ndx.contains(key)) {

                    val = getValue(i);
                    
                    if (ndx.insert(key, val) != null) {

                        throw new AssertionError();

                    }

                }

            }
            
        }
        
        return null;
        
    }

}
