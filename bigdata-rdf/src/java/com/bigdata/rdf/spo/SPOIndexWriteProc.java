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
 * Created on Jan 25, 2008
 */
package com.bigdata.rdf.spo;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IDataSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.proc.AbstractIndexProcedureConstructor;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Procedure for batch index on a single statement index (or index partition).
 * <p>
 * The key for each statement encodes the {s:p:o} of the statement in the order
 * that is appropriate for the index (SPO, POS, OSP, etc). The key is written
 * unchanged on the index.
 * <p>
 * The value for each statement is a byte that encodes the {@link StatementEnum}
 * and also encodes whether or not the "override" flag is set using - see
 * {@link StatementEnum#MASK_OVERRIDE} - followed by 8 bytes representing the
 * statement identifier IFF statement identifiers are enabled AND the
 * {@link StatementEnum} is {@link StatementEnum#Explicit}. The value requires
 * interpretation to determine the byte[] that will be written as the value on
 * the index - see the code for more details.
 * <p>
 * Note: This needs to be a custom batch operation using a conditional insert so
 * that we do not write on the index when the data would not be changed and to
 * handle the overflow flag and the optional statement identifier correctly.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOIndexWriteProc extends AbstractKeyArrayIndexProcedure implements
        IParallelizableIndexProcedure {

    protected transient static final Logger log = Logger.getLogger(SPOIndexWriteProc.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * 
     */
    private static final long serialVersionUID = 3969394126242598370L;

    private transient final long NULL = IRawTripleStore.NULL;
    
    @Override
    protected IDataSerializer getKeySerializer() {

        return new FastRDFKeyCompression(IRawTripleStore.N);

    }

    @Override
    protected IDataSerializer getValSerializer() {

        return new FastRDFValueCompression();

    }

    public final boolean isReadOnly() {
        
        return false;
        
    }
    
    /**
     * De-serialization constructor.
     */
    public SPOIndexWriteProc() {

    }

    /**
     * 
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @param vals
     */
    protected SPOIndexWriteProc(IDataSerializer keySer,
            IDataSerializer valSer,int fromIndex, int toIndex, byte[][] keys, byte[][] vals) {

        super(keySer,valSer,fromIndex, toIndex, keys, vals);

        assert vals != null;

    }

    public static class IndexWriteProcConstructor extends
            AbstractIndexProcedureConstructor<SPOIndexWriteProc> {

        public static IndexWriteProcConstructor INSTANCE = new IndexWriteProcConstructor();
        
        private IndexWriteProcConstructor() {}
        
        public SPOIndexWriteProc newInstance(IDataSerializer keySer,
                IDataSerializer valSer,int fromIndex, int toIndex,
                byte[][] keys, byte[][] vals) {

            return new SPOIndexWriteProc(keySer,valSer,fromIndex, toIndex, keys, vals);

        }
        
    }
    
    /**
     * 
     * @return The #of statements actually written on the index as an
     *         {@link Long}.
     */
    public Object apply(IIndex ndx) {

        // #of statements actually written on the index partition.
        long writeCount = 0;

        final int n = getKeyCount();

        // used to generate the values that we write on the index.
        final ByteArrayBuffer tmp = new ByteArrayBuffer(1+8/*max size*/);
        
        // true iff logging is enabled and this is the SPO index.
        final boolean isSPO = INFO?ndx.getIndexMetadata().getName().endsWith(SPOKeyOrder.SPO.getIndexName()):false;
        
        for (int i = 0; i < n; i++) {

            // the key encodes the {s:p:o} of the statement.
            final byte[] key = getKey(i);
            assert key != null;

            /*
             * The value encodes the statement type (byte 0). If statement
             * identifiers are enabled and the statement type is explicit, then
             * the value MUST also encode the statement identifier (bytes 1-9).
             * Otherwise the statement identifier MUST NOT be present.
             */
            final byte[] val = getValue(i);
            assert val != null;
            assert val.length == 1 || val.length==9;

            // figure out if the override bit is set.
            final boolean override = StatementEnum.isOverride(val[0]);

            /*
             * Decode the new (proposed) statement type (override bit is
             * masked off).
             */
            final StatementEnum newType = StatementEnum.decode(val[0]);

            /*
             * Decode the new (proposed) statement identifier.
             */
            final long new_sid = decodeStatementIdentifier(newType,val);
            
            /*
             * The current value for the statement in this index partition (or
             * null iff the stmt is not asserted).
             */
            final byte[] oldval = ndx.lookup(key);
            
            /*
             * The following reconciles the old and new statement type and the
             * optional statement identifier, both of which are interpreted in
             * the light of whether or not the override flag was set.
             */
            
            if (oldval == null) {

                /*
                 * Statement is NOT pre-existing.
                 */

                ndx.insert(key, SPO
                        .serializeValue(tmp, false/* override */,
                        newType, new_sid/*MAY be NULL*/));

                if(isSPO&&DEBUG) {
                    log.debug("new SPO: key="+BytesUtil.toString(key)+", sid="+new_sid);
                }
                
                writeCount++;

            } else {

                /*
                 * Statement is pre-existing.
                 */

                // old statement type.
                final StatementEnum oldType = StatementEnum.deserialize(oldval);

                if (override) {

                    if (oldType != newType) {

                        /*
                         * We are downgrading a statement from explicit to
                         * inferred during TM.
                         */

                        assert newType != StatementEnum.Explicit;
                        
                        // Note: No statement identifier since statement is not explicit.
                        ndx.insert(key, SPO
                                .serializeValue(tmp, false/*override*/, newType, NULL/*sid*/));

                        if(isSPO&&DEBUG) {
                            log.debug("Downgrading SPO: key="+BytesUtil.toString(key)+", oldType="+oldType+", newType="+newType);
                        }
                        
                        writeCount++;

                    }

                } else {

                    // choose the max of the old and the proposed type.
                    final StatementEnum maxType = StatementEnum.max(oldType,
                            newType);

                    if (oldType != maxType) {

                        /*
                         * Write on the index iff the type was actually changed.
                         * 
                         * Note: The [sid] will be NULL regardless of the code
                         * path below if statement identifiers are not enabled.
                         */

                        final long sid;

                        if (maxType == oldType) {

                            // If the old statement was explicit then use its sid.
                            sid = decodeStatementIdentifier(oldType, oldval);
                            
                        } else {
                            
                            // Otherwise use the new sid.
                            sid = new_sid;
                            
                        }

                        ndx.insert(key, SPO
                                .serializeValue(tmp, false/*override*/, maxType, sid));

                        if(isSPO&&DEBUG) {
                            log.debug("Changing statement type: key="+BytesUtil.toString(key)+", oldType="+oldType+", newType="+newType+", maxType="+maxType+", sid="+sid);
                        }
                        
                        writeCount++;

                    }

                }

            }

        }

        if (isSPO&&INFO)
            log.info("Wrote " + writeCount + " SPOs on ndx="
                    + ndx.getIndexMetadata().getName());
        
        return Long.valueOf(writeCount);

    }

    /**
     * Decodes and validate the statement identifier from the value.
     * 
     * @param type
     *            The statement type.
     * @param val
     *            The value.
     * 
     * @return The statement identifier if the statement is
     *         {@link StatementEnum#Explicit} and statement identifiers are
     *         enabled and otherwise {@link IRawTripleStore#NULL}.
     * 
     * @throws RuntimeException
     *             if validation fails.
     */
    protected long decodeStatementIdentifier(StatementEnum type, byte[] val) {
        
        final long sid;

        if (type == StatementEnum.Explicit && val.length == 9) {

            /*
             * An explicit statement with statement identifiers enabled.
             */

            vbuf.setBuffer(val, 1, 8);

            try {

                sid = vbuf.readLong();

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

            assert AbstractTripleStore.isStatement(sid) : "" + sid;

        } else {
            
            /*
             * Any type of statement {axiom, inferred, or explicit} but
             * statement identifiers must not be present.
             */

            assert val.length == 1 : "value length=" + val.length;

            sid = IRawTripleStore.NULL;

        }

        return sid;

    }

    /**
     * Used by {@link #decodeStatementIdentifier(StatementEnum, byte[])}
     */
    private transient final DataInputBuffer vbuf = new DataInputBuffer(
            new byte[] {});

}
