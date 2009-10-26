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
 * Created on Jan 26, 2007
 */

package com.bigdata.rdf.spo;

import java.io.Externalizable;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.striterator.IKeyOrder;

/**
 * Represents the key order used by an index for a triple relation.
 * 
 * @serial The serialization of the class is quite small since the only instance
 *         field is {@link #index()}. All other data are static. However, it is
 *         still MUCH more efficient to only transmit the {@link #index()} byte
 *         without the overhead of the class metadata, which is an additional
 *         <strong>60</strong> bytes! Classes embedding serialized
 *         {@link SPOKeyOrder} are strongly encouraged to make this
 *         optimization.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOKeyOrder implements IKeyOrder<ISPO>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 87501920529732159L;
    
    static private final transient long NULL = IRawTripleStore.NULL;

    /*
     * Note: these constants make it possible to use switch(index()) constructs.
     */
    // triple store indices.
    public static final transient int _SPO = 0;
    public static final transient int _OSP = 1;
    public static final transient int _POS = 2;
    // quad store indices.
    public static final transient int _SPOC = 3;
    public static final transient int _POCS = 4;
    public static final transient int _OCSP = 5;
    public static final transient int _CSPO = 6;
    public static final transient int _PCSO = 7;
    public static final transient int _SOPC = 8;

    public static final transient int FIRST_TRIPLE_INDEX = _SPO;
    public static final transient int LAST_TRIPLE_INDEX = _POS;

    public static final transient int FIRST_QUAD_INDEX = _SPOC;
    public static final transient int LAST_QUAD_INDEX = _SOPC;

    public static final transient int MAX_INDEX_COUNT = 9;
    
    /*
     * The three perfect natural orders for triples.
     */

    // FIXME quads : review ALL use of the SPO index (92 refs)
    public static final transient SPOKeyOrder SPO = new SPOKeyOrder(_SPO);
    
    // FIXME quads : review ALL use of the POS index (39 refs)
    public static final transient SPOKeyOrder POS = new SPOKeyOrder(_POS);

    // FIXME quads : review ALL use of the OSP index (31 refs)
    public static final transient SPOKeyOrder OSP = new SPOKeyOrder(_OSP);

    /*
     * The six perfect natural orders for quads.
     */

    public static final transient SPOKeyOrder SPOC = new SPOKeyOrder(_SPOC);
    public static final transient SPOKeyOrder POCS = new SPOKeyOrder(_POCS);
    public static final transient SPOKeyOrder OCSP = new SPOKeyOrder(_OCSP);
    public static final transient SPOKeyOrder CSPO = new SPOKeyOrder(_CSPO);
    public static final transient SPOKeyOrder PCSO = new SPOKeyOrder(_PCSO);
    public static final transient SPOKeyOrder SOPC = new SPOKeyOrder(_SOPC);

    /**
     * The name for each of the natural key orders.
     */
    static final transient String[] names = new String[] {
        // triples
        "SPO",//
        "OSP",//
        "POS",//
        // quads
        "SPOC",//
        "POCS",//
        "OCSP",//
        "CSPO",//
        "PCSO",//
        "SOPC" //
    };
    
    static final transient SPOKeyOrder[] values = new SPOKeyOrder[] {
        // triples
        SPO,
        OSP,
        POS,
        // quads
        SPOC,
        POCS,
        OCSP,
        CSPO,
        PCSO,
        SOPC,
        
    };

    static final transient GeneralComparator[] comparators = new GeneralComparator[] {
            // triples
            new GeneralComparator(_SPO),
            new GeneralComparator(_OSP),
            new GeneralComparator(_POS),
            // quads
            new GeneralComparator(_SPOC),//
            new GeneralComparator(_POCS),//
            new GeneralComparator(_OCSP),//
            new GeneralComparator(_CSPO),//
            new GeneralComparator(_PCSO),//
            new GeneralComparator(_SOPC) };

    /*
     * Constants corresponding to the columns of the SPO(C) relation.
     */
    private final static transient int S = 0;
    private final static transient int P = 1;
    private final static transient int O = 2;
    private final static transient int C = 3;
    
    /**
     * The permutation order for the keys for each of the natural key orders.
     */
    static final transient int[][] orders = new int[][] {
    // triples
            new int[] { S, P, O }, // 
            new int[] { O, S, P }, // 
            new int[] { P, O, S }, // 
    // quads
            new int[] { S, P, O, C }, // 
            new int[] { P, O, C, S }, // 
            new int[] { O, C, S, P }, // 
            new int[] { C, S, P, O }, // 
            new int[] { P, C, S, O }, // 
            new int[] { S, O, P, C }, // 
    };

    /**
     * The unique index used to identify this natural order.
     */
    private final byte index;

    private SPOKeyOrder(final int index) {

        this.index = (byte) index;

    }

    /**
     * Return <code>true</code> if this is the primary index for the relation.
     * 
     * @return <code>true</code> for {@link #SPO} or {@link #SPOC}. Those are
     *         the natural orders corresponding to the primary index for a
     *         triple store (SPO) and a quad store (SPOC) respectively.
     */
    final public boolean isPrimaryIndex() {
        
        return this == SPO || this == SPOC;
        
    }
    
    /**
     * Returns the singleton corresponding to the <i>index</i>.
     * 
     * @param index
     *            The index.
     * 
     * @return The singleton {@link SPOKeyOrder} having that <i>index</i>.
     * 
     * @throws IllegalArgumentException
     *             if the <i>index</i> is not valid.
     */
    static public SPOKeyOrder valueOf(final int index) {
        
        return values[index];
        
//        switch(index) {
//        case _SPO:
//            return SPO;
//        case _POS:
//            return POS;
//        case _OSP:
//            return OSP;
//        default:
//            throw new IllegalArgumentException("Unknown: index" + index);
//        }
        
    }
    
    /**
     * The base name for the index.
     */
    final public String getIndexName() {

        return names[index];
        
    }
    
    /**
     * Return {@link #getIndexName()}'s value.
     */
    public String toString() {
        
        return names[index];
        
    }

    /**
     * Return either 3 or 4 depending on the #of components in the key for
     * this natural key ordering.
     */
    final public int getKeyArity() {

        switch (index) {
        case _SPO:
        case _POS:
        case _OSP:
            return 3;
        case _SPOC:
        case _POCS:
        case _OCSP:
        case _CSPO:
        case _PCSO:
        case _SOPC:
            return 4;
        default:
            throw new AssertionError();
        }

    }

    /**
     * Return the index of the slot in the {@link ISPO} tuple which appears at
     * the specified position in the key.
     * 
     * @param keyPos
     *            The index into the key that is being generated.
     *            
     * @return The index of the slot in the {@link ISPO}.
     */
    final public int getKeyOrder(final int keyPos) {

        return orders[index][keyPos];

    }
    
    /**
     * The integer used to represent the {@link SPOKeyOrder}. For a triple
     * store, this is one of the constants: {@link #_SPO}, {@link #POS}, or
     * {@link #OSP}. For a quad store, this is one of the constants
     * {@link #_SPOC}, {@link #_POCS}, {@link _OCSP}, {@link #_CSPO},
     * {@link #_PCSO}, {@link #_SOPC}.
     */
    final public int index() {
        
        return index;
        
    }

    /**
     * Return the comparator that places {@link ISPO}s into the natural order
     * for the associated index.
     * 
     * @todo performance comparison if we get rid of the {@link SPOComparator},
     *       {@link POSComparator}, and {@link OSPComparator} and just use the
     *       {@link GeneralComparator}.  Will the hot spot compiler do better
     *       with just one class handling all of those comparisons?
     */
    final public Comparator<ISPO> getComparator() {

        switch (index) {
        case _SPO:
            return SPOComparator.INSTANCE;
        case _POS:
            return POSComparator.INSTANCE;
        case _OSP:
            return OSPComparator.INSTANCE;
        case _SPOC:
        case _POCS:
        case _OCSP:
        case _CSPO:
        case _PCSO:
        case _SOPC:
            return comparators[index];
        default:
            throw new AssertionError();
        }

    }

    /**
     * Generalized comparator for {@link ISPO}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class GeneralComparator implements Comparator<ISPO> {

        private final byte index;

        public GeneralComparator(final int index) {

            this.index = (byte)index;

        }

        public int compare(final ISPO o1, final ISPO o2) {

            if (o1 == o2) {
                return 0;
            }

            final int[] keyMap = orders[index];

            // compare terms one by one in the appropriate key order
            for (int i = 0; i < keyMap.length; i++) {
                
                final long t1 = o1.get(keyMap[i]);
                
                final long t2 = o2.get(keyMap[i]);
                
                int ret = t1 < t2 ? -1 : t1 > t2 ? 1 : 0;
                
                if (ret != 0) {
                
                    return ret;
                    
                }
                
            }

            // all terms match
            return 0;

        }

    }

    /**
     * Return the inclusive lower bound which would be used for a query against
     * this {@link IKeyOrder} for the given {@link IPredicate}.
     * 
     * @todo This method should be declared by {@link IKeyOrder}.
     */
    final public byte[] getFromKey(final IKeyBuilder keyBuilder,
            final IPredicate<ISPO> predicate) {

        final int keyArity = getKeyArity(); // use the key's "arity".

        keyBuilder.reset();

        boolean noneBound = true;
        
        for (int i = 0; i < keyArity; i++) {
        
            final IVariableOrConstant<Long> term = predicate.get(getKeyOrder(i));
            
            final long l;
            
            // Note: term MAY be null for the context position.
            if (term == null || term.isVar()) {
            
                l = Long.MIN_VALUE;
                
            } else {
                
                l = term.get();

                noneBound = false;
                
            }

            keyBuilder.append(l);
            
        }

        return noneBound ? null : keyBuilder.getKey();

    }

    /**
     * Return the exclusive upper bound which would be used for a query against
     * this {@link IKeyOrder} for the given {@link IPredicate}.
     * 
     * @todo This method should be declared by {@link IKeyOrder}.
     */
    final public byte[] getToKey(final IKeyBuilder keyBuilder,
            final IPredicate<ISPO> predicate) {

        keyBuilder.reset();

        final int keyArity = getKeyArity();
        
        boolean noneBound = true;
        
        boolean foundLastBound = false;
        
        for (int i = 0; i < keyArity; i++) {
        
            final IVariableOrConstant<Long> term = predicate
                    .get(getKeyOrder(i));
            
            long l;
            
            // Note: term MAY be null for context.
            if (term == null || term.isVar()) {
            
                l = Long.MIN_VALUE;
                
            } else {
                
                l = term.get();
                
                noneBound = false;
                
                if (!foundLastBound) {
                
                    if (i == keyArity - 1) {
                    
                        l++;
                        
                        foundLastBound = true;
                        
                    } else {
                        
                        final IVariableOrConstant<Long> next = predicate
                                .get(getKeyOrder(i + 1));
                        
                        // Note: next can be null for quads (context pos).
                        if (next == null || next.isVar()) {
                        
                            l++;
                            
                            foundLastBound = true;
                            
                        }
                        
                    }
                    
                }
                
            }
            
            keyBuilder.append(l);
            
        }

        return noneBound ? null : keyBuilder.getKey();

    }
    
    final public byte[] encodeKey(final IKeyBuilder keyBuilder, final ISPO spo) {

        keyBuilder.reset();

        final int[] a = orders[index];

        for (int i = 0; i < a.length; i++) {

            keyBuilder.append(spo.get(a[i]));

        }
        
        return keyBuilder.getKey();

    }
    
    /**
     * Decode the key into an {@link SPO}. The {@link StatementEnum} and the
     * optional SID will not be decoded, since it is carried in the B+Tree
     * value. However, if the {@link SPOKeyOrder} is a quad order then the
     * {@link SPO#c()} will be bound.
     * 
     * @param keyOrder
     *            The natural order of the key.
     * @param key
     *            The key.
     * 
     * @return The decoded key.
     */
    final public SPO decodeKey(final byte[] key) {
        
        /*
         * Note: GTE since the key is typically a reused buffer which may be
         * larger than the #of bytes actually holding valid data.
         */
        final int keyArity = getKeyArity();

        assert key.length >= 8 * keyArity;

        final long _0 = KeyBuilder.decodeLong(key, 0);
        
        final long _1 = KeyBuilder.decodeLong(key, 8);
      
        final long _2 = KeyBuilder.decodeLong(key, 8+8);

        // 4th key position exists iff quad keys.
        final long _3 = keyArity == 4 ? KeyBuilder.decodeLong(key, 8 + 8 + 8)
                : IRawTripleStore.NULL;

        /*
         * Re-order the key into SPO order.
         */
        
        final long s, p, o, c;
        
        switch (index) {

        /*
         * Triples
         * 
         * [c] will be NULL for triples, but the SID may be read from the value
         * associated with the key below and set on the SPO object.
         */

        case SPOKeyOrder._SPO:
            s = _0;
            p = _1;
            o = _2;
            c = IRawTripleStore.NULL;
            break;
            
        case SPOKeyOrder._POS:
            p = _0;
            o = _1;
            s = _2;
            c = IRawTripleStore.NULL;
            break;
            
        case SPOKeyOrder._OSP:
            o = _0;
            s = _1;
            p = _2;
            c = IRawTripleStore.NULL;
            break;

        /*
         * Quads
         */

        case SPOKeyOrder._SPOC:
            s = _0;
            p = _1;
            o = _2;
            c = _3;
            break;
            
        case SPOKeyOrder._POCS:
            p = _0;
            o = _1;
            c = _2;
            s = _3;
            break;

        case SPOKeyOrder._OCSP:
            o = _0;
            c = _1;
            s = _2;
            p = _3;
            break;

        case SPOKeyOrder._CSPO:
            c = _0;
            s = _1;
            p = _2;
            o = _3;
            break;

        case SPOKeyOrder._PCSO:
            p = _0;
            c = _1;
            s = _2;
            o = _3;
            break;

        case SPOKeyOrder._SOPC:
            s = _0;
            o = _1;
            p = _2;
            c = _3;
            break;

        default:

            throw new UnsupportedOperationException();

        }
    
        return new SPO(s, p, o, c);

    }

    /**
     * Imposes the canonicalizing mapping during object de-serialization.
     * <p>
     * Note: implementing {@link Externalizable} drops the serialized size from
     * 61 bytes per instance to 56 bytes per instance. On the other hand, if the
     * class embedding the {@link SPOKeyOrder} serializes the {@link #index} as
     * a <code>byte</code>, it only take a single byte to serialize each
     * instance.
     * <p>
     * Note: Serialization breaks with the introduction of quads as the
     * <code>name</code> field is no longer serialized and the {@link #index()}
     * is serialized as a byte field.
     */
    private Object readResolve() throws ObjectStreamException {

        return SPOKeyOrder.valueOf(index);

    }

    /**
     * Return the {@link SPOKeyOrder} for the given predicate.
     * 
     * @param predicate
     *            The predicate.
     * 
     * @return The {@link SPOKeyOrder}
     * 
     * @todo A variant of this method should be raised onto IKeyOrder without
     *       the keyArity parameter. That parameter is only there because we
     *       support two distinct families of natural orders in this class: one
     *       for triples and one for quads.
     */
    static public SPOKeyOrder getKeyOrder(final IPredicate<ISPO> predicate,
            final int keyArity) {

        final long s = predicate.get(0).isVar() ? NULL : (Long) predicate
                .get(0).get();
        
        final long p = predicate.get(1).isVar() ? NULL : (Long) predicate
                .get(1).get();
        
        final long o = predicate.get(2).isVar() ? NULL : (Long) predicate
                .get(2).get();

        if (keyArity == 3) {

            // Note: Context is ignored!

            if (s != NULL && p != NULL && o != NULL) {
                return SPO;
            } else if (s != NULL && p != NULL) {
                return SPO;
            } else if (s != NULL && o != NULL) {
                return OSP;
            } else if (p != NULL && o != NULL) {
                return POS;
            } else if (s != NULL) {
                return SPO;
            } else if (p != NULL) {
                return POS;
            } else if (o != NULL) {
                return OSP;
            } else {
                return SPO;
            }

        } else {

            @SuppressWarnings("unchecked")
            final IVariableOrConstant<Long> t = predicate.get(3);
            
            final long c = t == null ? NULL : (t.isVar() ? NULL : t.get());
            
            /*
             * if ((s == NULL && p == NULL && o == NULL && c == NULL) || (s !=
             * NULL && p == NULL && o == NULL && c == NULL) || (s != NULL && p
             * != NULL && o == NULL && c == NULL) || (s != NULL && p != NULL &&
             * o != NULL && c == NULL) || (s != NULL && p != NULL && o != NULL
             * && c != NULL)) { return SPOKeyOrder.SPOC; }
             */
            
            if ((s == NULL && p != NULL && o == NULL && c == NULL)
                    || (s == NULL && p != NULL && o != NULL && c == NULL)
                    || (s == NULL && p != NULL && o != NULL && c != NULL)) {
                return POCS;
            }

            if ((s == NULL && p == NULL && o != NULL && c == NULL)
                    || (s == NULL && p == NULL && o != NULL && c != NULL)
                    || (s != NULL && p == NULL && o != NULL && c != NULL)) {
                return OCSP;
            }

            if ((s == NULL && p == NULL && o == NULL && c != NULL)
                    || (s != NULL && p == NULL && o == NULL && c != NULL)
                    || (s != NULL && p != NULL && o == NULL && c != NULL)) {
                return CSPO;
            }

            if ((s == NULL && p != NULL && o == NULL && c != NULL)) {
                return PCSO;
            }

            if ((s != NULL && p == NULL && o != NULL && c == NULL)) {
                return SOPC;
            }

            return SPOC;

        }

    }

    /**
     * Iterator visits {@link #SPO}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static private class SPOOnlyKeyOrderIterator implements
            Iterator<SPOKeyOrder> {

        boolean exhausted = false;

        public boolean hasNext() {
            return !exhausted;
        }

        public SPOKeyOrder next() {
            if (!hasNext())
                throw new NoSuchElementException();
            exhausted = true;
            return SPOKeyOrder.SPO;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * Iterator visits {@link #SPOC}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static private class SPOCOnlyKeyOrderIterator implements
            Iterator<SPOKeyOrder> {

        boolean exhausted = false;

        public boolean hasNext() {
            return !exhausted;
        }

        public SPOKeyOrder next() {
            if (!hasNext())
                throw new NoSuchElementException();
            exhausted = true;
            return SPOKeyOrder.SPOC;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * The triple store indices.
     */
    static private final transient SPOKeyOrder[] tripleStoreIndices = { SPO,
            POS, OSP };

    /**
     * The quad store indices.
     */
    static private final transient SPOKeyOrder[] quadStoreIndices = { SPOC,
            POCS, OCSP, CSPO, PCSO, SOPC };

    /**
     * Return an iterator which visits the triple store indices ({@link #SPO},
     * {@link #POS}, {@link #OSP}).
     */
    static public Iterator<SPOKeyOrder> tripleStoreKeyOrderIterator() {

        return Arrays.asList(tripleStoreIndices).iterator();

    }

    /**
     * Return an iterator which visits the quad store indices ({@link #SPOC},
     * {@link #POCS}, {@link #OCSP}, {@link #CSPO}, {@link #PCSO}, {@link #SOPC}
     * ).
     */
    static public Iterator<SPOKeyOrder> quadStoreKeyOrderIterator() {

        return Arrays.asList(quadStoreIndices).iterator();

    }

    /**
     * Return an iterator which visits only {@link #SPO}.
     */
    static public Iterator<SPOKeyOrder> spoOnlyKeyOrderIterator() {
        
        return new SPOOnlyKeyOrderIterator();
        
    }
    
    /**
     * Return an iterator which visits only {@link #SPOC}.
     */
    static public Iterator<SPOKeyOrder> spocOnlyKeyOrderIterator() {
        
        return new SPOCOnlyKeyOrderIterator();
        
    }

}
