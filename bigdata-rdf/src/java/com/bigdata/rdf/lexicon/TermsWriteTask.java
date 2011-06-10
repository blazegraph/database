package com.bigdata.rdf.lexicon;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.TermsWriteProc.TermsWriteProcConstructor;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.service.Split;
import com.bigdata.service.ndx.pipeline.IDuplicateRemover;
import com.bigdata.service.ndx.pipeline.KVOC;
import com.bigdata.service.ndx.pipeline.KVOList;

/**
 * Synchronous RPC write on the TERMS index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: Term2IdWriteTask.java 3408 2010-08-04 18:53:35Z thompsonbry $
 */
public class TermsWriteTask implements Callable<KVO<BigdataValue>[]> {

    private static transient final Logger log = Logger.getLogger(TermsWriteTask.class);
            
//    private final LexiconRelation r;
    final IIndex ndx;
    final BigdataValueFactory valueFactory;
    private final boolean readOnly;
    private final boolean storeBlankNodes;
    private final int numTerms;
    private final BigdataValue[] terms;
    private final WriteTaskStats stats;
    
    public TermsWriteTask(final IIndex ndx, final BigdataValueFactory valueFactory,
            final boolean readOnly,
            final boolean storeBlankNodes, final int numTerms,
            final BigdataValue[] terms, final WriteTaskStats stats) {

        if (ndx == null)
            throw new IllegalArgumentException();

        if (valueFactory == null)
            throw new IllegalArgumentException();

        if (terms == null)
            throw new IllegalArgumentException();

        if (numTerms < 0 || numTerms > terms.length)
            throw new IllegalArgumentException();

        if (stats == null)
            throw new IllegalArgumentException();

//        this.r = r;
        
        this.ndx = ndx;
        
        this.valueFactory = valueFactory;

        this.readOnly = readOnly;
        
        this.storeBlankNodes = storeBlankNodes;
        
        this.numTerms = numTerms;
        
        this.terms = terms;
        
        this.stats = stats;
        
    }

    /**
     * Unify the {@link BigdataValue}s with the TERMS index, setting the
     * {@link IV}s on the {@link BigdataValue}s as a side-effect.
     * 
     * @return A dense {@link KVO}[] chunk consisting of only those distinct
     *         {@link BigdataValue}s whose {@link IV}s were not already known.
     *         (This may be used to write on the full text index).
     * 
     * @throws Exception
     */
    public KVO<BigdataValue>[] call() throws Exception {

		/*
		 * Insert into the TERMS index ({termCode,hash(Value),counter} ->
		 * Value). This will set the IV on the BigdataValue. If the Value was
		 * not in the lexicon, then a new entry is created in the TERMS index
		 * for the Value and the key for that entry is wrapped as its IV. If the
		 * Value is in the lexicon, then the key for the existing entry is
		 * wrapped as its IV.
		 * 
		 * Note: The code has to scan the "collision bucket" comprised of each
		 * Value having the same hash code. In practice, collisions are quite
		 * rare and the #of tuples in each "collision bucket" is quite small.
		 * 
		 * Note: TERMS index shards must not split "collision buckets".
		 */
        
        // The #of distinct terms lacking a pre-assigned term identifier in [a].
        int ndistinct = 0;

        // A dense array of correlated tuples.
        final KVO<BigdataValue>[] a;
        {
            
            final KVO<BigdataValue>[] b;

            /*
             * Make sure that each term has an assigned sort key.
             * 
             * Note: The caller SHOULD first remove anything with an
             * pre-assigned IV. That will let us avoid any further costs
             * associated with those Values (LexiconRelation is doing this.)
             */
            {

                long _begin = System.currentTimeMillis();
                
                // may contain duplicates and/or terms with pre-assigned term identifiers.
                b = new TermsIndexHelper().generateKVOs(valueFactory
                        .getValueSerializer(), terms, numTerms);

                stats.keyGenTime = System.currentTimeMillis() - _begin;

            }

            /*
             * Sort by the assigned sort key. This places the array into the
             * natural order for the term:id index.
             */
            {

                final long _begin = System.currentTimeMillis();

                Arrays.sort(b);

                stats.sortTime += System.currentTimeMillis() - _begin;

            }

            /*
             * For each distinct term that does not have a pre-assigned IV, add
             * it to a remote unisolated batch operation that assigns IVs.
             * 
             * Note: Both duplicate term references and terms with their IVs
             * already assigned are dropped out in this step.
             * 
             * FIXME Caller SHOULD first remove any duplicates, which is what we
             * are doing here.
             */
            {

                final long _begin = System.currentTimeMillis();

                /*
                 * Create a key buffer holding the sort keys. This does not
                 * allocate new storage for the sort keys, but rather aligns the
                 * data structures for the call to splitKeys(). This also makes
                 * a[] into a dense copy of the references in b[], but without
                 * duplicates and without terms that already have assigned term
                 * identifiers. Note that keys[] and a[] are correlated.
                 * 
                 * @todo Could be restated as an IDuplicateRemover, but note
                 * that this case is specialized since it can drop terms whose
                 * term identifier is known (they do not need to be written on
                 * T2ID, but they still need to be written on the reverse index
                 * to ensure a robust and consistent mapping).
                 */
                final byte[][] keys = new byte[numTerms][];
                final byte[][] vals = new byte[numTerms][];
                a = new KVO[numTerms];
                {

                    for (int i = 0; i < numTerms; i++) {

                        if (b[i].obj.getIV() != null) {
                            
                            if (log.isDebugEnabled())
                                log.debug("IV already assigned: "
                                        + b[i].obj);
                            
                            // IV is already assigned.
                            continue;
                            
                        }
                        
                        if (i > 0 && b[i - 1].obj == b[i].obj) {

                            if (log.isDebugEnabled())
                                log.debug("duplicate reference: "
                                        + b[i].obj);
                            
                            // duplicate reference.
                            continue;
                            
                        }

                        // assign to a[] (dense variant of b[]).
                        a[ndistinct] = b[i];
                        
                        // assign to keys[]/vals[] (dense; correlated with a[]).
                        keys[ndistinct] = b[i].key;
                        vals[ndistinct] = b[i].val;
                        
                        ndistinct++;

                    }

                }

                if (ndistinct == 0) {
                    
                    /*
                     * Nothing to be written.
                     */
                    
                    return null;// new KVO[0];
                    
                }
                
                final AbstractKeyArrayIndexProcedureConstructor ctor = new TermsWriteProcConstructor(
                        readOnly, storeBlankNodes);

				// run the procedure.
                ndx.submit(0/* fromIndex */, ndistinct/* toIndex */, keys,
                        vals, ctor, new TermsWriteProcResultHandler(a,
                                readOnly, stats.nunknown));

                stats.indexTime = stats.termsIndexTime = System.currentTimeMillis()
                        - _begin;

            }

        }
        
        stats.ndistinct = ndistinct;

        return KVO.dense(a, ndistinct);
        
    } // call

    /**
     * Class applies the term identifiers assigned by the
     * {@link Term2IdWriteProc} to the {@link BigdataValue} references in the
     * {@link KVO} correlated with each {@link Split} of data processed by that
     * procedure.
     * <p>
     * Note: Of necessity, this requires access to the {@link BigdataValue}s
     * whose term identifiers are being resolved. This implementation presumes
     * that the array specified to the ctor and the array returned for each
     * chunk that is processed have correlated indices and that the offset into
     * {@link #a} is given by {@link Split#fromIndex}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id: Term2IdWriteTask.java 3408 2010-08-04 18:53:35Z thompsonbry $
     */
	static class TermsWriteProcResultHandler implements
			IResultHandler<TermsWriteProc.Result, Void> {

        private final KVO<BigdataValue>[] a;
        private final boolean readOnly;
        
        /**
         * @todo this could be the value returned by {@link #getResult()} which
         *       would make the API simpler.
         */ 
        private final AtomicInteger nunknown;
        
        /**
         * 
         * @param a
         *            A dense array of {@link KVO}s.
         * @param readOnly
         *            if readOnly was specified for the {@link Term2IdWriteProc}.
         * @param nunknown
         *            Incremented as a side effect for each terms that could not
         *            be resolved (iff readOnly == true).
         */
		public TermsWriteProcResultHandler(final KVO<BigdataValue>[] a,
				final boolean readOnly, final AtomicInteger nunknown) {

            if (a == null)
                throw new IllegalArgumentException();

            if (nunknown == null)
                throw new IllegalArgumentException();

            this.a = a;

            this.readOnly = readOnly;
            
            this.nunknown = nunknown;
            
        }

        /**
         * Copy the assigned / discovered term identifiers onto the
         * corresponding elements of the terms[].
         */
        public void aggregate(final TermsWriteProc.Result result,
                final Split split) {

            for (int i = split.fromIndex, j = 0; i < split.toIndex; i++, j++) {

                final IV iv = result.ivs[j];

                if (iv == null) {

                    if (!readOnly)
                        throw new AssertionError();

                    nunknown.incrementAndGet();

                } else {

                    // assign the term identifier.
                    a[i].obj.setIV(iv);

                    if(a[i] instanceof KVOList) {
                        
                        final KVOList<BigdataValue> tmp = (KVOList<BigdataValue>) a[i];

                        if (!tmp.isDuplicateListEmpty()) {

                            // assign the term identifier to the duplicates.
                            tmp.map(new AssignTermId(iv));

                        }
                        
                    }
                    
					if (log.isDebugEnabled())
						log.debug("termId=" + iv + ", term=" + a[i].obj);

                }

            }

        }

        public Void getResult() {

            return null;

        }

    }

    /**
     * Assigns the term identifier to duplicate {@link BigdataValue} for a
     * single write operation when an {@link IDuplicateRemover} was applied.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id: Term2IdWriteTask.java 3408 2010-08-04 18:53:35Z thompsonbry $
     * 
     * @todo this should be more transparent. One way to do that is to get rid
     *       of {@link KVOList#map(com.bigdata.service.ndx.pipeline.KVOList.Op)}
     *       and {@link KVOList.Op} and provide a TERM2ID index write specific
     *       interface extending KVO. When the term identifier is assigned, we
     *       then invoke the method on that interface to set the term identifier
     *       on the original and any duplicates. The method would have to know
     *       about {@link KVOList} however, and that means that we would really
     *       need to different implementations depending on whether {@link KVOC}
     *       was being extended or not. This is possibly even more messy.
     */
    static public class AssignTermId implements KVOList.Op<BigdataValue> {
        
        private final IV iv;

        public AssignTermId(final IV iv) {

            this.iv = iv;
            
        }

        public void apply(final KVO<BigdataValue> t) {

            t.obj.setIV(iv);
            
//            System.err.println("Assigned term identifier to duplicate: "+tid+" : "+t.obj);
            
        }

    }
    
}
