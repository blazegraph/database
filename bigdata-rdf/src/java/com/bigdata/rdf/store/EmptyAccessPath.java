package com.bigdata.rdf.store;

import java.util.Arrays;
import java.util.Iterator;

import org.openrdf.model.Value;

import com.bigdata.btree.ITupleIterator;
import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.util.KeyOrder;

/**
     * An access path that is known to be empty. There is a single instance of
     * this class. Various methods will return that object if you request an
     * access path using the Sesame {@link Value} objects and one of the
     * {@link Value}s is not known to the database. In such cases we know that
     * nothing can be read from the database for the given triple pattern.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class EmptyAccessPath implements IAccessPath {
        
        /**
         * @throws UnsupportedOperationException
         */
        public long[] getTriplePattern() {
            
            throw new UnsupportedOperationException();
            
        }

//        public IIndex getStatementIndex() {
//            
//            return AbstractTripleStore.this.getStatementIndex(getKeyOrder());
//            
//        }

        public KeyOrder getKeyOrder() {
            
            // arbitrary.
            return KeyOrder.SPO;
            
        }

        /**
         * Always returns <code>true</code>.
         */
        public boolean isEmpty() {
            
            return true;
            
        }

        /**
         * Always returns ZERO(0).
         */
        public long rangeCount() {
            
            return 0;
            
        }

        /**
         * @throws UnsupportedOperationException
         */
        public ITupleIterator rangeQuery() {

            throw new UnsupportedOperationException();
            
        }

        public ISPOIterator iterator() {

            return new SPOArrayIterator(new SPO[]{},0);
            
        }

        public ISPOIterator iterator(ISPOFilter filter) {

            return new SPOArrayIterator(new SPO[]{},0);
            
        }

        public ISPOIterator iterator(int limit, int capacity) {

            return new SPOArrayIterator(new SPO[]{},0);
            
        }

        public ISPOIterator iterator(int limit, int capacity, ISPOFilter filter) {

            return new SPOArrayIterator(new SPO[]{},0);
            
        }

        public Iterator<Long> distinctTermScan() {
            
            return Arrays.asList(new Long[]{}).iterator();
            
        }

        public int removeAll() {

            return 0;
            
        }
        
        public int removeAll(ISPOFilter filter) {

            return 0;
            
        }
        
    }