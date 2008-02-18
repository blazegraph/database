package com.bigdata.rawstore;

import java.io.InputStream;

/**
     * An object that may be used to read or write a block from a store. In
     * general, an instance of this interface either supports read or write but
     * not both.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IBlock {

        /**
         * The address of the block on the store.
         */
        public long getAddress();

        /**
         * The length of the block.
         */
        public int length();

        /**
         * The source from which the block's data may be read.
         * <p>
         * Note: It is important to close() this input stream.
         * 
         * @throws UnsupportedOperationException
         *             if read is not supported.
         */
        public InputStream inputStream(); 
        
//        /**
//         * The sink on which the block's data may be written.
//         * <p>
//         * Note: It is important to flush() and close() this output stream.
//         * 
//         * @throws UnsupportedOperationException
//         *             if write is not supported.
//         */
//        public OutputStream outputStream(); 
        
    }