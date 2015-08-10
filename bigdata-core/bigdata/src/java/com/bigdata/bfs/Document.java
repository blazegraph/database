package com.bigdata.bfs;

import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

/**
 * Document model interface for operations on the {@link IContentRepository}. 
 * 
 * @author mike@systap.com
 * @version $Id$
 */
public interface Document extends DocumentHeader
{
    
    /**
     * An input stream on the byte[] stored in the {@link IContentRepository}.
     */
    InputStream getInputStream();

    /**
     * A reader on the byte[] stored in the {@link IContentRepository} where
     * bytes are decoded to characters using the encoding identified by
     * {@link DocumentHeader#getContentEncoding()}.
     * 
     * @throws IllegalStateException
     *             if the {@link DocumentHeader#getContentEncoding()} is not
     *             set.
     */
    Reader getReader() throws UnsupportedEncodingException;
        
}
