package com.bigdata.repo;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

/**
 * Document model interface for operations on the {@link ContentRepository}. 
 * 
 * @author mike@systap.com
 * @version $Id$
 */
public interface Document extends DocumentHeader
{
    
    /**
     * An input stream on the byte[] stored in the {@link ContentRepository}.
     */
    InputStream getInputStream();

    /**
     * A reader on the byte[] stored in the {@link ContentRepository} where
     * bytes are decoded to characters using the encoding identified by
     * {@link DocumentHeader#getContentEncoding()}.
     */
    Reader getReader() throws UnsupportedEncodingException;
    
    /**
     * The uninterpreted byte[] stored in the {@link ContentRepository} for this
     * document. In order to convert this to characters you MUST decode it using
     * the character set encoding returned by
     * {@link DocumentHeader#getContentEncoding()}.
     */
    byte[] getContent();

    /**
     * Decodes the byte[] stored in the {@link ContentRepository}.
     * 
     * @return The decoded character data.
     * 
     * @throws IOException
     */
    String getContentAsString() throws IOException;
    
}
