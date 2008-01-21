package com.bigdata.repo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;

public class DocumentImpl extends DocumentHeaderImpl implements Document 
{
    
    private byte[] content;
    
    /**
     * Create a new empty document.
     */
    public DocumentImpl()
    {
        
    }
    
    /**
     * Copy constructor for header information.
     * 
     * @param header
     *            to copy
     */
    public DocumentImpl(DocumentHeader header) {

        super(header);
    }

    public InputStream getInputStream() {
        
        return new ByteArrayInputStream(content);
        
    }
   
    public Reader getReader() throws UnsupportedEncodingException {

        return new InputStreamReader(new ByteArrayInputStream(content),
                getContentEncoding());

    }

    /**
     * Set the content by copying the byte[].
     * <p>
     * Note: Do NOT use this when the content is character data since the
     * encoding attribute will NOT be set.
     * 
     * @param b
     *            The content.
     * @param off
     *            The offset of the 1st byte to be copied.
     * @param len
     *            The #of bytes to be copied.
     */
    public void copyStream(byte[] b,int off, int len) {

        super.setContentEncoding(null);
        
        content = new byte[len];
        
        System.arraycopy(b, off, content, 0, len);
        
    }

    /**
     * Set the content by copying the byte[].
     * <p>
     * Note: Do NOT use this when the content is character data since the
     * encoding attribute will NOT be set.
     * 
     * @param b The content.
     */
    public void copyStream(byte[] b) {

        copyStream(b,0,b.length);
        
    }

    /**
     * Set the content by copying the given stream.
     * <p>
     * Note: Do NOT use this when the content is character data since the
     * encoding attribute will NOT be set.
     * 
     * @param is
     * 
     * @throws IOException
     */
    public void copyStream(InputStream is) throws IOException {

        super.setContentEncoding(null);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        while(true) {
            
            int b = is.read();
            
            if(b==-1) {
                
                content = baos.toByteArray();
                
                is.close();
                
                return;
                
            }
            
        }
        
    }

    /**
     * Set the content by converting characters to into bytes using the
     * specified encoding.
     * 
     * @param encoding
     *            The encoding that will be used to convert characters to bytes.
     * @param s
     *            The string.
     *            
     * @throws UnsupportedEncodingException 
     */
    public void copyString(String encoding, String s ) throws UnsupportedEncodingException {

        super.setContentEncoding(encoding);

        content = s.getBytes(encoding);
        
    }
    
    /**
     * Set the content by copying characters from the given reader and
     * converting them into bytes using the specified encoding.
     * 
     * @param encoding
     *            The encoding that will be used to convert characters to bytes.
     * @param r
     *            The reader.
     *            
     * @throws IOException
     */
    public void copyReader(String encoding, Reader r) throws IOException {

        super.setContentEncoding(encoding);

        String s = readString( r );
        
        content = s.getBytes(encoding);
        
    }
    
    /**
     * Suck the character data from the reader into a string.
     * 
     * @param reader
     * 
     * @return The string
     * 
     * @throws IOException
     */
    private static String readString( Reader reader ) throws IOException
    {
        
        StringWriter writer = new StringWriter();
        
        try {
            
            int i;
            
            while ( ( i = reader.read() ) != -1 ) {
                
                writer.write( i );
                
            }
            
        } finally {
            
            try {
                
                reader.close();
                
                writer.close();
                
            } catch ( Exception ex ) {
                
                log.warn( "Could not close reader/writer: "+ex, ex );
                
            }
            
        }
        
        return writer.toString();
        
    }

}
