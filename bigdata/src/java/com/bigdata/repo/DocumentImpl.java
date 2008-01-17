package com.bigdata.repo;

import java.io.ByteArrayInputStream;
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
     * @param header to copy
     */
    
    public DocumentImpl( DocumentHeader header )
    {
     
        super( header );        
    }
    
    /**
     * Copy constructor for document information.
     * 
     * @param document to copy
     */
    
    public DocumentImpl( Document document )
    {
     
        this( (DocumentHeader) document );
        
        setContent( document.getContent() );
        
    }
    
    public void setContent( byte[] content )
    {
        
        this.content = content;
        
    }
    
    public byte[] getContent()
    {
        
        return content;
        
    }
    
    public String getContentAsString() throws IOException {

        return readUTF( getReader() );
        
    }

    public InputStream getInputStream() {
        
        return new ByteArrayInputStream(content);
        
    }
   
    public Reader getReader() throws UnsupportedEncodingException {

        return new InputStreamReader(new ByteArrayInputStream(content),
                getContentEncoding());

    }

    /**
     * Suck the character data from the reader into a string.
     * 
     * @param reader
     * @return
     * @throws IOException
     */
    private static String readUTF( Reader reader ) throws IOException
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
