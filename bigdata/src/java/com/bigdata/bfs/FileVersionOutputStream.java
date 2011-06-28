package com.bigdata.bfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Class buffers up to a block of data at a time and flushes blocks using an
 * atomic append operation on the identifier file version.
 * 
 * @todo this would benefit from asynchronous write-behind of the last block
 *       so that caller's do not wait for the RPC that writes the block onto
 *       the data index. use a blocking queue of buffers to be written so
 *       that the caller can not get far ahead of the database. a queue
 *       capacity of 1 or 2 should be sufficient.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FileVersionOutputStream extends OutputStream {

    protected final BigdataFileSystem repo;
    protected final String id;
    protected final int version;
    
    /**
     * The file identifier.
     */
    public String getId() {
        
        return id;
        
    }

    /**
     * The file version identifer.
     */
    public int getVersion() {

        return version;
        
    }

    /**
     * The buffer in which the current block is being accumulated.
     */
    private final byte[] buffer;

    /**
     * The index of the next byte in {@link #buffer} on which a byte would be
     * written.
     */
    private int len = 0;

    /**
     * #of bytes written onto this output stream.
     */
    private long nwritten;
    
    /**
     * #of bytes written onto this output stream.
     * 
     * @todo handle overflow of long - leave counter at {@link Long#MAX_VALUE}.
     */
    public long getByteCount() {
        
        return nwritten;
        
    }

    /**
     * #of blocks written onto the file version.
     */
    private long nblocks;
    
    /**
     * #of blocks written onto the file version.
     */
    public long getBlockCount() {
       
        return nblocks;
        
    }
    
    /**
     * Create an output stream that will atomically append blocks of data to
     * the specified file version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     */
    public FileVersionOutputStream(BigdataFileSystem repo, String id, int version) {
        
        if (repo == null)
            throw new IllegalArgumentException();
        if (id == null)
            throw new IllegalArgumentException();
        
        this.repo = repo;
        
        this.id = id;
        
        this.version = version;

        this.buffer = new byte[repo.getBlockSize()];
        
    }

    /**
     * Buffers the byte. If the buffer would overflow then it is flushed.
     * 
     * @throws IOException
     */
    public void write(int b) throws IOException {

        if (len == buffer.length) {

            // buffer would overflow.
            
            flush();
            
        }
        
        buffer[len++] = (byte) (b & 0xff);
        
        nwritten++;
        
    }

    /**
     * If there is data data accumulated in the buffer then it is written on
     * the file version using an atomic append (empty buffers are NOT
     * flushed).
     * 
     * @throws IOException
     */
    public void flush() throws IOException {
        
        if (len > 0) {

            BigdataFileSystem.log.info("Flushing buffer: id="+id+", version="+version+", len="+len);
            
            repo.appendBlock(id, version, buffer, 0, len);

            len = 0;
            
            nblocks++;
            
        }
        
    }
    
    /**
     * Flushes the buffer.
     * 
     * @throws IOException
     */
    public void close() throws IOException {
       
        flush();
        
    }

    /**
     * Consumes the input stream, writing blocks onto the file version. The
     * output stream is NOT flushed.
     * 
     * @param is
     *            The input stream (closed iff it is fully consumed).
     * 
     * @return The #of bytes copied from the input stream.
     * 
     * @throws IOException
     */
    public long copyStream(InputStream is) throws IOException {

        long ncopied = 0L;

        while (true) {

            if (this.len == buffer.length) {

                // flush if the buffer would overflow.
                
                flush();
                
            }
            
            // next byte to write in the buffer.
            final int off = this.len;

            // #of bytes remaining in the buffer.
            final int remainder = this.buffer.length - off;

            // read into the buffer.
            final int nread = is.read(buffer, off, remainder);

            if (nread == -1) {

                // the input stream is exhausted.
                
                BigdataFileSystem.log.info("Copied " + ncopied + " bytes: id=" + id
                        + ", version=" + version);

                try {

                    is.close();
                    
                } catch (IOException ex) {
                    
                    BigdataFileSystem.log.warn("Problem closing input stream: id=" + id
                            + ", version=" + version, ex);
                    
                }

                return ncopied;

            }

            // update the index of the next byte to write in the buffer.
            this.len = off + nread;

            // update #of bytes copied.
            ncopied += nread;

            // update #of bytes written on this output stream.
            nwritten += nread;

        }

    }
    
}
