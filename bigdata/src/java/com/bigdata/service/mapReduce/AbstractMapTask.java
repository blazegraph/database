package com.bigdata.service.mapReduce;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.UnicodeKeyBuilder;
import com.bigdata.io.DataOutputBuffer;

/**
 * Abstract base class for {@link IMapTask}s.
 * <p>
 * Note: The presumption is that there is a distinct instance of the map task
 * for each task executed and that each task is executed within a
 * single-threaded environment.
 * <p>
 * Note: Any declared fields are materialized on the master and the service, so
 * make the field <strong>transient</strong> unless you need to send it to the
 * service and do not initialize anything large on the master (unless it is
 * transient). Lazy initialization is nice since we only do it on the service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractMapTask implements IMapTask {

    protected final UUID uuid;
    protected final Object source;
    protected final int nreduce;
    protected final IHashFunction hashFunction;

    // service side data structures (all transient).
    private transient List tuples;
    private transient int[] histogram;
    private transient KeyBuilder keyBuilder;
    private transient DataOutputBuffer valBuilder;
    static private transient final Tuple[] EMPTY = new Tuple[]{};

    /**
     * The {@link KeyBuilder} MUST be used by the {@link IMapTask} so that
     * the generated keys will have a total ordering determined by their
     * interpretation as an <em>unsigned</em> byte[].
     * 
     * @todo does not always have to support unicode
     * @todo could configure the buffer size for some tasks.
     * @todo could choose the collation sequence for unicode.
     */
    protected KeyBuilder getKeyBuilder() {
        if(keyBuilder==null) {
            keyBuilder = new UnicodeKeyBuilder();
        }
        return keyBuilder;
    }
    
    /**
     * The values may be formatted using this utility class. The basic
     * pattern is:
     * 
     * <pre>
     * valBuilder.reset().append(foo).toByteArray();
     * </pre>
     */
    protected DataOutputBuffer getDataOutputBuffer() {
        if(valBuilder==null) {
            valBuilder = new DataOutputBuffer();
        }
        return valBuilder;
    }

    /**
     * @param uuid
     *            The UUID of the map task. This MUST be the same UUID each time
     *            if a map task is re-executed for a given input. The UUID
     *            (together with the tuple counter) is used to generate a key
     *            that makes the map operation "retry safe". That is, the
     *            operation may be executed one or more times and the result
     *            will be the same. This guarentee arises because the values for
     *            identical keys are overwritten during the reduce operation.
     * @param source
     *            The source from which the map task will read its data. This is
     *            commonly a {@link File} in a networked file system but other
     *            kinds of sources may be supported.
     * @param nreduce
     *            The #of reduce tasks that are being feed by this map task.
     * @param hashFunction
     *            The hash function used to hash partition the tuples generated
     *            by the map task into the input sink for each of the reduce
     *            tasks.
     */
    protected AbstractMapTask(UUID uuid, Object source, Integer nreduce, IHashFunction hashFunction) {
        
        if(uuid==null) throw new IllegalArgumentException();

        if(source==null) throw new IllegalArgumentException();

        if(nreduce==null) throw new IllegalArgumentException();

        if(hashFunction==null) throw new IllegalArgumentException();

        this.uuid = uuid;
        
        this.source = source;
        
        this.nreduce = nreduce;
        
        this.hashFunction = hashFunction;

    }

    public UUID getUUID() {
        
        return uuid;
        
    }

    /**
     * The source from which the map task will read its data. This is commonly a
     * {@link File} in a networked file system but other kinds of sources may be
     * supported.
     */
    public Object getSource() {
       
        return source;
        
    }
    
    /**
     * Return the tuples.
     * 
     * @return
     */
    public Tuple[] getTuples() {
    
        int ntuples = getTupleCount();
        
        if(ntuples==0) return EMPTY;
        
        return (Tuple[]) tuples.toArray(new Tuple[ntuples]);
        
    }
    
    /**
     * The #of tuples written by the task.
     */
    public int getTupleCount() {

        return tuples==null?0:tuples.size();

    }

    /**
     * Hash partitions the tuple based on the key already in {@link #keyBuilder}
     * into one of {@link #nreduce} output buckets. Forms a unique key using the
     * data already in {@link #keyBuilder} and appending the task UUID and the
     * int32 tuple counter. Finally, invokes {@link #output(byte[], byte[])} to
     * output the key-value pair. The resulting key preserves the key order,
     * groups all keys with the same value for the same map task, and finally
     * distinguishes individual key-value pairs using the tuple counter.
     * 
     * @param val
     *            The value for the tuple.
     * 
     * @see #output(int,byte[], byte[])
     */
    public void output(byte[] val) {
    
        if(keyBuilder==null) {
            
            throw new IllegalStateException("No key?");
            
        }
        
        // @todo try LinkedList vs ArrayList.
        if(tuples==null) {
          
            /*
             * Note: Lazy initialization of data structures on the service only.
             */
            
            tuples = new ArrayList<Tuple>(1000);
           
            histogram = new int[nreduce];
            
        }
        
        // The hash is computed using ONLY the application key.
        byte[] key = keyBuilder.getKey();

        // Note: We have to fix up the sign when the hash code is negative!
        final int hashCode = hashFunction.hashCode(key);

        // The reduce partition into which this tuple was hash partitioned.
        final int partition = (hashCode<0?-hashCode:hashCode) % nreduce;
        
        // Now build the complete key.
        //
        // @todo We could use an int32 or int64 identifier from the master for
        // this map task in place of the UUID here and save some bytes in the
        // key.
        key = keyBuilder.append(uuid).append(tuples.size()).getKey();

        // Buffer the key for this output partition.
        output(partition,key,val);
        
    }
    
    /**
     * Output a key-value pair (tuple) to the appropriate reduce task. For
     * example, the key could be a token and the value could be the #of times
     * that the token was identified in the input. All tuples will be buffered
     * until the map task completes successfully and the written onto the
     * appropriate reduce partitions.
     * 
     * @param partition
     *            The output partition in [0:{@link #nreduce}}.
     * @param key
     *            The complete key. The key MUST be encoded such that the keys
     *            may be placed into a total order by interpreting them as an
     *            <em>unsigned</em> byte[]. See {@link KeyBuilder}.
     * @param val
     *            The value. The value encoding is essentially arbitrary but the
     *            {@link DataOutputBuffer} may be helpful here.
     */
    protected void output(int partition, byte[] key, byte[] val) {

        if(partition<0||partition>nreduce) {
            
            throw new IllegalArgumentException("Partition must be in [0:"+nreduce+"], not "+partition);
            
        }
        
        if (key == null)
            throw new IllegalArgumentException();

        if (val == null)
            throw new IllegalArgumentException();

        histogram[partition]++;
        
        // @todo could insert into the appropriate partition immediately rather
        // than storing that data on the Tuple.
        tuples.add(new Tuple(partition,key,val));

    }

    /**
     * Return the histogram of the #of tuples in each output partition.
     */
    public int[] getHistogram() {

        return histogram;
        
    }
    
}