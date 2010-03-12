package com.bigdata.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;
import java.util.SortedMap;

import junit.framework.TestCase;

/**
 * Investigates the benefits (if any) of managed concurrent reads with the aim of developing
 * metrics on which to base a ConcurrentReadManager for use by the Disk based strategies.
 * 
 * @author mgc
 *
 */
public class TestManagedReads extends TestCase {

    public TestManagedReads() {
    }

    /**
     * @param name
     */
    public TestManagedReads(String name) {
        super(name);
    }
    
    RandomAccessFile m_raf = null;
    byte[] m_data = null;
    static class Record implements Comparable {
    	public long pos;
    	public int size;
    	public Record(long ppos, int psize) {
    		pos = ppos;
    		size = psize;
    	}
		@Override
		public int compareTo(Object o) {
			return pos < ((Record) o).pos ? -1 : 1;
		}
    }
    ArrayList<Record> m_records = new ArrayList<Record>(20000); // capacity for 20000 record entries
	Random r = new Random();
    
    /**
     * Generates file of filesize composed of a number of records between min and max size.
     * Write random data to file.
     * 
     * Assume average size of (minrec + maxec)/2, this should give rough number of records.
     * 
     * We want to have approximately 20,000 records to read from so adjust proportion created to
     * that stored by using a mod.  For example, if we want a 10Gb file of average 512 byte records
     * that is 20Million records which would result in a modulus of 1000 to be used to save sample record
     * entries. A 1Gb file (the smallest worth testing) would use result in a modulus of 100.
     * 
     * @param filesize
     * @param minrec
     * @param maxrec
     * @throws IOException 
     */
    void generateContent(File fd, long filesize, int minrec, int maxrec) throws IOException {
    	long start = System.currentTimeMillis();
    	
    	long totRecs = filesize /((maxrec + minrec)/2);
    	long mod = 1;
    	if (totRecs > 20000) {
    		mod = totRecs / 20000;
    	}
    	
    	m_data = new byte[maxrec];
    	r.nextBytes(m_data);   	
    	
		m_raf = new RandomAccessFile(fd, "rw");
		m_raf.setLength(filesize);
		
		m_raf.seek(0);
		long pos = 0;
		int span = maxrec - minrec;
		int recnum = 0;
		while (pos < (filesize - maxrec)) {
			int sze = minrec + r.nextInt(span);
			m_raf.write(m_data, 0, sze);
			
			if ((++recnum % mod) == 0) {
				m_records.add(new Record(pos, sze));
			}
			
			pos += sze;
		}
		
		System.out.println("File content generated in " + (System.currentTimeMillis() - start) + "ms");
		
    }
    
    static final long GIGABYTE = 1024L * 1024L * 1024L;
    static final int testminrec = 64;
    static final int testmaxrec = 960;
	File fd = new File("/systap/benchmarks/read-file-test2");
	boolean generated = false;
    public void generate() {
    	if (generated) {
    		return;
    	}
    	
    	fd.getParentFile().mkdirs();
    	try {
	    	if (!fd.exists()) {
				fd.createNewFile();
	    	}
	    	
	    	fd.deleteOnExit();
	    	
	 		generateContent(fd, 10 * GIGABYTE, testminrec, testmaxrec);
	 		
	 		generated = true;
    	} catch (IOException ioe) {
    		ioe.printStackTrace();
    	}
    }

    public void testAll() {
    	managedReads();
    	unmanagedReads();
    	managedReads();
    }
    public void unmanagedReads() {
    	generate();
    	
    	byte[] rdbuf = new byte[testmaxrec];
    	
    	long start = System.currentTimeMillis();
    	for (int i = 0; i < 5000; i++) {
    		Record rec = m_records.get(r.nextInt(m_records.size() - 1));
    		try {
				m_raf.seek(rec.pos);
	    		m_raf.read(rdbuf, 0, rec.size);
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
		System.out.println("testUnmanagedReads 5000 completed in " + (System.currentTimeMillis() - start) + "ms");
    }

    public void managedReads() {
    	generate();
    	
    	byte[] rdbuf = new byte[testmaxrec];
    	
    	long start = System.currentTimeMillis();
    	for (int i = 0; i < 1000; i++) {
    		ArrayList<Record> recs = new ArrayList<Record>(5);
    		
    		for (int j = 0; j < 5; j++) {
	    		recs.add(m_records.get(r.nextInt(m_records.size() - 1)));
    		}
    		long ss = System.currentTimeMillis();
    		Collections.sort(recs);
    		
    		for (int j = 0; j < 5; j++) {
	    		Record rec = recs.get(j);
	    		try {
					m_raf.seek(rec.pos);
		    		m_raf.read(rdbuf, 0, rec.size);
				} catch (IOException e) {
					e.printStackTrace();
				}
    		}
    	}
    	
		System.out.println("testManagedReads 5000 completed in " + (System.currentTimeMillis() - start) + "ms");
    }
}
