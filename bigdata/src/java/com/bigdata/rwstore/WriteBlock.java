/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package com.bigdata.rwstore;

import java.io.*;

public class WriteBlock {
	protected static java.util.logging.Logger cat = java.util.logging.Logger.getLogger(WriteBlock.class.getName());

	
  int m_total = 0;
  RandomAccessFile m_file = null;
  
  WriteEntry m_head = null;
  WriteEntry m_tail = null; // search back from end if better bet in general
  
  public WriteBlock(RandomAccessFile file) {
  	m_file = file;
  }
  
  boolean m_directWrite = true;
  
  public void addWrite(long diskAddr, byte[] buf, int size) {
  	if (size == 0) { // nowt to do
  		return;
  	}
  	
    if (diskAddr < 0) {
      throw new Error("addWrite called to negative address! - " + diskAddr);
    }
    
    if (m_directWrite && diskAddr > 100 * 1000 * 1000) {
    	// only start buffering writes once filesize > 100Mb ?
    	m_directWrite = false;
    }
    
    if (m_directWrite) {
	    try {
	      m_file.seek(diskAddr);
	      m_file.write(buf, 0, size);
	    } catch (Exception e) {
	      throw new Error("WriteBlock.doWrite : " + m_file + "-" + diskAddr + "-" + buf + "-" + e);
	    }
    } else {   
	    WriteEntry block = new WriteEntry(diskAddr, buf, size);
	    m_total += size;
	
	    placeEntry(block);
	
	    if (m_total > 50 * 1024) { // should be configured?
	      flush();
	    }
    }
  }
  
  /**********************************************************
   * find first block whose address is greater than this, and insert before
   *
   * if block has same address, then update with this buffer/size
   **/
  void placeEntry(WriteEntry entry) {
  	if (m_head == null) {
  		m_head = entry;
  		m_tail = entry;
  	} else {
  		WriteEntry tail = m_tail;
  		
  		while (tail != null) {
  			if (tail.m_diskAddr < entry.m_diskAddr) {
  				entry.m_next =  tail.m_next;
  				entry.m_prev = tail;
  				tail.m_next = entry;
  				
  				break;
  			}
  			
  			tail = tail.m_prev;
  		}
  		
  		if (tail == null) {
  			entry.m_next = m_head;
  			m_head = entry;
  		}
  		
			if (entry.m_next != null) {
				entry.m_next.m_prev = entry;
			} else {
				m_tail = entry;
			}
  	}
  }
  
  public void flush() {
  	WriteEntry entry = m_head;
    long addr = 0;
    while (entry != null) {
      entry.doWrite(m_file);

      if (addr == entry.m_diskAddr) {
        throw new RuntimeException("WriteBlock.flush : *** DUPLICATE WRITE *** " + addr);
      }
      
      addr = entry.m_diskAddr;
      
      entry = entry.m_next;
    }

    clear();
  }

  public void clear() {
    m_head = null;
    m_tail = null;
    m_total = 0;
  }

  public boolean removeWriteToAddr(long addr) {
  	WriteEntry entry = m_head;
  	
  	while (entry != null) {
  		if (entry.m_diskAddr == addr) {
  			if (entry.m_prev == null) {
  				m_head = entry.m_next;
  			} else {
  				entry.m_prev.m_next = entry.m_next;
  			}
  			
  			if (entry.m_next == null) {
  				m_tail = entry.m_prev;
  			} else {
  				entry.m_next.m_prev = entry.m_prev;
  			}
  			
  			return true;
  		}
  		
  		entry = entry.m_next;
  	}
  	
  	return false;
  }



  static class WriteEntry {

		WriteEntry m_prev = null;
		WriteEntry m_next = null;
		
	  long m_diskAddr;
	  byte[] m_buf = null;
	  
	  int m_writeCount = 0;
	
	  WriteEntry(long diskAddr, byte[] buf, int size) {
	    m_diskAddr = diskAddr;
	    
	    if (size > 0) {
	    	m_buf = new byte[size];
	    	System.arraycopy(buf, 0, m_buf, 0, size);
	    }
	  }
	
	  void doWrite(RandomAccessFile file) {
	  	if (m_buf == null) {
	  		cat.warning("WriteEntry:doWrite - with null buffer");
	  		
	  		return;
	  	}
	  	
	  	if (m_writeCount++ > 0) {
	  		throw new Error("Do NOT understand this");
	  	}
	  	
	    try {
	      file.seek(m_diskAddr);
	      file.write(m_buf);
	    } catch (Exception e) {
	      throw new RuntimeException("WriteBlock.doWrite : " + file + "-" + m_diskAddr + "-" + m_buf + "-" + e);
	    }
	  }
	  
	  public boolean equals(Object obj) {
	    return m_diskAddr == ((WriteEntry) obj).m_diskAddr;
	  }
	
	  public int compareTo(Object obj) {
	  	long diskAddr = ((WriteEntry) obj).m_diskAddr;
	    if (m_diskAddr < diskAddr) {
	    	return -1;
	    } else if (m_diskAddr > diskAddr) {
	    	return 1;
	    } else {
	    	return 0;
	    }
	  }
	}
}
