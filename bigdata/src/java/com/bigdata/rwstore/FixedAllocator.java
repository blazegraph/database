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

import java.util.*;
import java.io.*;

/**
 * FixedAllocator
 *
 * Maintains List of AllocBlock(s)
 */
public class FixedAllocator implements Allocator {
	private WriteBlock m_writes = null;
  private int m_freeBits;
  private int m_freeTransients;

  private long m_diskAddr;
  private int m_index;
  
  protected boolean m_preserveSession = false;

  public void setIndex(int index) {
  	m_index = index;
  }
  
  public void preserveSessionData() {
  	m_preserveSession = true;
  }

  public long getStartAddr() {
  	return RWStore.convertAddr(m_startAddr);
  }
  
  public int compareTo(Object o) {
  	Allocator other = (Allocator) o;
  	if (other.getStartAddr() == 0) {
  		return -1;
  	} else {
  		long val = getStartAddr() - other.getStartAddr();
  		
  		if (val == 0) {
  			throw new Error("Die ... bugger die .... Two allocators at same address");
  		}
  		
  		return (int) val;
  	}
  }	

  public long getDiskAddr() {
    return m_diskAddr;
  }
  public void setDiskAddr(long addr) {
    m_diskAddr = addr;
  }
  
  public long getPhysicalAddress(int offset) {
  	offset -= 3;
  	
  	int allocBlockRange = 32 * m_bitSize;
  	
  	AllocBlock block = (AllocBlock) m_allocBlocks.get(offset/allocBlockRange);
		
		return RWStore.convertAddr(block.m_addr) + (m_size * (offset % allocBlockRange));
  }
  	
  public int getPhysicalSize(int offset) {
  	return m_size;
  }

  public int getBlockSize() {
  	return m_size;
  }

  private ArrayList m_freeList;
  public void setFreeList(ArrayList list) {
    m_freeList = list;
    
    if (hasFree()) {
    	m_freeList.add(this);
    }
  }

  public byte[] write() {
    try {
      byte[] buf = new byte[1024];
      DataOutputStream str = new DataOutputStream(new FixedOutputStream(buf));

      str.writeInt(m_size);

      Iterator iter = m_allocBlocks.iterator();
      while (iter.hasNext()) {
        AllocBlock block = (AllocBlock) iter.next();

        str.writeInt(block.m_addr);
        for (int i = 0; i < m_bitSize; i++) {
          str.writeInt(block.m_bits[i]);
        }

				if (!m_preserveSession) {
        	block.m_transients = (int[]) block.m_bits.clone();
        }
        
        block.m_commit = (int[]) block.m_bits.clone();
      }

			if (!m_preserveSession) {
      	m_freeBits += m_freeTransients;

	      // Handle re-addition to free list once transient frees are added back
	      if ((m_freeTransients == m_freeBits) && (m_freeTransients != 0)) {
	        m_freeList.add(this);
	      }

      	m_freeTransients = 0;
      }

      return buf;
    }
    catch (IOException e) {
      throw new StorageTerminalError("Error on write", e);
    }
  }
  // read does not read in m_size since this is read to determine the class of allocator
  public void read(DataInputStream str) {
    try {
      m_freeBits = 0;

      Iterator iter = m_allocBlocks.iterator();
      int blockSize = m_bitSize * 32 * m_size;
      while (iter.hasNext()) {
        AllocBlock block = (AllocBlock) iter.next();

        block.m_addr = str.readInt();
        for (int i = 0; i < m_bitSize; i++) {
          block.m_bits[i] = str.readInt();

					/**
					 * Need to calc how many free blocks are available,
					 *	minor optimisation by checking against either empty or full
					 *	to avoid scanning every bit unnecessarily
					 **/
          if (block.m_bits[i] == 0) { // empty
            m_freeBits += 32;
          } else if (block.m_bits[i] != 0xFFFFFFFF) { // not full
	          int anInt = block.m_bits[i];
            for (int bit = 0; bit < 32; bit++) {
	            if ((anInt & (1 << bit)) == 0) {
                m_freeBits++;
              }
            }
          }
        }

        block.m_transients = (int[]) block.m_bits.clone();
        block.m_commit = (int[]) block.m_bits.clone();

        if (m_startAddr == 0) {
          m_startAddr = block.m_addr;
        }
        //int endAddr = block.m_addr + blockSize;
        if (block.m_addr > 0) {
          m_endAddr = block.m_addr + blockSize;
        }
      }
    }
    catch (IOException e) {
      throw new StorageTerminalError("Error on read", e);
    }

  }

  int m_size;

  int m_startAddr = 0;
  int m_endAddr = 0;

  int m_bitSize;

  ArrayList m_allocBlocks;

  FixedAllocator(int size, boolean preserveSessionData, WriteBlock writes) {
    m_diskAddr = 0;

    m_size = size;

    m_bitSize = (int) (512L / m_size);
    if (m_bitSize == 0) {
      m_bitSize = 1;
    }
    
    m_writes = writes;

    // number of blocks in this allocator
    int numBlocks = 255/(m_bitSize + 1);

    m_allocBlocks = new ArrayList(numBlocks);
    for (int i = 0; i < numBlocks; i++) {
      m_allocBlocks.add(new AllocBlock(0, m_bitSize, m_writes));
    }

    m_freeTransients = 0;
    m_freeBits = 32 * m_bitSize * numBlocks;
    
    m_preserveSession = preserveSessionData;
  }

  public String getStats() {
    String stats = "Block size : " + m_size + " start : " + getStartAddr() + " free : " + m_freeBits +"\r\n";

    Iterator iter = m_allocBlocks.iterator();
    while (iter.hasNext()) {
      AllocBlock block = (AllocBlock) iter.next();
      if (block.m_addr == 0) {
        break;
      }
      stats = stats + block.getStats() + "\r\n";
      RWStore.s_allocation += block.getAllocBits() * m_size;
    }

    return stats;
  }

  public boolean verify(int addr) {
    if (addr >= m_startAddr && addr < m_endAddr) {

      Iterator iter = m_allocBlocks.iterator();
      while (iter.hasNext()) {
        AllocBlock block = (AllocBlock) iter.next();
        if (block.verify(addr, m_size)) {
          return true;
        }
      }
    }

    return false;
  }

  public boolean addressInRange(int addr) {
   if (addr >= m_startAddr && addr < m_endAddr) {

      Iterator iter = m_allocBlocks.iterator();
      while (iter.hasNext()) {
        AllocBlock block = (AllocBlock) iter.next();
        if (block.addressInRange(addr, m_size)) {
          return true;
        }
      }
    }

    return false;
	}    	

  public boolean free(int addr) {
  	if (addr < 0) {
  		int offset = ((-addr) & 0xFFFF) - 3;
  		
  		int nbits = 32 * m_bitSize;
  		
  		if (((AllocBlock) m_allocBlocks.get(offset / nbits)).freeBit(offset % nbits, getPhysicalAddress(offset + 3))) {
  			if (m_freeBits++ == 0) {
  				m_freeList.add(this);
  			}
  		} else {  		
      	m_freeTransients++;
      }
      
  		return true;
  	} else if (addr >= m_startAddr && addr < m_endAddr) {

      Iterator iter = m_allocBlocks.iterator();
      while (iter.hasNext()) {
        AllocBlock block = (AllocBlock) iter.next();
        if (block.free(addr, m_size)) {
          m_freeTransients++;

          return true;
        }
      }
    }

    return false;
  }

  public int alloc(RWStore store, int size) {
    int addr = -1;

    Iterator iter = m_allocBlocks.iterator();
    int count = -1;
    while (addr == -1 && iter.hasNext()) {
      count++;

      AllocBlock block = (AllocBlock) iter.next();
      if (block.m_addr == 0) {
        int blockSize = 32 * m_bitSize * m_size;
        blockSize >>= 13;
        block.m_addr = store.allocBlock(blockSize);
				
        if (m_startAddr == 0) {
          m_startAddr = block.m_addr;
        }
        m_endAddr = block.m_addr - blockSize;
      }
      addr = block.alloc(m_size);
    }

    if (addr != -1) {
    	addr += 3;
    	
      if (--m_freeBits == 0) {
        m_freeList.remove(this);
      }
      
      addr += (count * 32 * m_bitSize);
      
      int value = -((m_index << 16) + addr);
      
      return value;
    } else {
    	return 0;
    }
  }
  
  public boolean hasFree() {
    return m_freeBits > 0;
  }
  
  public void addAddresses(ArrayList addrs) {
  	Iterator blocks = m_allocBlocks.iterator();
  	
  	int baseAddr = -((m_index << 16) + 3);
  	
  	while (blocks.hasNext()) {
  		AllocBlock block = (AllocBlock) blocks.next();

			block.addAddresses(addrs, baseAddr);
  		
  		baseAddr -= 32 * m_bitSize;
   	}
  }
}
