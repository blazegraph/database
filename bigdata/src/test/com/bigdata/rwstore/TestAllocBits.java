package com.bigdata.rwstore;

import java.util.Random;

import junit.framework.TestCase;

public class TestAllocBits extends TestCase {
	
	public void testBitCounts() {
		Random r = new Random();
		
		for (int i = 0; i < 50000; i++) {
			final int tst = r.nextInt();
			final int r1 = countZeros(tst);
			final int r2 = 32 - Integer.bitCount(tst); // zeros are 32 - 1s
			assertTrue(r1 == r2);
		}
		
	}
	
	int countZeros(final int tst) {
		int cnt = 0;
		for (int bit = 0; bit < 32; bit++) {
			if ((tst & (1 << bit)) == 0) {
				cnt++;
			}
		}
		
		return cnt;
	}

}
