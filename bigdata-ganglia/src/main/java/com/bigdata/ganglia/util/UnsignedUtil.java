/*
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.ganglia.util;

public class UnsignedUtil {

	/** Maximum unsigned int16 value. */
	public static final int MAX_UINT16 = 0xffff;//(2 ^ 16) - 1;// aka 65535;

	/** Maximum unsigned int32 value. */
	public static final long MAX_UINT32 = 0xffffffffL;//(2L ^ 32L) - 1L;
	
	public static int encode(int v) {

		if (v < 0) {

			v = v - 0x80000000;

		} else {
        
			v = 0x80000000 + v;
        
		}
		
		return v;
		
	}
    
    public static final long encode(long v) {

        if (v < 0) {
            
            v = v - 0x8000000000000000L;

        } else {
            
            v = v + 0x8000000000000000L;
            
        }

        return v;
        
    }
    
}
