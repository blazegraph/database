/*
 * This file contains JNI implementations of routines declared by
 * BytesUtil.java.
 *
 * @see http://java.sun.com/docs/books/jni/html/jniTOC.html
 */

/*
 * Compile the Java class and then generate the C header file from that class.
 * From the bigdata directory, do:

javac src/java/com/bigdata/btree/BytesUtil.java

javah -classpath src/java com.bigdata.btree.BytesUtil

 * This places the .class file in the source directory and the .h files in the
 * bigdata directory.
 */

/* Now compile the C file. You can compile this under linix as follows:
 *

set JAVA_HOME="/usr/java/j2sdk1.4.2_05"

export PATH=$PATH:/$JAVA_HOME/bin

export LD_LIBRARY_PATH=.

gcc -fPIC -g -I$JAVA_HOME/include -I$JAVA_HOME/include/linux -c BytesUtil.c

gcc -shared -W1,-soname,libBytesUtil.so -olibBytesUtil.so BytesUtil.o -lc

java -classpath src/java com.bigdata.btree.BytesUtil

 * On Win32, the following command builds a dynamic link library (DLL)
 * using the Microsoft Visual C++ compiler:

cl "-I%JAVA_HOME%\include" "-I%JAVA_HOME%\include\win32" -LD BytesUtil.c -FeBytesUtil.dll

other things tried, some of which may work or have useful optimizations:

cl options
/nologo /G6 /MTd /W3 /Gm /GX /ZI /Od /YX

link options:
kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib
/nologo /dll /incremental:no /machine:I386 /pdbtype:sept

cl "-I%JAVA_HOME%\include" "-I%JAVA_HOME%\include\win32" -c /G6 /MTd /W3 /Gm /GX /ZI /Od /YX BytesUtil.c

cl "-I%JAVA_HOME%\include" "-I%JAVA_HOME%\include\win32" -c BytesUtil.c

link /dll /debug /incremental:no /machine:I386 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib BytesUtil.obj

cl /GD /LD "-I%JAVA_HOME%\include" "-I%JAVA_HOME%\include\win32" BytesUtil.c

-- some possible options: /W3 /WX /MD 

or try something like:

cl /O1 /Zi /MD /D _STATIC_CPPLIB /W3 /DNDEBUG /DWIN32 /DIAL /D_LITTLE_ENDIAN /D_X86_ /Dx86 /DWIN32_LEAN_AND_MEAN /c *.c 

link /dll /opt:REF /incremental:no /debug /out:XXX.dll *.obj 

See http://weblogs.java.net/blog/kellyohair/archive/2006/01/index.html for this example.

*/
 
/* Java on the Win32 platform requires a different declaration for
   jlong (__int64 vs long long).  This causes problems that I have not
   been able to resolve when trying to use cygwin/gcc to compile the
   code under windows.

   typedef long long jlong;
*/

#include <jni.h>
#include <stdio.h>
#include "com_bigdata_btree_BytesUtil.h"

/**
 * Bit-wise comparison of unsigned byte[]s.
 * 
 * @param a A byte[].
 * 
 * @param b A byte[].
 * 
 * @return a negative integer, zero, or a positive integer as the
 * first argument is less than, equal to, or greater than the second.
 */
JNIEXPORT jint JNICALL Java_com_bigdata_btree_BytesUtil__1compareBytes
  (JNIEnv *env, jclass cl, jint alen, jbyteArray a, jint blen, jbyteArray b)
{

   // lock (or maybe copy) the array contents.
   register jbyte *a1 = (*env)->GetPrimitiveArrayCritical(env, a, 0);

   register jbyte *b1 = (*env)->GetPrimitiveArrayCritical(env, b, 0);

   register int i;

   register int ret;

   /* We need to check in case the VM tried to make a copy. */
   if (a1 != NULL && b1 != NULL) {

     // data are good - compare unsigned byte[]s.
     for ( i = 0; i < alen && i < blen; i++) {

       ret = ((unsigned char)a1[i]) - ((unsigned char)b1[i]);

       if (ret != 0) {

         // release lock on arrays.
         (*env)->ReleasePrimitiveArrayCritical(env, b, b1, 0);
         (*env)->ReleasePrimitiveArrayCritical(env, a, a1, 0);

	 // done: arrays differ at index[i].
         return ret;

       }

     }

     // release lock on arrays.
     (*env)->ReleasePrimitiveArrayCritical(env, b, b1, 0);
     (*env)->ReleasePrimitiveArrayCritical(env, a, a1, 0);

     // done.
     return alen - blen;

     } else {

       /* Throw an out of memory exception */

       jclass newExcCls;

       // release lock on arrays.

       if(b1 != NULL) (*env)->ReleasePrimitiveArrayCritical(env, b, b1, 0);

       if(a1 != NULL) (*env)->ReleasePrimitiveArrayCritical(env, a, a1, 0);

       newExcCls = (*env)->FindClass(env, 
                       "java/lang/OutOfMemoryError");

         if (newExcCls == NULL) {

             /* Unable to find the exception class, give up. */

             return 0;

         }

         (*env)->ThrowNew(env, newExcCls, "in ByteUtils JNI code");

	 return 0; // keep the compiler happy.

     }

}

/**
 * Byte-wise comparison of byte[]s (the arrays are treated as arrays of
 * unsigned bytes).
 * 
 * @param aoff The offset into <i>a</i> at which the comparison will
 * begin.
 *
 * @param alen The #of bytes in <i>a</i> to consider starting at
 * <i>aoff</i>.
 *
 * @param a A byte[].
 *
 * @param boff The offset into <i>b</i> at which the comparison will
 * begin.
 *
 * @param blen The #of bytes in <i>b</i> to consider starting at
 * <i>boff</i>.
 *
 * @param b A byte[].
 * 
 * @return a negative integer, zero, or a positive integer as the
 * first argument is less than, equal to, or greater than the second.
 */
JNIEXPORT jint JNICALL Java_com_bigdata_btree_BytesUtil__1compareBytesWithOffsetAndLen
  (JNIEnv *env, jclass cl,
   jint aoff, jint alen, jbyteArray a, 
   jint boff, jint blen, jbyteArray b
   )
{

   // lock (or maybe copy) the array contents.
   register jbyte *a1 = (*env)->GetPrimitiveArrayCritical(env, a, 0);

   register jbyte *b1 = (*env)->GetPrimitiveArrayCritical(env, b, 0);

   // last index to consider in a[].
   const jint alimit = aoff + alen;

   // last index to consider in b[].
   const jint blimit = boff + blen;

   register int i, j;

   register int ret;

   /* We need to check in case the VM tried to make a copy. */
   if (a1 != NULL && b1 != NULL) {

     for ( i = aoff, j = boff; i < alimit && j < blimit; i++, j++) {

       ret = ((unsigned char)a1[i]) - ((unsigned char)b1[j]);

       if (ret != 0) {

         // release lock on arrays.

         (*env)->ReleasePrimitiveArrayCritical(env, b, b1, 0);

         (*env)->ReleasePrimitiveArrayCritical(env, a, a1, 0);

         return ret;

       }

     }

     // release lock on arrays.

     (*env)->ReleasePrimitiveArrayCritical(env, b, b1, 0);

     (*env)->ReleasePrimitiveArrayCritical(env, a, a1, 0);

     return alen - blen;

     } else {

       /* Throw an out of memory exception */

       jclass newExcCls;

       // release lock on arrays.

       if(b1 != NULL) (*env)->ReleasePrimitiveArrayCritical(env, b, b1, 0);

       if(a1 != NULL) (*env)->ReleasePrimitiveArrayCritical(env, a, a1, 0);

       newExcCls = (*env)->FindClass(env, 
                       "java/lang/OutOfMemoryError");

         if (newExcCls == NULL) {

             /* Unable to find the exception class, give up. */

             return 0;

         }

         (*env)->ThrowNew(env, newExcCls, "in ByteUtils JNI code");

	 return 0; // keep the compiler happy.

     }

}
