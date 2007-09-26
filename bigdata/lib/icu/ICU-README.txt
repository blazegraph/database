This project redistributes ICU4C, ICU4J, and ICU4JNI version 3.6.

You must specify both the library path (to locate the DLLs for ICU4C and the JNI
interface) and the classpath (for the Java side of the JNI interface).  You can
edit the following line and run it to verify that ICU4JNI is running correctly
on your platform.

Note: Under Win32 platforms, the native libraries are located using the PATH vs
java.library.path.

java -Djava.library.path=c:\icu4jni\build\lib\icu -classpath ;c:\icu4jni\build\classes; com.ibm.icu4jni.test.TestAll
