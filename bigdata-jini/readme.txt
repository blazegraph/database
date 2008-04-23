Some notes on installation and use follow:

JINI

- jini is used as a service fabric for bigdata.  <start up jini and
  then configure your data and metadata services; clients then
  discover those services>

- jini 2.1 may report errors locating the shared libraries from awk,
  dirname, basename and grep when installing under un*x.  The problem
  is an assumption about the kernel version.  This problem is resolved
  by editing the installer (with a hex editor) and the launchall
  script.  See http://www.jini.org/wiki/Category:Getting_Started and
  http://www.linuxquestions.org/questions/showthread.php?t=370056 for
  a resolution.

- Downloadable code is NOT required for deployments, but MAY be useful
  for the following purposes:

   (a) exposing services to the Jini service browser;

   (b) running procedures against services which were deployed before
       the procedures were written;

  If you have a complete classpath when running the various services
  then jini will not seek to transfer code from the client as the code
  will be resolved locally by the service.

  In order to support downloadable code you need to have an HTTP
  server that will serve class files to jini, including the interfaces
  for all remote services.  You can use any HTTP server for this
  purpose, and the server can be located on any machine accessible
  from the host(s) on which you are running jini.  As a convenience,
  jini bundles a simple HTTP server that you can start using a command
  like the following:

    java -jar ${JINI_HOME}/lib/classserver.jar -port 8080 -dir classes -trees -verbose&

  The javadoc describes the logging and command line options for this
  HTTP server.
  
    https://java.sun.com/products/jini/2.1/doc/api/com/sun/jini/tool/ClassServer.html

  The directory from which downloadable code will be served should
  contain at least the bigdata jar(s) plus any remote application code
  that you have defined (i.e., code that will run in the server
  process).

  The recommended approach to downloadable code is to extract the
  relevant classes into a directory that will be named to the HTTP
  server as follows.  Assuming that bigdata.jar is located in the
  current directory:

  mkdir classes
  cd classes
  jar xfz ../bigdata.jar

  If you deploy a new version of any JAR, then you SHOULD delete the
  classes directory and redeploy all relevant JARs to make sure that
  old class files are not left lying around.

- You can enable NIO support with JERI using TCP by specifying the
  following property to the JVM.  Note that JRMP does NOT allow for
  the possibility of NIO.

	-Dcom.sun.jini.jeri.tcp.useNIO=true

  More information on JERI and NIO is available using the following links.

   http://archives.java.sun.com/cgi-bin/wa?A2=ind0504&L=jini-users&P=33490
   http://archives.java.sun.com/cgi-bin/wa?A2=ind0506&L=jini-users&P=9626
   http://archives.java.sun.com/cgi-bin/wa?A2=ind0504&L=jini-users&D=0&P=26542
   http://java.sun.com/products/jini/2.0.1/doc/api/net/jini/jeri/tcp/package-summary.html

  Note that one server thread will still be required per concurrent RPC request
  owing to the semantics of RPC (call and wait for response) and the definition
  of JERI.

  