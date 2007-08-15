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

- You need to have an HTTP server that will serve class files for
  downloadable code to jini, including the interfaces for all remote
  services.  You can use any HTTP server for this purpose, and the
  server can be located on any machine accessible from the host(s) on
  which you are running jini.  As a convenience, jini bundles a simple
  HTTP server that you can start using a command like the following:

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
