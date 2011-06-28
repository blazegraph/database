set JAVA_HOME="C:\\Program\ Files\\Java\\jdk1.5.0_07"
#set JAVA_HOME="C:\\Program\ Files\\Java\\jrockit-R26.4.0-jdk1.5.0_06"
echo $JAVA_HOME
#ls -l "$JAVA_HOME\include"

gcc -I"$JAVA_HOME\include" -I"$JAVA_HOME\include\win32" BytesUtil.c