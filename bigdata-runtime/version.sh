mvn versions:set -DnewVersion=1.5.3-`date +%Y%m%d`
mvn clean package install
