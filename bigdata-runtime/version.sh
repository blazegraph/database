mvn versions:set -DnewVersion=2.1.0-`date +%Y%m%d`
mvn clean package install
