#!/bin/bash 
 env |grep HADOOPHOST > /tmp/hadoophost

 sed 's/^.*=//g' /tmp/hadoophost>/tmp/result

 cat /tmp/result >> /etc/hosts
 
 java -Xms1024m -Xmx1024m -jar ./app.jar
