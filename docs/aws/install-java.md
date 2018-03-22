---
layout: default
title: Installing Java for Imhotep
permalink: /docs/aws/install-java/
---

Imhotep components require a Java virtual machine. Imhotep is known to work with Java version 7. Please download and install a Java 7 JDK from [Oracle](http://www.oracle.com/technetwork/java/javase/downloads/java-archive-downloads-javase7-521261.html).

These instructions assume you have downloaded jdk-7u80-linux-x64.rpm and are installing on a Linux AMI.

```
yum install -u glibc.i686
yum install -y jdk-7u80-linux-x64.rpm

alternatives --install /usr/bin/java java /usr/java/jdk1.7.0_80/jre/bin/java 20000  --slave /usr/bin/keytool keytool /usr/java/jdk1.7.0_80/jre/bin/keytool --slave /usr/bin/orbd orbd /usr/java/jdk1.7.0_80/jre/bin/orbd --slave /usr/bin/pack2000 pack2000 /usr/java/jdk1.7.0_80/jre/bin/pack200 --slave /usr/bin/rmid rmid /usr/java/jdk1.7.0_80/jre/bin/rmid --slave /usr/bin/rmiregistry rmiregistry /usr/java/jdk1.7.0_80/jre/bin/rmiregistry --slave /usr/bin/servertool servertool /usr/java/jdk1.7.0_80/jre/bin/servertool --slave /usr/bin/tnameserv tnameserv /usr/java/jdk1.7.0_80/jre/bin/tnameserv --slave /usr/bin/unpack2000 unpack2000 /usr/java/jdk1.7.0_80/jre/bin/unpack200
```

