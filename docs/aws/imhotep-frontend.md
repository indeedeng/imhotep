---
layout: default
title: Imhotep Frontend Setup
permalink: /docs/aws/imhotep-frontend/
---

1. Create an EC2 instance:

    1. In the Choose an Amazon Machine Image step, select the latest **Amazon Linux AMI (HVM), SSD Volume Type**.

    2. In the Choose an Instance Type step, select c3.xlarge.

    3. In the Configure Instance Details step, select the same subnet you selected for Zookeeper and ImhotepDaemon.

    4. In the Add Storage step, click **Add New Volume** and select **Instance Store 0** from the dropdown.

    5. In the Add Tags step, set the Name tag to "Imhotep Frontend."

    6. In the Create Security Group step, select the "default" security group and both of the security groups you created previously (“Imhotep SSH Access Security” and “Imhotep Frontend Access Security”).

    7. Review your choices and click the Launch button.

    8. Select the Key Pair you created earlier and finish creating the launch configuration.

2. Connect to instance using ssh: in the EC2 dashboard, select the instance and click **Connect** to see instructions for connecting. You will need to use the key pair you created earlier.

3. In the ssh console:

    9. Become root:

        ```
        sudo su -
        ```

    10. Update the system:

        ```
        yum update -y
        ```

    11. Install required packages:

        ```
        yum install -y tomcat7 httpd mod_ssl
        ```

    12. Create a user for the shard builder:

        ```
        useradd shardbuilder
        ```

    13. Format and mount `/var/data` on the SSD you attached for local Imhotep storage by running the following commands:

        ```
        umount /media/ephemeral0
        mkfs.ext4 -N 1000000 -m 1 -O dir_index,extent,sparse_super /dev/xvdb
        mkdir /var/data
        mount -t ext4 /dev/xvdb /var/data
        ```

    14. Confirm that `/var/data` is properly configured by running lsblk. You should see output like this:

        ```
        [root@ip-***-**-**-** ~]# lsblk
        NAME    MAJ:MIN RM SIZE RO TYPE MOUNTPOINT
        xvda    202:0    0   8G  0 disk
        └─xvda1 202:1    0   8G  0 part /
        xvdb    202:16   0  40G  0 disk /var/data
        ```

    15. Install Java 7: [instructions](../install-java/)

    16. Install configuration files:

        ```
        curl https://raw.githubusercontent.com/indeedeng/imhotep-cloudformation/master/iql/crontab >> /etc/crontab
        curl https://raw.githubusercontent.com/indeedeng/imhotep-cloudformation/master/iql/catalina.properties > /etc/tomcat7/catalina.properties
        curl https://raw.githubusercontent.com/indeedeng/imhotep-cloudformation/master/iupload/server.xml > /etc/tomcat7/server.xml
        curl https://raw.githubusercontent.com/indeedeng/imhotep-cloudformation/master/apache/httpd.conf > /etc/httpd/conf/httpd.conf
        curl https://raw.githubusercontent.com/indeedeng/imhotep-cloudformation/master/apache/ssl.conf > /etc/httpd/conf.d/ssl.conf
        ```

    17. If you intend to access the Imhotep webapps through HTTP, edit `/etc/httpd/conf/httpd.conf` to uncomment the `Listen 80` line.

    18. Create `/opt/tomcat_shared/` directory containing a new file `core-site.xml`. This Hadoop client configuration is used to write files to S3. Set s3-key and s3-secret to the access key id and secret you created before.

        ```xml
        <?xml version="1.0"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
        <configuration>
                  <property>
                      <name>hadoop.tmp.dir</name>
                      <value>/var/data/hdfs-tmp</value>
                  </property>
                  <property>
                      <name>fs.s3n.awsAccessKeyId</name>
                      <value>S3_KEY_HERE</value>
                  </property>
                  <property>
                      <name>fs.s3n.awsSecretAccessKey</name>
                      <value>S3_SECRET_HERE</value>
                  </property>
                  <property>
                      <!-- config valid for clientserver -->
                      <name>fs.defaultFS</name>
                      <value>file:///opt/iupload/</value>
                      <final>true</final>
                  </property>
                  <property>
                      <!-- config valid for clientserver -->
                      <name>io.compression.codec.lzo.class</name>
                      <value>com.hadoop.compression.lzo.LzoCodec</value>
                  </property>
                  <property>
                      <!-- config valid for clientserver -->
                      <name>io.compression.codecs</name>
                      <value>
                         org.apache.hadoop.io.compress.DefaultCodec,
                         org.apache.hadoop.io.compress.GzipCodec,
                         org.apache.hadoop.io.compress.BZip2Codec,
                         com.hadoop.compression.lzo.LzoCodec,
                         com.hadoop.compression.lzo.LzopCodec,
                         org.apache.hadoop.io.compress.SnappyCodec,
                         com.hadoop.compression.lzo.LzoCodec,
                         com.hadoop.compression.lzo.LzopCodec
                      </value>
                  </property>
        </configuration>
        ```

    19. Install the TSV converter:

        ```
        mkdir /opt/imhotepTsvConverter
        cd /opt/imhotepTsvConverter
        wget -O imhotepTsvConverter.tar.gz https://indeedeng-imhotep-build.s3.amazonaws.com/tsv-builder-1.0.1-SNAPSHOT-complete.tar.gz
        tar xzf imhotepTsvConverter.tar.gz
        ln -s /opt/imhotepTsvConverter/tsv-builder-* /opt/imhotepTsvConverter/shardBuilder
        mkdir /opt/imhotepTsvConverter/conf
        chown shardbuilder /opt/imhotepTsvConverter/conf
        cp /opt/tomcat_shared/core-site.xml /opt/imhotepTsvConverter/conf
        mkdir /opt/imhotepTsvConverter/logs
        chown -R shardbuilder /opt/imhotepTsvConverter
        mkdir /var/data/build
        chmod go+w /var/data/build
        ```

    20. Create `/opt/imhotepTsvConverter/tsvConverter.sh` (owner `shardbuilder`, group `shardbuilder`, mode `0755`), the script that runs the TSV converter. Set `S3_BUILD_BUCKET` and `S3_DATA_BUCKET` to the appropriate names for the buckets you created earlier.

        ```
        #!/bin/bash

        lockfile -r 0 /tmp/tsvConverter.lock || exit 1

        export CLASSPATH="/opt/imhotepTsvConverter/shardBuilder/lib/*:/opt/imhotepTsvConverter/conf:"$CLASSPATH

        S3_BUILD_BUCKET=S3_BUILD_BUCKET_HERE
        S3_DATA_BUCKET=S3_DATA_BUCKET_HERE

        java -Xmx20G com.indeed.imhotep.builder.tsv.TsvConverter \
            --index-loc s3n://$S3_BUILD_BUCKET/iupload/tsvtoindex \
            --success-loc s3n://$S3_BUILD_BUCKET/iupload/indexedtsv \
            --failure-loc s3n://$S3_BUILD_BUCKET/iupload/failed \
            --data-loc s3n://$S3_DATA_BUCKET/ \
            --build-loc /var/data/build

        # Remove the lockfile
        rm -f /tmp/tsvConverter.lock
        ```

    21. Install the IQL webapp:

        ```
        mkdir -p /var/data/iql/ramses_metadata
        chmod go+w /var/data/
        chmod go+w /var/data/iql/
        chmod go+w /var/data/iql/ramses_metadata
        mkdir -p /var/data/tomcat7/temp
        chmod go+w /var/data/tomcat7
        chmod go+w /var/data/tomcat7/temp
        mkdir -p /var/data/iql/local_cache
        chmod go+w /var/data/iql/local_cache
        mkdir /opt/iql
        wget -O /opt/iql/iql.war https://indeedeng-imhotep-build.s3.amazonaws.com/iql-1.0.6-SNAPSHOT.war
        cp /opt/iql/iql.war /var/lib/tomcat7/webapps/ 
        ```

    22. Configure kernel settings

        ```
        echo 80 > /proc/sys/vm/dirty_ratio
        echo 80 > /proc/sys/vm/dirty_background_ratio
        echo 36000 > /proc/sys/vm/dirty_expire_centisecs
        ```

    23. Change Tomcat temporary directory:

        ```
        sed -i 's/^CATALINA_TMPDIR=.*/CATALINA_TMPDIR=\"\/var\/data\/tomcat7\/temp\"/' /etc/tomcat7/tomcat7.conf
        ```

    24. Create `/etc/tomcat7/iql.properties`, replacing `ZOOKEEPER_HOST` with the Private IP or Private DNS of your zookeeper instance, and setting S3 key/secret/buckets as noted:

        ```
        imhotep.daemons.zookeeper.quorum=ZOOKEEPER_HOST
        imhotep.daemons.zookeeper.path=/imhotep/daemons
        imhotep.daemons.host=

        imhotep.daemons.interactive.zookeeper.quorum=
        imhotep.daemons.interactive.zookeeper.path=
        imhotep.daemons.interactive.host=

        ramses.metadata.dir=/var/data/iql/ramses_metadata
        query.cache.enabled=true
        query.cache.backend=S3
        query.cache.s3.bucket=S3_CACHE_BUCKET
        query.cache.s3.s3key=S3_KEY
        query.cache.s3.s3secret=S3_SECRET
        topterms.cache.dir=/var/data/iql/local_cache
        user.concurrent.query.limit=2
        row.limit=1000000

        shortlink.enabled=true
        shortlink.backend=S3
        shortlink.s3.bucket=S3_DATA_BUCKET
        shortlink.s3.s3key=S3_KEY
        shortlink.s3.s3secret=S3_SECRET
        ```

    25. Create `/etc/tomcat7/iupload.properties`, replacing `S3_BUILD_BUCKET` with your S3 build bucket:

        ```
        hdfs.base.path.qa=s3n://S3_BUILD_BUCKET/iuploadqa/
        hdfs.base.path.prod=s3n://S3_BUILD_BUCKET/iupload/

        permission.provider.use.default=true
        use.qa.repository=false
        ```

    26. Create /tmp/zookeeper.ip, containing just the Private IP address of your zookeeper instance.

    27. Install IUpload:

        ```
        mkdir /opt/iupload
        wget -O /opt/iupload/iupload.war https://indeedeng-imhotep-build.s3.amazonaws.com/iupload-war-1.0.2-SNAPSHOT.war
        cp /opt/iupload/iupload.war /var/lib/tomcat7/webapps/
        ```

    28. Create temporary data directory:

        ```
        mkdir -p /var/data/hdfs-tmp/s3
        chmod -R go+w /var/data/hdfs-tmp
        ```

    29. (Optional) If you will use https to access the webapps, create a self-signed cert:

        ```
        mkdir /var/data/apache
        chmod go+rw /var/data/apache
        cd /var/data/apache
        PUBLIC_HOSTNAME=`curl http://169.254.169.254/latest/meta-data/public-hostname`
        openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout ssl.key -out ssl.crt -subj "/CN=$PUBLIC_HOSTNAME"
        ```

    30. To password protect the webapps, create users for Apache:

        ```
        htpasswd -b -c /var/data/apache/passwords USER_NAME PASSWORD
        ```

    31. Start apache and tomcat:

        ```
        service httpd start
        service tomcat7 start
        ```

[Back to Instruction Index](../)
