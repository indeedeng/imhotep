---
layout: default
title: ImhotepDaemon Setup
permalink: /docs/aws/imhotep-daemon/
---



1. Launch a new EC2 instance:

    1. In the Choose an Amazon Machine Image step, select the latest **Amazon Linux AMI (HVM), SSD Volume Type**.

    2. In the Choose an Instance Type step, select a type with at least 15GB of RAM and SSD instance storage from the r3, r4, m3, m4, c3, or c4 families. A good starting point is r3.large.

    3. In the Configure Instance Details step, select the same subnet you selected for Zookeeper.

    4. In the Add Storage step, click **Add New Volume** and select **Instance Store 0** from the dropdown. This will be the attached SSD that the ImhotepDaemon uses to store and access the downloaded data shards.

    5. In the Add Tags step, set the Name tag to "ImhotepDaemon."

    6. In the Create Security Group step, select the "default" security group and the ssh security group you created previously (“Imhotep SSH Access Security”).

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

    12. Format and mount /var/data on the SSD you attached for local Imhotep storage by running the following commands:
    
        ```
        mkfs.ext4 -N 1000000 -m 1 -O dir_index,extent,sparse_super /dev/xvdb
        mkdir /var/data
        mount -t ext4 /dev/xvdb /var/data
        ```

    13. Confirm that /var/data is properly configured by running lsblk. You should see output like this:
    
        ```
        [root@ip-***-**-**-** ~]# lsblk
        NAME    MAJ:MIN RM SIZE RO TYPE MOUNTPOINT
        xvda    202:0    0   8G  0 disk
        └─xvda1 202:1    0   8G  0 part /
        xvdb    202:16   0  30G  0 disk /var/data
        ```

    14. Create directories and configure permissions:
    
        ```
        mkdir /opt/imhotep
        mkdir /var/data/indexes
        mkdir /var/data/imhotep
        mkdir /var/data/imhotep/logs
        mkdir /var/data/imhotep/tmp
        chmod a+rw /var/data/imhotep
        chmod a+rw /var/data/indexes
        chmod a+rw /var/data/imhotep/logs
        chmod a+rw /var/data/imhotep/tmp
        ```

    15. Create an imhotep user:
        
        ```
        useradd -r imhotep
        ```

    16. Install Java 7: [instructions](../install-java/)

    17. Install and configure supervisor
        
        ```
        easy_install supervisor
        mkdir -p /opt/supervisor
        chmod a+rw /opt/supervisor
        curl https://raw.githubusercontent.com/indeedeng/imhotep-cloudformation/master/supervisor/supervisord.conf > /etc/supervisord.conf
        curl https://raw.githubusercontent.com/indeedeng/imhotep-cloudformation/master/supervisor/imhotep.conf > /opt/supervisor/imhotep.conf
        curl https://raw.githubusercontent.com/indeedeng/imhotep-cloudformation/master/supervisor/supervisor.init.sh > /etc/rc.d/init.d/supervisor
        chmod a+x /etc/rc.d/init.d/supervisor
        chkconfig --level 345 supervisor on
        ```

    18. Set up imhotep data directories
    
        ```
        mkdir -p /var/data/indexes /var/data/imhotep/logs
        chown -R imhotep.imhotep /var/data/
        ```

    19. Mount a tmpfs partition
    
        ```
        mkdir -p /var/tempFS
        chown imhotep /var/tempFS
        chgrp imhotep /var/tempFS
        mount -t tmpfs -o size=10g tmpfs /var/tempFS
        ```

    20. Configure logging
    
        ```
        curl https://raw.githubusercontent.com/indeedeng/imhotep-cloudformation/master/imhotep/log4j.xml > /opt/imhotep/log4j.xml
        ```

    21. Configure kernel settings

        ```
        echo 80 > /proc/sys/vm/dirty_ratio
        echo 80 > /proc/sys/vm/dirty_background_ratio
        echo 36000 > /proc/sys/vm/dirty_expire_centisecs
        ```

    22. Download Imhotep server build. Note that there may be a more recent version available, [check here](https://indeedeng-imhotep-build.s3.amazonaws.com/).
    
        ```
        wget -O /tmp/imhotep-server-1.0.11-SNAPSHOT-complete.tar.gz https://indeedeng-imhotep-build.s3.amazonaws.com/imhotep-server-1.0.11-SNAPSHOT-complete.tar.gz
        ```

    23. Install Imhotep server build:
    
        ```
        mkdir -p /var/data/imhotep/source/target
        cp /tmp/imhotep*.tar.gz /var/data/imhotep/source/target
        cd /opt/imhotep
        tar xzf /var/data/imhotep/source/target/imhotep-server*.tar.gz 
        ln -s /opt/imhotep/imhotep-server-* /opt/imhotep/imhotep-server
        ```

    24. Extract and install native libraries:
    
        ```
        mkdir -p /opt/imhotep/lib
        cd /opt/imhotep/lib
        rm -rf native
        jar xvf /opt/imhotep/imhotep-server-*/lib/imhotep-server-*.jar native/Linux-amd64/
        jar xvf /opt/imhotep/imhotep-server-*/lib/util-mmap-*.jar native/Linux-amd64/
        ln -s /opt/imhotep/lib/native/Linux-amd64/libmetricregroup.so.* /usr/lib/libmetricregroup.so
        ln -s /opt/imhotep/lib/native/Linux-amd64/libvarint.so.* /usr/lib/libvarint.so
        ln -s /opt/imhotep/lib/native/Linux-amd64/libindeedmmap.so.* /usr/lib/libindeedmmap.so
        ```

    25. Create /opt/imhotep/imhotep-caching.yaml (user imhotep, group imhotep) using the template below. Set s3-bucket to the name of the data bucket you created earlier. Set s3-key and s3-secret to the access key id and secret you created before.

        ```
        ---
        -   type: S3
            order: 2
            mountpoint: /
            s3-bucket: DATA_BUCKET_NAME_HERE
            s3-key: S3_KEY_HERE
            s3-secret: S3_SECRET_HERE
        -   type: SQAR_AUTOMOUNTING
            order: 4
            mountpoint: /
        -   type: CACHED
            order: 6
            mountpoint: /
            cache-dir: /var/data/file_cache
            cacheSizeMB: 32000
        ```

    26. Create /opt/imhotep/imhotep.sh (owner imhotep, group imhotep, mode 0755) using the template below. Set MAX_HEAP to the size in GB of memory allocated to the ImhotepDaemon. A good guideline for MAX_HEAP is to leave between 10GB and 20% of instance memory free. (Examples: r3.large 5, r3.xlarge 20, r3.2xlarge 50.) Set ZOOKEEPER_HOST to the address (Private IP or Private DNS) of the Zookeeper instance you configured earlier.
    
        ```
        #!/bin/bash
        export CLASSPATH="/opt/imhotep:/opt/imhotep/imhotep-server/lib/*:"$CLASSPATH
        MAX_HEAP_GB=YOUR_MAX_HEAP_HERE (5 for r3.large)
        ZOOKEEPER_HOST=ZOOKEEPER_IP_HERE
        java -Xmx"$MAX_HEAP_GB"G -Dlog4j.configuration=file:///opt/imhotep/log4j.xml -Djava.io.tmpdir=/var/data/imhotep/tmp -Dcom.indeed.flamdex.simple.useNative=true -Dcom.indeed.flamdex.simple.useSSSE3=true com.indeed.imhotep.service.ImhotepDaemon /var/data/indexes /var/tempFS --port 12345 --memory $((MAX_HEAP_GB * 1024 - 512)) --zknodes $ZOOKEEPER_HOST:2181 --zkpath /imhotep/daemons --lazyLoadProps /opt/imhotep/imhotep-caching.yaml
        ```

    27. Start supervisor
    
        ```
        service supervisor start
        ```

**Next Step**: [Set up ImhotepFrontend](../imhotep-frontend/)
