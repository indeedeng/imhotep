---
layout: default
title: Zookeeper Setup
permalink: /docs/aws/zookeeper/
---


1. Launch a new EC2 instance:

    1. In the Choose an Amazon Machine Image step, select the latest Amazon Linux AMI (HVM), SSD Volume Type.

    2. In the Choose an Instance Type step, select m3.medium.

    3. In the Configure Instance Details step, select a subnet and make note of it for setting up the ImhotepDaemon and Frontend instances later.

    4. In the Add Storage step, click Add New Volume and select Instance Store 0 from the dropdown. This will be the attached SSD that Zookeeper uses for its file data.

    5. In the Add Tags step, set the Name tag to "Zookeeper Instance."

    6. In the Create Security Group step, select the "default" security group and the SSH security group you created previously (“Imhotep SSH Access Security”).

    7. Review your choices and click the Launch button.

    8. Select the Key Pair you created earlier and finish creating the launch configuration.

    9. Save the **Private** (internal) IP address of this instance for later use.

2. Connect to instance using ssh: in the EC2 dashboard, select the instance and click **Connect** to see instructions for connecting. You will need to use the key pair you created earlier.

3. In the ssh console:

    10. Become root:
    
        ```
        sudo su -
        ```

    11. Update the system:
    
        ```
        yum update -y
        ```

    13. Format and mount /var/data on the SSD you attached for local Imhotep storage by running the following commands:
    
        ```
        umount /media/ephemeral0
        mkfs.ext4 -N 1000000 -m 1 -O dir_index,extent,sparse_super /dev/xvdb
        mkdir /var/data
        mount -t ext4 /dev/xvdb /var/data
        ```

    14. Confirm that /var/data is properly configured by running lsblk. You should see output like this:
    
        ```
        [root@ip-***-**-**-** ~]# lsblk
        NAME    MAJ:MIN RM SIZE RO TYPE MOUNTPOINT
        xvda    202:0    0   8G  0 disk
        └─xvda1 202:1    0   8G  0 part /
        xvdb    202:16   0  30G  0 disk /var/data
        ```

    15. Install Java 7: [instructions](../install-java/)

    16. Download and unpack zookeeper:
    
        ```
        wget -O /tmp/zookeeper.tar.gz http://archive.cloudera.com/cdh5/cdh/5/zookeeper-3.4.5-cdh5.10.0.tar.gz
        cd /opt 
        tar xzf /tmp/zookeeper.tar.gz 
        ln -s zookeeper-3.4.5-cdh5.10.0 zookeeper
        ```

    17. Create directories:
    
        ```
        mkdir /var/zookeeper_snapshots
        mkdir /var/data/zookeeper
        ```

    18. Create /opt/zookeeper/conf/zoo.cfg:
    
        ```
        tickTime=2000
        initLimit=10
        syncLimit=5
        dataDir=/var/zookeeper_snapshots
        clientPort=2181
        ```

    19. Start zookeeper:
    
        ```
        cd /var/data/zookeeper
        /opt/zookeeper/bin/zkServer.sh start
        ```

**Next Step**: [Set up ImhotepDaemon](../imhotep-daemon/)
