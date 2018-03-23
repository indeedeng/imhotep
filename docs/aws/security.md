---
layout: default
title: Security and Access Setup
permalink: /docs/aws/security/
---

### SSH Access Setup

Create a key pair (in **EC2 > Key Pairs**). You will use this to provide ssh access to your instances. Download the .pem file for your key pair.

### Security Group Setup

Create a security group (in **EC2 > Security Groups**) named "Imhotep SSH Access Security." [TODO: does it have to have this name?]

1. Use a description like "Enable SSH access on the inbound port."

2. Add a SSH port 22 rule, configured however you like to limit the source IPs that can SSH into your instances (e.g. limited to your personal or corporate network IPs).

3. Click Create.

Create a security group (in **EC2 > Security Groups**) named "Imhotep Frontend Access Security."

4. Use a description like "Enable web traffic inbound port(s)."

5. Add either an HTTP or HTTPS rule (or both, depending on how you want to access Imhotep frontend servers). Configure these rules however you like to limit the source IPs that can access your frontends (e.g. limited to your personal or corporate network IPs).

6. Click Create.

**Next Step**: [Set up Zookeeper](../zookeeper/)
