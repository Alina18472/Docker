#!/bin/bash

# sed -i s/mirror.centos.org/vault.centos.org/g /etc/yum.repos.d/*.repo
# sed -i s/^#.*baseurl=http/baseurl=http/g /etc/yum.repos.d/*.repo
# sed -i s/^mirrorlist=http/#mirrorlist=http/g /etc/yum.repos.d/*.repo
# yum clean all && yum -y update


sed -i 's/mirror.centos.org/vault.centos.org/g' /etc/yum.repos.d/*.repo && \
    sed -i 's/^#baseurl/baseurl/g' /etc/yum.repos.d/*.repo && \
    sed -i 's/^mirrorlist/#mirrorlist/g' /etc/yum.repos.d/*.repo && \
    yum clean all && \
    yum update -y && \
    yum install -y python3 python3-pip && \
    yum clean all