FROM apache/hadoop:3

ADD ./update-repos.sh /opt/

USER root

RUN /bin/sh /opt/update-repos.sh

USER hadoop
