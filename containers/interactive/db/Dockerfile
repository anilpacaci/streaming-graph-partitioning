# based on official Ubuntu 16.04 release
FROM cassandra:2.1

USER root

# base directory
WORKDIR /sgp

# # ------------------------------------------------------------
# # ENVIRONMENT VARIABLES
# # ------------------------------------------------------------

ENV JANUSGRAPH_HOME=/sgp/janusgraph

# # ------------------------------------------------------------
# # Set up JanusGraph code
# # ------------------------------------------------------------

# install necessary dependencies for powergraph
RUN apt-get update &&\
	apt-get -y install default-jdk gcc openssh-server openmpi-bin python dnsutils sudo vim iputils-ping

# # ------------------------------------------------------------
# # SSH Setup
# # ------------------------------------------------------------
# Add host keys
RUN cd /etc/ssh/ && ssh-keygen -A -N ''
RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd


# Config SSH Daemon
RUN  sed -i "s/#PasswordAuthentication.*/PasswordAuthentication no/g" /etc/ssh/sshd_config \
  && sed -i "s/PermitRootLogin.*/PermitRootLogin yes/g" /etc/ssh/sshd_config \
  && sed -i "s/#AuthorizedKeysFile/AuthorizedKeysFile/g" /etc/ssh/sshd_config
 
# Set up user's public and private keys
ENV SSHDIR /root/.ssh
RUN mkdir -p ${SSHDIR}

# Default ssh config file that skips (yes/no) question when first login to the host
RUN echo "StrictHostKeyChecking no" > ${SSHDIR}/config

COPY ssh/ ${SSHDIR}/

RUN cat ${SSHDIR}/*.pub >> ${SSHDIR}/authorized_keys
RUN chmod -R 600 ${SSHDIR}/* \
         && chown -R ${USER}:${USER} ${SSHDIR}

# # ------------------------------------------------------------
# # initialization script
# # ------------------------------------------------------------
# copy JanusGraph Codebase
COPY janusgraph.tar.gz /sgp/
RUN tar -xzvf /sgp/janusgraph.tar.gz
COPY scripts/ /sgp/

# copy cassandra specific configuration
COPY cassandra-env.sh /etc/cassandra

# # ------------------------------------------------------------
# # initialization script
# # ------------------------------------------------------------
COPY ./docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]

# # ------------------------------------------------------------
# # Setup 
# # ------------------------------------------------------------

# ssh ports
EXPOSE 22
# janusgraph ports
EXPOSE 8182

