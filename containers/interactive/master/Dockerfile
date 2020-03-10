# based on official Ubuntu 16.04 release
FROM ubuntu:16.04

USER root

# base directory
WORKDIR /sgp

# # ------------------------------------------------------------
# # ENVIRONMENT VARIABLES
# # ------------------------------------------------------------

ENV JANUSGRAPH_HOME=/sgp/janusgraph

# # ------------------------------------------------------------
# # Set up tools
# # ------------------------------------------------------------

RUN apt-get update &&\
	apt-get -y install default-jdk openssh-server python python3-pip dnsutils vim sudo memcached gnuplot

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 10 &&\ 
  pip3 install --upgrade pip &&\
  python -m pip install pandas &&\
  python -m pip install numpy &&\
  python -m pip install jmxquery &&\
  python -m pip install jproperties
  
COPY janusgraph.tar.gz /sgp/
RUN tar -xzvf /sgp/janusgraph.tar.gz
COPY scripts /sgp/scripts
RUN chmod +x -R /sgp/scripts 

# # ------------------------------------------------------------
# # SSH Setup
# # ------------------------------------------------------------
# Add host keys
RUN cd /etc/ssh/ && ssh-keygen -A -N ''

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
# # Initialization
# # ------------------------------------------------------------
COPY ./docker-entrypoint.py /
ENTRYPOINT ["python", "/docker-entrypoint.py"]
