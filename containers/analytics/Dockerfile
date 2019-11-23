# based on official Ubuntu 16.04 release
FROM ubuntu:16.04

USER root

# base directory
WORKDIR /sgp

# # ------------------------------------------------------------
# # Set up PowerLyra code
# # ------------------------------------------------------------

# install necessary dependencies for powergraph
RUN apt-get update &&\
	apt-get -y install default-jdk gcc openssh-server openmpi-bin python python-pip dnsutils sudo

# install pandas
RUN pip install --upgrade pip &&\
    python -m pip install pandas

# copy powerlyra codebase
COPY powerlyra/ /sgp/powerlyra/
COPY scripts/ /sgp/scripts/

# # ------------------------------------------------------------
# # Set up default user 
# # ------------------------------------------------------------

ARG USER=mpi
ENV USER ${USER}
RUN useradd -ms /bin/bash ${USER} \
      && echo "${USER}   ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

ENV USER_HOME /home/${USER}
RUN chown -R ${USER}:${USER} ${USER_HOME} &&\
	mkdir /sgp/parameters/ &&\
	mkdir /sgp/results/ &&\
	chown -R ${USER}:${USER} /sgp &&\
	chmod -R 777 /sgp

# # ------------------------------------------------------------
# # Set up SSH Server 
# # ------------------------------------------------------------

# Add host keys
RUN cd /etc/ssh/ && ssh-keygen -A -N ''

# Config SSH Daemon
RUN  sed -i "s/#PasswordAuthentication.*/PasswordAuthentication no/g" /etc/ssh/sshd_config \
  && sed -i "s/#PermitRootLogin.*/PermitRootLogin no/g" /etc/ssh/sshd_config \
  && sed -i "s/#AuthorizedKeysFile/AuthorizedKeysFile/g" /etc/ssh/sshd_config
 
# Set up user's public and private keys
ENV SSHDIR ${USER_HOME}/.ssh
RUN mkdir -p ${SSHDIR}

# Default ssh config file that skips (yes/no) question when first login to the host
RUN echo "StrictHostKeyChecking no" > ${SSHDIR}/config

COPY ssh/ ${SSHDIR}/

RUN cat ${SSHDIR}/*.pub >> ${SSHDIR}/authorized_keys
RUN chmod -R 600 ${SSHDIR}/* \
         && chown -R ${USER}:${USER} ${SSHDIR}

# # ------------------------------------------------------------
# # MPI Setup
# # ------------------------------------------------------------

COPY get_hosts /home/mpi/get_hosts
RUN chmod +x /home/mpi/get_hosts &&\
	chown -R ${USER}:${USER} /home/mpi/get_hosts

# # Start SSH Server 
RUN sudo mkdir /var/run/sshd
EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]

