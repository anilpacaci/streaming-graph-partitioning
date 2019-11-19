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

# copy JanusGraph Codebase
COPY janusgraph/ ${JANUSGRAPH_HOME}

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

