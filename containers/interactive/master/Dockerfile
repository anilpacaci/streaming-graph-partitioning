# based on official Ubuntu 16.04 release
FROM ubuntu:16.04

USER root

# base directory
WORKDIR /sgp

# # ------------------------------------------------------------
# # Set up tools
# # ------------------------------------------------------------

RUN apt-get update &&\
	apt-get -y install default-jdk openssh-server python dnsutils sudo memcached

# # ------------------------------------------------------------
# # ENVIRONMENT VARIABLES
# # ------------------------------------------------------------

ENV JANUSGRAPH_HOME=/sgp/janusgraph







# # ------------------------------------------------------------
# # Initialization
# # ------------------------------------------------------------

ENTRYPOINT ["python", "docker-entrypoint.py"]
