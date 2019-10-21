# Parent image
FROM centos:7

LABEL description="HuBMAP Entity API Service" \
	version="0.1.0"

# Change to directory that contains the Dockerfile
WORKDIR /usr/src/app

# Copy from host to image
COPY . .

# Reduce the number of layers in image by minimizing the number of separate RUN commands
# 1 - Update the package listings
# 2 - Install wget and OpenJDK 8
# 3 - Download neo4j community server
# 4 - Extract neo4j tarball
# 5 - Rename folder to neo4j
# 6 - Remove the downloaded neo4j tarball
# 7 - Move the hubmap.db to neo4j database directory
# 8 - Move the neo4j config over to overwrite the default
# 9 - Make the start script executable
# 10 - Clean all yum cache
RUN yum update -y && \
    yum install -y wget java-1.8.0-openjdk && \
    wget http://dist.neo4j.org/neo4j-community-3.5.12-unix.tar.gz && \
    tar -zxvf neo4j-community-3.5.12-unix.tar.gz && \
    mv neo4j-community-3.5.12 neo4j && \
    rm neo4j-community-3.5.12-unix.tar.gz && \
    mv hubmap.db /usr/src/app/neo4j/data/databases && \
    mv neo4j.conf neo4j/conf && \
    chmod +x start.sh && \
    yum clean all
    
# The EXPOSE instruction informs Docker that the container listens on the specified network ports at runtime. 
# EXPOSE does not make the ports of the container accessible to the host.
EXPOSE 7474 7687

# Start the neo4j server when container spins up
CMD ["./start.sh"]