# Parent image
FROM hubmap/api-base-image:1.0.0

LABEL description="HuBMAP Entity API Service"

# The commons branch to be used in requirements.txt during image build
# Default is master branch specified in docker-compose.yml if not set before the build
ARG COMMONS_BRANCH

# Change to directory that contains the Dockerfile
WORKDIR /usr/src/app

# Copy from host to image
COPY . .

# http://nginx.org/en/linux_packages.html#RHEL-CentOS
# Set up the yum repository to install the latest mainline version of Nginx
RUN echo $'[nginx-mainline]\n\
name=nginx mainline repo\n\
baseurl=http://nginx.org/packages/mainline/centos/$releasever/$basearch/\n\
gpgcheck=1\n\
enabled=0\n\
gpgkey=https://nginx.org/keys/nginx_signing.key\n\
module_hotfixes=true\n'\
>> /etc/yum.repos.d/nginx.repo

# Reduce the number of layers in image by minimizing the number of separate RUN commands
# 1 - Install the prerequisites
# 2 - By default, the repository for stable nginx packages is used. We would like to use mainline nginx packages
# 3 - Install nginx (using the custom yum repo specified earlier)
# 4 - Remove the default nginx config file
# 5 - Overwrite the nginx.conf with ours to run nginx as non-root
# 6 - Remove the nginx directory copied from host machine (nginx/conf.d gets mounted to the container)
# 7 - Upgrade pip (the one installed in base image may be old) and install flask app dependencies (pip3 also works)
# 8 - Make the start script executable
# 9 - Clean all yum cache
RUN yum install -y yum-utils && \
    yum-config-manager --enable nginx-mainline && \
    yum install -y nginx && \
    rm /etc/nginx/conf.d/default.conf && \
    mv nginx/nginx.conf /etc/nginx/nginx.conf && \
    rm -rf nginx && \
    pip install --upgrade pip -r src/requirements.txt && \
    chmod +x start.sh && \
    yum clean all 

# The EXPOSE instruction informs Docker that the container listens on the specified network ports at runtime. 
# EXPOSE does not make the ports of the container accessible to the host.
# Here 5000 is for the uwsgi socket, 8080 for nginx
EXPOSE 5000 8080

# Set an entrypoint
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

CMD ["./start.sh"]
