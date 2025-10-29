# Parent image
FROM hubmap/api-base-image:1.2.0

LABEL description="HuBMAP Entity API Service"

# The commons branch to be used in requirements.txt during image build
# Default is main branch specified in docker-compose.yml if not set before the build
ARG COMMONS_BRANCH

# Change to directory that contains the Dockerfile
WORKDIR /usr/src/app

# Copy from host to image
COPY . .

# Set up the repository file for the mainline version of
# nginx which dnf should use (in the legacy "yum" location.)
RUN set -eux && \
    cat <<'EOF' > /etc/yum.repos.d/nginx.repo
[nginx-mainline]
name=nginx mainline repo
baseurl=http://nginx.org/packages/mainline/centos/$releasever/$basearch/
gpgcheck=1
enabled=0
gpgkey=https://nginx.org/keys/nginx_signing.key
module_hotfixes=true
EOF

# Reduce the number of layers in image by minimizing the number of separate RUN commands
# 1 - Install the prerequisites
# 2 - By default, the repository for stable nginx packages is used. We would like to use mainline nginx packages
# 3 - Install nginx (using the custom dnf/yum repo specified earlier)
# 4 - Remove the default nginx config file
# 5 - Overwrite the nginx.conf with ours to run nginx as non-root
# 6 - Remove the nginx directory copied from host machine (nginx/conf.d gets mounted to the container)
# 7 - Upgrade pip (the one installed in base image may be old) and install service requirements.txt packages
# 8 - Make the start script executable
# 9 - Clean the dnf/yum cache and other locations to reduce Docker Image layer size.
# Assume the base image has upgraded dnf and installed its dnf-plugins-core
 RUN dnf install --assumeyes dnf-plugins-core && \
     dnf config-manager --enable nginx-mainline && \
     dnf install --assumeyes nginx && \
     # Push aside nginx default.conf files that may exist on the system
     [ ! -f /etc/nginx/conf.d/default.conf ] || mv /etc/nginx/conf.d/default.conf /tmp/etc_nginx_conf.d_default.conf.ORIGINAL && \
     [ ! -f /etc/nginx/nginx.conf ] || mv /etc/nginx/nginx.conf /tmp/etc_nginx_nginx.conf.ORIGINAL && \
     # Install the nginx default.conf file just installed in WORKDIR
     mv nginx/nginx.conf /etc/nginx/nginx.conf && \
     # Clean up the nginx install directory in WORKDIR
     [ ! -d nginx ] || mv nginx /tmp/nginx_from_WORKDIR && \
     # Push aside the verification file from the base image which will
     # no longer report correctly once uWSGI is started for the service.
     [ ! -f /tmp/verify_uwsgi.sh ] || mv /tmp/verify_uwsgi.sh /tmp/verify_uwsgi.sh.ORIGINAL && \
     # Install the requirements.txt file for the service
     pip3.13 install --no-cache-dir --upgrade pip -r src/requirements.txt && \
     # Make the script referenced in the CMD directive below executable.
     chmod 755 start.sh && \
     # Clean up artifacts to slim down this layer of the Docker Image
     dnf clean all && \
     rm -rf /var/cache/dnf \
            /var/log/dnf \
     	     /var/log/yum \
     	     /root/.cache

# The EXPOSE instruction informs Docker that the container listens on the specified network ports at runtime. 
# EXPOSE does not make the ports of the container accessible to the host.
# Here 5000 is for the uwsgi socket, 8080 for nginx
EXPOSE 5000 8080

# Set an entrypoint by moving the file copied into the WORKDIR to
# the location referenced by the ENTRYPOINT directive below, and
# make it executable.
RUN mv entrypoint.sh /usr/local/bin/entrypoint.sh && \
    chmod 755 /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

CMD ["./start.sh"]
