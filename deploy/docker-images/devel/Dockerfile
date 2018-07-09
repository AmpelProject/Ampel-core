# Create environment from official image

FROM continuumio/miniconda:4.3.27 AS conda-base

COPY base-environment.yml /
RUN conda env create --file /base-environment.yml
RUN conda env remove -n potemkin

FROM conda-base AS conda

# Set up deploy key and clone Ampel git repo
RUN mkdir /root/.ssh/
ADD deploy-keys/id_rsa* /root/.ssh/
RUN ls /root/.ssh
RUN chmod 700 /root/.ssh/id_rsa*
RUN chown -R root:root /root/.ssh
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan github.com >> /root/.ssh/known_hosts
# Set deploy keys for each repo individually
# https://eclipsesource.com/blogs/2012/07/30/accessing-multiple-private-github-repositories-without-a-dedicated-build-user/ 
RUN find /root/.ssh/ -type f \( -iname 'id_rsa*' -and -not -iname '*.pub' \) | cut -d. -f 3 | while read repo; do \
echo $repo; \
echo "Host github-$repo" >> /root/.ssh/config; \
echo "    HostName github.com" >> /root/.ssh/config; \
echo "    User git" >> /root/.ssh/config; \
echo "    IdentityFile /root/.ssh/id_rsa.$repo" >> /root/.ssh/config; \
done
RUN mkdir /Ampel
RUN git clone git@github-ampel:AmpelProject/Ampel.git /Ampel/ampel-core
RUN git clone git@github-ampel-base:AmpelProject/Ampel-base.git /Ampel/ampel-base
RUN git clone git@github-ampel-ztfbh:robertdstein/Ampel-ZTFbh.git /Ampel/ampel-contrib-ztfbh
RUN git clone git@github-ampel-contrib-hu:AmpelProject/ampel-contrib-hu.git /Ampel/ampel-contrib-hu

COPY environment.yml /
RUN conda env create --file /environment.yml

# Transplant environment (without conda itself) into bare-bones alpine
FROM alpine:3.7
RUN apk add --no-cache ca-certificates wget libgcc libstdc++ findutils && \
    wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://raw.githubusercontent.com/sgerrand/alpine-pkg-glibc/master/sgerrand.rsa.pub && \
    wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.27-r0/glibc-2.27-r0.apk && \
    apk add glibc-2.27-r0.apk && \
    rm glibc-2.27-r0.apk

COPY --from=conda /lib/x86_64-linux-gnu/libgcc_s.so.1 /usr/glibc-compat/lib/
COPY --from=conda /opt/conda/envs /opt/conda/envs
COPY --from=conda /Ampel /Ampel

RUN addgroup -g 1001 ampel && \
    adduser -D -u 1001 -G ampel ampel && \
    mkdir /ztf /run/secrets && \
    mkdir -p /catalogs/catsHTM
USER ampel

ENV CONDA_PREFIX=/opt/conda/envs/ampel PATH=/opt/conda/envs/ampel/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

VOLUME ["/Ampel"]

ADD docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
