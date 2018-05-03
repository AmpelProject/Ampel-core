# Create environment from official image

FROM continuumio/miniconda:4.3.27 AS conda
COPY requirements.txt /
RUN conda create --yes -n ampel --file /requirements.txt python=3 --channel=ampelproject --channel=conda-forge

# Transplant environment (without conda itself) into bare-bones alpine
FROM alpine:3.7
RUN apk add --no-cache ca-certificates wget libgcc libstdc++ && \
    wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://raw.githubusercontent.com/sgerrand/alpine-pkg-glibc/master/sgerrand.rsa.pub && \
    wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.27-r0/glibc-2.27-r0.apk && \
    apk add glibc-2.27-r0.apk && \
    rm glibc-2.27-r0.apk
COPY --from=conda /lib/x86_64-linux-gnu/libgcc_s.so.1 /usr/glibc-compat/lib/
COPY --from=conda /opt/conda/envs /opt/conda/envs

RUN /opt/conda/envs/ampel/bin/pip install graphitesend
RUN addgroup -g 1001 ampel && \
    adduser -D -u 1001 -G ampel ampel && \
    mkdir /Ampel /ztf /run/secrets
USER ampel

# For development, we want to have the supporting environment frozen into the
# image, but the Ampel code itself on the (writeable) host filesystem, mounted
# at /Ampel. If we could mount volumes in `docker build` we'd just do
# RUN /opt/conda/envs/ampel/bin/pip install -e /Ampel
# Since we can't, though, we have to run the above in a container instance and
# commit the result; see add-ampel.sh

ENV CONDA_PREFIX=/opt/conda/envs/ampel PATH=/opt/conda/envs/ampel/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

CMD ["sh"]
