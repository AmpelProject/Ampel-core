# Create environment from official image

FROM continuumio/miniconda:4.3.27 AS conda

# Set up deploy key and clone Ampel git repo
RUN mkdir /root/.ssh/
ADD id_rsa /root/.ssh/
RUN chmod 700 /root/.ssh/id_rsa
RUN chown -R root:root /root/.ssh
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan github.com >> /root/.ssh/known_hosts
RUN git clone git@github.com:AmpelProject/Ampel.git /Ampel

COPY environment.yml /
RUN conda env create --file /environment.yml

RUN cd /Ampel && git pull && pip install -e .

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
    mkdir /ztf /run/secrets
USER ampel

ENV CONDA_PREFIX=/opt/conda/envs/ampel PATH=/opt/conda/envs/ampel/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

VOLUME ["/Ampel"]

ADD docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
