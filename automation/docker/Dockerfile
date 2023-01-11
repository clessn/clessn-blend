FROM r-base:latest

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get -y update \
    && apt-get -y install --no-install-recommends vim-tiny \
    ssh \
    git \
    curl \
    locales-all \
    build-essential \
    libssl3 \
    libssl-dev \
    libxml2 \
    libxml2-dev \
    libcurl4 \
    libcurl4-openssl-dev \
    libasound2 \
    ffmpeg

RUN cd /root \
    && curl -O https://www.python.org/ftp/python/3.9.5/Python-3.9.5.tar.xz \
    && tar -xf Python-3.9.5.tar.xz \
    && cd Python-3.9.5 \
    && ./configure --enable-optimizations \
    && make -j 4 \
    && make install

RUN pip3 install --upgrade pip

RUN python3 -m pip install --upgrade \
    pytube \
    pydub \
    azure-cognitiveservices-speech \
    dropbox \
    matplotlib

COPY automation/docker/packages_1.R /tmp
RUN Rscript /tmp/packages_1.R

COPY automation/docker/packages_2.R /tmp
RUN Rscript /tmp/packages_2.R

COPY automation/docker/packages_3.R /tmp
RUN Rscript /tmp/packages_3.R

COPY automation/docker/packages_clessnverse.R /tmp
RUN Rscript /tmp/packages_clessnverse.R


RUN useradd clessn \
    && adduser clessn staff \
    && mkdir /home/clessn \
    && mkdir /home/clessn/dev \
    && mkdir /home/clessn/dev/clessn \
    && mkdir /home/clessn/dev/clessn/clessn-blend \
    && mkdir /home/clessn/logs \
    && chmod 777 /home/clessn/logs
    #\
    #&& echo "CipherString = DEFAULT@SECLEVEL=1" >> /etc/ssl/openssl.cnf 

WORKDIR /home/clessn/dev

ADD pipelines/   /home/clessn/dev/clessn/clessn-blend/pipelines
ADD scrapers/    /home/clessn/dev/clessn/clessn-blend/scrapers
ADD automation/  /home/clessn/dev/clessn/clessn-blend/automation

RUN chown -R clessn:staff /home/clessn \
    && chown -R clessn:staff /usr/local/lib/R/site-library

COPY automation/docker/.rtweet_token.rds /home/clessn/.rtweet_token.rds

RUN chmod 750 -R /home/clessn/

USER clessn

WORKDIR /home/clessn

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
