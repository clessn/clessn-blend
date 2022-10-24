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


RUN useradd clessn \
    && addgroup clessn staff \
    && mkdir /home/clessn \
    && mkdir /home/clessn/dev \
    && mkdir /home/clessn/dev/clessn \
    && mkdir /home/clessn/dev/clessn/clessn-blend \
    && mkdir /home/clessn/logs 
    #\
    #&& echo "CipherString = DEFAULT@SECLEVEL=1" >> /etc/ssl/openssl.cnf 

WORKDIR /home/clessn/dev

COPY automation/docker/packages.R /home/clessn/dev
RUN Rscript /home/clessn/dev/packages.R

COPY automation/docker/more_packages.R /home/clessn/dev
RUN Rscript /home/clessn/dev/more_packages.R

COPY automation/docker/qc125_packages.R /home/clessn/dev
RUN Rscript /home/clessn/dev/qc125_packages.R

COPY automation/docker/clessnverse_packages.R /home/clessn/dev
RUN Rscript /home/clessn/dev/clessnverse_packages.R

ADD pipelines/   /home/clessn/dev/clessn/clessn-blend/pipelines
ADD scrapers/    /home/clessn/dev/clessn/clessn-blend/scrapers
ADD automation/  /home/clessn/dev/clessn/clessn-blend/automation

COPY automation/docker/.rtweet_token.rds /home/clessn/.rtweet_token.rds

RUN chown -R clessn:staff /home/clessn \
    && chown -R clessn:staff /usr/local/lib/R/site-library

RUN chmod 750 -R /home/clessn/

USER clessn

WORKDIR /home/clessn

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
