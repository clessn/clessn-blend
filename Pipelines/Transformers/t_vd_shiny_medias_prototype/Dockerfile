FROM r-base:4.1.3

# Install python
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    libgit2-dev \
    python3 \
    python3-pip \
    python3-setuptools \
    python3-dev
RUN pip3 install --upgrade pip

WORKDIR /usr/local/src
# setup poetry
RUN apt-get install -y \
    && pip3 install "poetry~=1.1.13" \
    && poetry config virtualenvs.create false \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists
EXPOSE 8080
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-dev;

# setup renv
COPY renv.lock .
RUN Rscript -e 'install.packages("renv")'
RUN Rscript -e 'renv::restore()'
RUN mv /usr/local/lib/R/site-library/* /usr/lib/R/library/

# copy all data and switch user
COPY . .
RUN chmod -R 775 .
RUN chown -R 1000:root .
USER 1000

# run server
CMD ["python3", "./pipeline/server.py"]
