FROM r-base:4.1.3

# Install python
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
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
RUN Rscript -e 'install.packages("renv")'

COPY renv.lock .
RUN Rscript -e 'renv::restore()'
# copy content
COPY . .

RUN chmod -R 775 .
RUN chown -R 1000:root .
USER 1000

# run server
CMD ["python3", "./pipeline/server.py"]
#CMD ["Rscript", "main.R"]
