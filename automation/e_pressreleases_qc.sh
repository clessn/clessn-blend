#!/bin/sh

/usr/local/bin/R --no-save --no-restore -e 'install.packages("remotes", repos = "http://cran.us.r-project.org")'
/usr/local/bin/R --no-save --no-restore -e 'remotes::install_github("clessn/clessnverse")'

cd ~

Rscript --no-save --no-restore $CLESSN_ROOT_DIR/clessn-blend/pipelines/extractors/e_pressreleases_qc.R
