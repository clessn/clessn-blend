#!/bin/sh

/usr/local/bin/R --no-save --no-restore -e 'install.packages("remotes", repos = "http://cran.us.r-project.org")'
/usr/local/bin/R --no-save --no-restore -e 'remotes::install_github("clessn/clessnverse", ref="v1", force=T)'

/usr/local/bin/R --no-save --no-restore -e 'remotes::install_url("https://cran.r-project.org/src/contrib/Archive/rtweet/rtweet_0.7.0.tar.gz", force=T)'

cd ~

Rscript --no-save --no-restore $CLESSN_ROOT_DIR/clessn-blend/scrapers/twitter_blender/twitter_blender.R -m $1 -o $2 -t $3 -s $4 -f $5
