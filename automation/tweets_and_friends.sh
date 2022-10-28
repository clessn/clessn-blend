#!/bin/sh

R --no-save --no-restore -e 'install.packages("remotes", repos = "http://cran.us.r-project.org")'
R --no-save --no-restore -e 'remotes::install_github("clessn/clessnverse", ref="v1", force=T)'

R --no-save --no-restore -e 'remotes::install_url("https://cran.r-project.org/src/contrib/Archive/rtweet/rtweet_0.7.0.tar.gz", force=T)'

cd ~

Rscript --no-save --no-restore $CLESSN_ROOT_DIR/clessn-blend/scrapers/twitter_blender/tweets_and_friends.R -t 3200 -o "file" -p $1
