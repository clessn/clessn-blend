#!/bin/sh

/usr/local/bin/R --no-save --no-restore -e 'install.packages("remotes", repos = "http://cran.us.r-project.org")'
/usr/local/bin/R --no-save --no-restore -e 'remotes::install_github("clessn/clessnverse")'

cd ~

Rscript --no-save --no-restore $CLESSN_ROOT_DIR/clessn-blend/scrapers/twitter_blender/tweets_and_friends.R -t 3200 -o "file" -p $1
