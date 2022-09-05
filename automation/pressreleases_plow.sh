#!/bin/sh

R --no-save --no-restore -e 'install.packages("remotes", repos = "http://cran.us.r-project.org")'
R --no-save --no-restore -e 'remotes::install_github("clessn/clessnverse", ref="v1", force=T)'

cd ~


if [ $1 == "canadafederalparties"  ]
then
  echo "Scraping canada federal parties press releases"
  Rscript $CLESSN_ROOT_DIR/clessn-blend/scrapers/pressreleases-plow/canada-federal-parties-pressreleases.R --log_output $2 --dataframe_mode $3 --hub_mode $4 --download_data $5 --translate $6
else
  echo "invalid parameter 1 " $1 
fi
