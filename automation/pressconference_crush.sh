#!/bin/sh

R --no-save --no-restore -e 'install.packages("remotes", repos = "http://cran.us.r-project.org")'
R --no-save --no-restore -e 'remotes::install_github("clessn/clessnverse", ref="v1", force=T)'

cd ~


if [ $1 == "quebecnationalassembly"  ]
then
  echo "Scraping quebec national assembly press conference"
  Rscript $CLESSN_ROOT_DIR/clessn-blend/scrapers/pressconferences-crush/quebec-national-assembly-pressroom.R --log_output $2 --dataframe_mode $3 --hub_mode $4 --download_data $5 --translate $6
else
  echo "invalid parameter 1 " $1 
fi
