#!/bin/sh

R --no-save --no-restore -e 'install.packages("remotes", repos = "http://cran.us.r-project.org")'
R --no-save --no-restore -e 'remotes::install_github("clessn/clessnverse", ref="v1", force=T)'

cd ~


if [ $1 == "quebec"  ]
then
  echo "Scraping quebec national assembly"
  Rscript $CLESSN_ROOT_DIR/clessn-blend/scrapers/parliament-mash/quebec-debates.R --log_output $2 --dataframe_mode $3 --hub_mode $4 --download_data $5 --translate $6
elif [ $1 == "canada"  ]
then
  echo "Scraping canada house of commons"
  Rscript $CLESSN_ROOT_DIR/clessn-blend/scrapers/parliament-mash/canada-hansards.R --log_output $2 --dataframe_mode $3 --hub_mode $4 --download_data $5 --translate $6
elif [ $1 == "europe"  ]
then
  echo "Scraping european parliament"
  Rscript $CLESSN_ROOT_DIR/clessn-blend/scrapers/parliament-mash/europe-plenary-debates.R --log_output $2 --dataframe_mode $3 --hub_mode $4 --download_data $5 --translate $6
else
  echo "invalid parameter 1 " $1 
fi
