#!/bin/sh

. $CLESSN_ROOT_DIR/clessn-blend/automation/.env

foldername="clessn-blend/scrapers/parliament-mash"

if [ $1 = "quebec" ]
then
  echo "Scraping quebec national assembly"
  scriptname="quebec-debates"
elif [ $1 = "canada" ]
then
  echo "Scraping canada house of commons"
  scriptname="canada_hansards"
elif [ $1 = "europe" ]
then
  scriptname="europe-plenary-debates"
  echo "Scraping european parliament"
else
  scriptname="badbadbad"
  output="invalid parameter 1 provided to shell script" $1
  ret=1 
fi

generate_post_data()
{
  cat <<EOF
  {
   "text": "\n${status}: ${scriptname} ${output_msg} on $(date)\n${output}\n"
  }
EOF
}

R --no-save --no-restore -e 'install.packages("remotes", repos = "http://cran.us.r-project.org")'
R --no-save --no-restore -e 'remotes::install_github("clessn/clessnverse", ref="v1", force=T, upgrade="never")'

cd ~

if [ $scriptname != "badbadbad" ]; then
  Rscript --no-save --no-restore $CLESSN_ROOT_DIR/$foldername/$scriptname.R --log_output $2 --dataframe_mode $3 --hub_mode $4 --download_data $5 --translate $6 2>&1
  ret=$?
  sed 's/\"/\\"/g' -i ~/logs/parliament_mash"_"$1.log
  sed 's/
//g' -i ~/logs/parliament_mash"_"$1.log
  sed 's/--:--/     /g' -i ~/logs/parliament_mash"_"$1.log
  sed 's/:--/   /g' -i ~/logs/parliament_mash"_"$1.log
fi


if [ $ret -eq 0 ]; then
  status="SUCCESS"
  output_msg="completed successfully"
fi

if [ $ret -eq 1 ]; then
  status="ERROR"
  output_msg="generated one or more errors"
fi

if [ $ret -eq 2 ]; then
  status="WARNING"
  output_msg="generated one or more warnings"
fi

if [ $ret -ne 0 ]; then
  output=`tail -n 10 ~/logs/parliament_mash"_"$1.log`
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" $CLESSN_BLEND_ERRORS_WEBHOOK
else
  output=`tail -n 2 ~/logs/parliament_mash"_"$1.log`
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" $CLESSN_BLEND_WEBHOOK
fi
