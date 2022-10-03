#!/bin/sh

foldername="clessn-blend/scrapers/pressconferences-crush"

if [ $1 = "quebecnationalassembly" ]
then
  echo "Scraping canadian federal parties press releases"
  scriptname="quebec_national_assembly_pressroom.R"
else
  scriptname="badbadbad"
  output="invalid parameter 1 provided to shell script" $1
  ret=1 
fi

generate_post_data()
{
  cat <<EOF
  {
    "text": "\n.\n${scriptname}\n============ start of message ============\n${status}: ${scriptname} ${output_msg} on $(date)\nEXIT CODE: ${ret}\n${output}\n============ end of message ============\n.\n.\n"
  }
EOF
}

R --no-save --no-restore -e 'install.packages("remotes", repos = "http://cran.us.r-project.org")'
R --no-save --no-restore -e 'remotes::install_github("clessn/clessnverse", ref="v1", force=T)'

cd ~

if [ $scriptname != "badbadbad" ]; then
  Rscript --no-save --no-restore $CLESSN_ROOT_DIR/$foldername/$scriptname --log_output $2 --dataframe_mode $3 --hub_mode $4 --download_data $5 --translate $6 1> "$scriptname.out"
  ret=$?
  sed 's/\"/\\"/g' -i $scriptname.out
  sed 's///g ' -i $scriptname.out
  output=`tail $scriptname.out -n 5`
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
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" https://hooks.slack.com/services/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
else
  curl -X POST -H 'Content-type: application/json' --data "{\"text\":\"${status}: ${scriptname} ${output_msg} on $(date)\n\"}" https://hooks.slack.com/services/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
fi

if [ -f "$scriptname.out" ]; then
  rm -f "$scriptname.out"
fi
