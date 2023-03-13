#!/bin/sh

foldername="clessn-blend/scrapers/twitter_blender"

generate_post_data()
{
  cat <<EOF
  {
  "text": "\n${status}: ${scriptname} ${output_msg} on $(date)\n${output}\n"
  }
EOF
}

scriptname="twitter_blender"

R --no-save --no-restore -e 'remotes::install_github("clessn/clessnverse", ref="v1", force=T, upgrade="never")'

R --no-save --no-restore -e 'remotes::install_url("https://cran.r-project.org/src/contrib/Archive/rtweet/rtweet_0.7.0.tar.gz", force=T, upgrade="never")'

cd ~

Rscript --no-save --no-restore $CLESSN_ROOT_DIR/$foldername/$scriptname.R -m $1 -o $2 -t $3 -s $4 -f $5 2>&1 
ret=$?

sed 's/\"/\\"/g' -i ~/logs/$scriptname"_"$3"_"$4.log
sed 's/^M//g ' -i ~/logs/$scriptname"_"$3"_"$4.log
sed 's/--:--/     /g ' -i ~/logs/$scriptname"_"$3"_"$4.log
sed 's/:--/   /g ' -i ~/logs/$scriptname"_"$3"_"$4.log

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
  output=`tail -n 10 ~/logs/$scriptname"_"$3"_"$4.log`
  echo $output
  echo $(generate_post_data)
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" https://hooks.slack.com/services/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
else
  output=`tail -n 2 ~/logs/$scriptname"_"$3"_"$4.log`
  echo $output
  echo $(generate_post_data)
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" https://hooks.slack.com/services/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
fi
