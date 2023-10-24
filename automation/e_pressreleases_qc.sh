#!/bin/sh

. $CLESSN_ROOT_DIR/clessn-blend/automation/.env

generate_post_data()
{
  cat <<EOF
  {
   "text": "\n${status}: ${scriptname} ${output_msg} on $(date)\n${output}\n"
  }
EOF
}

scriptname="e_pressreleases_qc"

R --no-save --no-restore -e 'install.packages("remotes", repos = "http://cran.us.r-project.org")'
R --no-save --no-restore -e 'remotes::install_github("clessn/clessnverse", force=T, upgrade="never")'

cd ~

Rscript --no-save --no-restore $CLESSN_ROOT_DIR/clessn-blend/pipelines/extractors/$scriptname.R 2>&1
ret=$?

sed 's/\"/\\"/g' -i ~/logs/$scriptname.log
sed 's/^M//g' -i ~/logs/$scriptname.log

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
  output=`tail -n 10 ~/logs/$scriptname.log`
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" $CLESSN_BLEND_ERRORS_WEBHOOK
else
  output=`tail -n 7 ~/logs/$scriptname.log`
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" $CLESSN_BLEND_WEBHOOK
fi
