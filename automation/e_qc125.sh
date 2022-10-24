#!/bin/sh

generate_post_data()
{
  cat <<EOF
  {
    "text": "\n${status}: ${scriptname} ${output_msg} on $(date)\nEXIT CODE: ${ret}\n============ tail of logs ============\n${output}\n============ end of logs ============\n "
  }
EOF
}

scriptname="e_qc125.R"

R --no-save --no-restore -e 'install.packages("remotes", repos = "http://cran.us.r-project.org")'
R --no-save --no-restore -e 'remotes::install_github("clessn/clessnverse", force=T)'

cd ~

Rscript --no-save --no-restore $CLESSN_ROOT_DIR/clessn-blend/pipelines/extractors/$scriptname 1> "$scriptname.out"

ret=$?

sed 's/\"/\\"/g' -i $scriptname.out
sed 's/^M//g ' -i $scriptname.out
output=`tail -n 5 $scriptname.out`

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

#if [ $ret -ne 0 ]; then
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" https://hooks.slack.com/services/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
#else
#  curl -X POST -H 'Content-type: application/json' --data "{\"text\":\"${status}: ${scriptname} ${output_msg} on $(date)\n\"}" https://hooks.slack.com/services/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
#fi

if [ -f "$scriptname.out" ]; then
  rm -f "$scriptname.out"
fi
