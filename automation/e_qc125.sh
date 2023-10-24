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

scriptname="e_qc125.R"

R --no-save --no-restore -e 'install.packages("remotes", repos = "http://cran.us.r-project.org")'
R --no-save --no-restore -e 'remotes::install_github("clessn/clessnverse", force=T, upgrade="never")'

cd ~

Rscript --no-save --no-restore $CLESSN_ROOT_DIR/clessn-blend/pipelines/extractors/$scriptname 2>&1 | tee "$scriptname.out"

ret=$?

sed 's/\"/\\"/g' -i $scriptname.out
sed 's/^M//g' -i $scriptname.out

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
  output=`tail -n 10 $scriptname.out`
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" $CLESSN_BLEND_ERRORS_WEBHOOK
else
  output=`tail -n 2 $scriptname.out`
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" $CLESSN_BLEND_WEBHOOK
fi


if [ -f "$scriptname.out" ]; then
  rm -f "$scriptname.out"
fi
