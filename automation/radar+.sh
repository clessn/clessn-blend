#!/bin/sh

basefolder="clessn-blend/pipelines"
scriptname="radar+"

if [ $1 = "extract" ];then
  function="Extractor"
  foldername="extractors"
  prefix="e_"
elif [ $1 = "load" ];then
  function="Loader"
  foldername="loaders"
  prefix="l_"
elif [ $1 = "refine" ];then
  function="Refiner"
  foldername="refiners"
  prefix="r_"
else
  function="badbadbad"
  output="invalid parameter 1 provided to shell script" $1
  ret=1
fi


echo "$function of $scriptname starting..."

generate_post_data()
{
  cat <<EOF
  {
    "text": "\n*${status}*: ${scriptname} ${output_msg} on $(date)\n${output}\n"
  }
EOF
}

R --no-save --no-restore -e 'remotes::install_github("clessn/clessnverse", force=T, upgrade="never")'

cd ~

if [ $scriptname != "badbadbad" ]; then
  shift 3
  Rscript --no-save --no-restore $CLESSN_ROOT_DIR/$basefolder/$foldername/$prefix$scriptname.R $@ 2>&1
  ret=$?
  sed 's/\"/\\"/g' -i ~/logs/$prefix$scriptname.log
  sed 's///g ' -i ~/logs/$prefix$scriptname.log
  sed 's/--:--/     /g ' -i ~/logs/$prefix$scriptname.log
  sed 's/:--/   /g ' -i ~/logs/$prefix$scriptname.log
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
  output=`tail -n 10 ~/logs/$prefix$scriptname.log`
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" https://hooks.slack.com/services/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
else
  output=`tail -n 16 ~/logs/$prefix$scriptname.log`
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" https://hooks.slack.com/services/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
fi
