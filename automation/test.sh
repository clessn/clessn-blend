#!/bin/sh

generate_post_data()
{
  cat <<EOF
  {
    "text": "\n.\n${scriptname}\n============ start of message ============\n${status}: ${scriptname} ${output_msg} on $(date)\nEXIT CODE: ${ret}\nOUTPUT:${output}\n============ end of message ============\n.\n.\n "
  }
EOF
}

scriptname="test.R"


ret=$?
sed 's/\"/\\"/g' -i $scriptname.out
output=`cat $scriptname.out`

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

echo "curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" https://hooks.slack.com/services/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"
curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" https://hooks.slack.com/services/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

#if [ -f "$scriptname.out" ]; then
  #rm -f "$scriptname.out"
#fi
