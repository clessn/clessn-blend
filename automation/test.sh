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

scriptname="test.R"
echo "testing new webhooks" > $scriptname.out

#ret=$?
ret=2
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

if [ $ret -eq 0 ]; then
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" $CLESSN_BLEND_WEBHOOK
fi

if [ $ret -eq 1 ]; then
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" $CLESSN_BLEND_ERRORS_WEBHOOK
fi

if [ $ret -eq 2 ]; then
  curl -X POST -H 'Content-type: application/json' --data "$(generate_post_data)" $CLESSN_BLEND_WARNINGS_WEBHOOK
fi

if [ -f "$scriptname.out" ]; then
  rm -f "$scriptname.out"
fi
