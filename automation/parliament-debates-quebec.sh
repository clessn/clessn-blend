#!/bin/sh
cd $CLESSN_ROOT_DIR/clessn-blend/Pipelines/Loaders/l_pressreleases_qc
R -e 'remotes::install_github("clessn/clessnverse", ref="v1")'
R -e 'source("$CLESSN_ROOT_DIR/clessn-blend/agoraplus-canada/R/agorapluscanada-debats.R", encoding = "UTF-8")'
