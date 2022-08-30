#!/bin/sh
cd $CLESSN_ROOT_DIR/clessn-blend/agoraplus-canada/R 
R -e 'remotes::install_github("clessn/clessnverse", ref="v1")'
Rscript agorapluscanada-debats.R --dataframe_mode "rebuild" --hub_mode "update" --log_output "file,console,hub" --download_data FALSE --translate=TRUE
