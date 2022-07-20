###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                        l_globales_canada                                    #
#                                                                             #
#  This loader merges the CES from 1965 to 2019 together and creates          #
#  merged variables when it is appropriate.                                   #
#                                                                             #
###############################################################################


###############################################################################
########################           Functions            ######################
###############################################################################

# Function to prepare data of GlobalES data before loading it in the warehouse
#
# Return a dataframe ready to be loaded in the warehouse
merge_all_ces <- function(list_tables) {
  # Ajouter un identifiant unique à chaque observation (unique_id - ou autre)
  # Si autre que unique_id, modifier la fonction upload_warehouse_globales
  input_df <- Reduce(function(...) merge(..., all=TRUE),
                         list_tables)
  # Death penalty
  input_df$issDeathALL <- NA
  input_df$issDeathALL[input_df$issDeath00 == 0 | input_df$issDeath04 == 0 |
                             input_df$issDeath06 == 0 |
                             input_df$issDeath08 == 0 | input_df$issDeath11 == 0 |
                             input_df$issDeath15 == 0 | input_df$issDeath84 == 0 |
                             input_df$issDeath88 == 0 | input_df$issDeath93 == 0 |
                             input_df$issDeath97 == 0] <- 0 # Against Death Penalty
  input_df$issDeathALL[input_df$issDeath00 >= 0.5 | input_df$issDeath04 >= 0.5 |
                             input_df$issDeath06 >= 0.5 |
                             input_df$issDeath08 >= 0.5 | input_df$issDeath11 >= 0.5 |
                             input_df$issDeath15 >= 0.5 | input_df$issDeath84 >= 0.5 |
                             input_df$issDeath88 >= 0.5 | input_df$issDeath93 >= 0.5 |
                             input_df$issDeath97 >= 0.5] <- 1  # In Favour of Death Penalty
  #### 2.1 Political variables  ####
  #### 2.1.1. ppFeel ####
  #### A. Liberal ####
  input_df$ppFeelLibALL      <- input_df$ppFeelLib65
  input_df$ppFeelLibALL[is.na(input_df$ppFeelLibALL)] <- input_df$ppFeelLib68[is.na(input_df$ppFeelLibALL)]
  input_df$ppFeelLibALL[is.na(input_df$ppFeelLibALL)] <- input_df$ppFeelLib74[is.na(input_df$ppFeelLibALL)]
  input_df$ppFeelLibALL[is.na(input_df$ppFeelLibALL)] <- input_df$ppFeelLib79[is.na(input_df$ppFeelLibALL)]
  input_df$ppFeelLibALL[is.na(input_df$ppFeelLibALL)] <- input_df$ppFeelLib84[is.na(input_df$ppFeelLibALL)]
  input_df$ppFeelLibALL[is.na(input_df$ppFeelLibALL)] <- input_df$ppFeelLib88[is.na(input_df$ppFeelLibALL)]
  input_df$ppFeelLibALL[is.na(input_df$ppFeelLibALL)] <- input_df$ppFeelLib93[is.na(input_df$ppFeelLibALL)]
  input_df$ppFeelLibALL[is.na(input_df$ppFeelLibALL)] <- input_df$ppFeelLib97[is.na(input_df$ppFeelLibALL)]
  input_df$ppFeelLibALL[is.na(input_df$ppFeelLibALL)] <- input_df$ppFeelLib00[is.na(input_df$ppFeelLibALL)]
  input_df$ppFeelLibALL[is.na(input_df$ppFeelLibALL)] <- input_df$ppFeelLib04[is.na(input_df$ppFeelLibALL)]
  input_df$ppFeelLibALL[is.na(input_df$ppFeelLibALL)] <- input_df$ppFeelLib06[is.na(input_df$ppFeelLibALL)]
  input_df$ppFeelLibALL[is.na(input_df$ppFeelLibALL)] <- input_df$ppFeelLib08[is.na(input_df$ppFeelLibALL)]
  input_df$ppFeelLibALL[is.na(input_df$ppFeelLibALL)] <- input_df$ppFeelLib11[is.na(input_df$ppFeelLibALL)]
  input_df$ppFeelLibALL[is.na(input_df$ppFeelLibALL)] <- input_df$ppFeelLib15[is.na(input_df$ppFeelLibALL)]
  input_df$ppFeelLibALL[is.na(input_df$ppFeelLibALL)] <- input_df$ppFeelLib19[is.na(input_df$ppFeelLibALL)]


  #### B. NDP ####

  input_df$ppFeelNdpALL      <- input_df$ppFeelNdp65
  input_df$ppFeelNdpALL[is.na(input_df$ppFeelNdpALL)] <- input_df$ppFeelNdp68[is.na(input_df$ppFeelNdpALL)]
  input_df$ppFeelNdpALL[is.na(input_df$ppFeelNdpALL)] <- input_df$ppFeelNdp74[is.na(input_df$ppFeelNdpALL)]
  input_df$ppFeelNdpALL[is.na(input_df$ppFeelNdpALL)] <- input_df$ppFeelNdp79[is.na(input_df$ppFeelNdpALL)]
  input_df$ppFeelNdpALL[is.na(input_df$ppFeelNdpALL)] <- input_df$ppFeelNdp84[is.na(input_df$ppFeelNdpALL)]
  input_df$ppFeelNdpALL[is.na(input_df$ppFeelNdpALL)] <- input_df$ppFeelNdp88[is.na(input_df$ppFeelNdpALL)]
  input_df$ppFeelNdpALL[is.na(input_df$ppFeelNdpALL)] <- input_df$ppFeelNdp93[is.na(input_df$ppFeelNdpALL)]
  input_df$ppFeelNdpALL[is.na(input_df$ppFeelNdpALL)] <- input_df$ppFeelNdp97[is.na(input_df$ppFeelNdpALL)]
  input_df$ppFeelNdpALL[is.na(input_df$ppFeelNdpALL)] <- input_df$ppFeelNdp00[is.na(input_df$ppFeelNdpALL)]
  input_df$ppFeelNdpALL[is.na(input_df$ppFeelNdpALL)] <- input_df$ppFeelNdp04[is.na(input_df$ppFeelNdpALL)]
  input_df$ppFeelNdpALL[is.na(input_df$ppFeelNdpALL)] <- input_df$ppFeelNdp06[is.na(input_df$ppFeelNdpALL)]
  input_df$ppFeelNdpALL[is.na(input_df$ppFeelNdpALL)] <- input_df$ppFeelNdp08[is.na(input_df$ppFeelNdpALL)]
  input_df$ppFeelNdpALL[is.na(input_df$ppFeelNdpALL)] <- input_df$ppFeelNdp11[is.na(input_df$ppFeelNdpALL)]
  input_df$ppFeelNdpALL[is.na(input_df$ppFeelNdpALL)] <- input_df$ppFeelNdp15[is.na(input_df$ppFeelNdpALL)]
  input_df$ppFeelNdpALL[is.na(input_df$ppFeelNdpALL)] <- input_df$ppFeelNdp19[is.na(input_df$ppFeelNdpALL)]


  #### C. Bloc ####

  input_df$ppFeelBlocALL  <- input_df$ppFeelBloc93
  input_df$ppFeelBlocALL[is.na(input_df$ppFeelBlocALL)] <- input_df$ppFeelBloc97[is.na(input_df$ppFeelBlocALL)]
  input_df$ppFeelBlocALL[is.na(input_df$ppFeelBlocALL)] <- input_df$ppFeelBloc00[is.na(input_df$ppFeelBlocALL)]
  input_df$ppFeelBlocALL[is.na(input_df$ppFeelBlocALL)] <- input_df$ppFeelBloc04[is.na(input_df$ppFeelBlocALL)]
  input_df$ppFeelBlocALL[is.na(input_df$ppFeelBlocALL)] <- input_df$ppFeelBloc08[is.na(input_df$ppFeelBlocALL)]
  input_df$ppFeelBlocALL[is.na(input_df$ppFeelBlocALL)] <- input_df$ppFeelBloc11[is.na(input_df$ppFeelBlocALL)]
  input_df$ppFeelBlocALL[is.na(input_df$ppFeelBlocALL)] <- input_df$ppFeelBloc15[is.na(input_df$ppFeelBlocALL)]
  input_df$ppFeelBlocALL[is.na(input_df$ppFeelBlocALL)] <- input_df$ppFeelBloc19[is.na(input_df$ppFeelBlocALL)]


  #### D. Conservative ####

  input_df$ppFeelConsALL      <- input_df$ppFeelPCons65
  input_df$ppFeelConsALL[is.na(input_df$ppFeelConsALL)] <- input_df$ppFeelPCons68[is.na(input_df$ppFeelConsALL)]
  input_df$ppFeelConsALL[is.na(input_df$ppFeelConsALL)] <- input_df$ppFeelPCons74[is.na(input_df$ppFeelConsALL)]
  input_df$ppFeelConsALL[is.na(input_df$ppFeelConsALL)] <- input_df$ppFeelPCons79[is.na(input_df$ppFeelConsALL)]
  input_df$ppFeelConsALL[is.na(input_df$ppFeelConsALL)] <- input_df$ppFeelPCons84[is.na(input_df$ppFeelConsALL)]
  input_df$ppFeelConsALL[is.na(input_df$ppFeelConsALL)] <- input_df$ppFeelPCons88[is.na(input_df$ppFeelConsALL)]
  input_df$ppFeelConsALL[is.na(input_df$ppFeelConsALL)] <- input_df$ppFeelCons93_MergePC[is.na(input_df$ppFeelConsALL)]
  input_df$ppFeelConsALL[is.na(input_df$ppFeelConsALL)] <- input_df$ppFeelCons97_MergePC[is.na(input_df$ppFeelConsALL)]
  input_df$ppFeelConsALL[is.na(input_df$ppFeelConsALL)] <- input_df$ppFeelCons00_MergePC[is.na(input_df$ppFeelConsALL)]
  input_df$ppFeelConsALL[is.na(input_df$ppFeelConsALL)] <- input_df$ppFeelCons04[is.na(input_df$ppFeelConsALL)]
  input_df$ppFeelConsALL[is.na(input_df$ppFeelConsALL)] <- input_df$ppFeelCons06[is.na(input_df$ppFeelConsALL)]
  input_df$ppFeelConsALL[is.na(input_df$ppFeelConsALL)] <- input_df$ppFeelCons08[is.na(input_df$ppFeelConsALL)]
  input_df$ppFeelConsALL[is.na(input_df$ppFeelConsALL)] <- input_df$ppFeelCons11[is.na(input_df$ppFeelConsALL)]
  input_df$ppFeelConsALL[is.na(input_df$ppFeelConsALL)] <- input_df$ppFeelCons15[is.na(input_df$ppFeelConsALL)]
  input_df$ppFeelConsALL[is.na(input_df$ppFeelConsALL)] <- input_df$ppFeelCons19[is.na(input_df$ppFeelConsALL)]


  #### 2.1.2. leadFeel ####

  #### A. Liberal ####

  input_df$leadFeelLibALL      <- input_df$leadFeelLibpre65
  input_df$leadFeelLibALL[is.na(input_df$leadFeelLibALL)] <- input_df$leadFeelLibpre68[is.na(input_df$leadFeelLibALL)]
  input_df$leadFeelLibALL[is.na(input_df$leadFeelLibALL)] <- input_df$leadFeelLibpre74[is.na(input_df$leadFeelLibALL)]
  input_df$leadFeelLibALL[is.na(input_df$leadFeelLibALL)] <- input_df$leadFeelLibpre79[is.na(input_df$leadFeelLibALL)]
  input_df$leadFeelLibALL[is.na(input_df$leadFeelLibALL)] <- input_df$leadFeelLibpre84[is.na(input_df$leadFeelLibALL)]
  input_df$leadFeelLibALL[is.na(input_df$leadFeelLibALL)] <- input_df$leadFeelLibpre88[is.na(input_df$leadFeelLibALL)]
  input_df$leadFeelLibALL[is.na(input_df$leadFeelLibALL)] <- input_df$leadFeelLibpre93[is.na(input_df$leadFeelLibALL)]
  input_df$leadFeelLibALL[is.na(input_df$leadFeelLibALL)] <- input_df$leadFeelLibpre97[is.na(input_df$leadFeelLibALL)]
  input_df$leadFeelLibALL[is.na(input_df$leadFeelLibALL)] <- input_df$leadFeelLibpre00[is.na(input_df$leadFeelLibALL)]
  input_df$leadFeelLibALL[is.na(input_df$leadFeelLibALL)] <- input_df$leadFeelLibpre04[is.na(input_df$leadFeelLibALL)]
  input_df$leadFeelLibALL[is.na(input_df$leadFeelLibALL)] <- input_df$leadFeelLibpre06[is.na(input_df$leadFeelLibALL)]
  input_df$leadFeelLibALL[is.na(input_df$leadFeelLibALL)] <- input_df$leadFeelLibpre08[is.na(input_df$leadFeelLibALL)]
  input_df$leadFeelLibALL[is.na(input_df$leadFeelLibALL)] <- input_df$leadFeelLibpre11[is.na(input_df$leadFeelLibALL)]
  input_df$leadFeelLibALL[is.na(input_df$leadFeelLibALL)] <- input_df$leadFeelLib15[is.na(input_df$leadFeelLibALL)]
  input_df$leadFeelLibALL[is.na(input_df$leadFeelLibALL)] <- input_df$leadFeelLib19[is.na(input_df$leadFeelLibALL)]


  #### B. Conservative ####

  input_df$leadFeelConsALL      <- input_df$leadFeelPConspre65
  input_df$leadFeelConsALL[is.na(input_df$leadFeelConsALL)] <- input_df$leadFeelPConspre68[is.na(input_df$leadFeelConsALL)]
  input_df$leadFeelConsALL[is.na(input_df$leadFeelConsALL)] <- input_df$leadFeelPConspre74[is.na(input_df$leadFeelConsALL)]
  input_df$leadFeelConsALL[is.na(input_df$leadFeelConsALL)] <- input_df$leadFeelPConspre79[is.na(input_df$leadFeelConsALL)]
  input_df$leadFeelConsALL[is.na(input_df$leadFeelConsALL)] <- input_df$leadFeelPConspre84[is.na(input_df$leadFeelConsALL)]
  input_df$leadFeelConsALL[is.na(input_df$leadFeelConsALL)] <- input_df$leadFeelPConspre88[is.na(input_df$leadFeelConsALL)]
  input_df$leadFeelConsALL[is.na(input_df$leadFeelConsALL)] <- input_df$leadFeelConspre93_MergePC[is.na(input_df$leadFeelConsALL)]
  input_df$leadFeelConsALL[is.na(input_df$leadFeelConsALL)] <- input_df$leadFeelConspre97_MergePC[is.na(input_df$leadFeelConsALL)]
  input_df$leadFeelConsALL[is.na(input_df$leadFeelConsALL)] <- input_df$leadFeelConspre00_MergePC[is.na(input_df$leadFeelConsALL)]
  input_df$leadFeelConsALL[is.na(input_df$leadFeelConsALL)] <- input_df$leadFeelConspre04[is.na(input_df$leadFeelConsALL)]
  input_df$leadFeelConsALL[is.na(input_df$leadFeelConsALL)] <- input_df$leadFeelConspre06[is.na(input_df$leadFeelConsALL)]
  input_df$leadFeelConsALL[is.na(input_df$leadFeelConsALL)] <- input_df$leadFeelConspre08[is.na(input_df$leadFeelConsALL)]
  input_df$leadFeelConsALL[is.na(input_df$leadFeelConsALL)] <- input_df$leadFeelConspre11[is.na(input_df$leadFeelConsALL)]
  input_df$leadFeelConsALL[is.na(input_df$leadFeelConsALL)] <- input_df$leadFeelCons15[is.na(input_df$leadFeelConsALL)]
  input_df$leadFeelConsALL[is.na(input_df$leadFeelConsALL)] <- input_df$leadFeelCons19[is.na(input_df$leadFeelConsALL)]


  #### C. BLOC ####

  input_df$leadFeelNdpALL      <- input_df$leadFeelNdppre65
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre68[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre74[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre79[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre84[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre88[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre93[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre97[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre00[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre04[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre06[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre08[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre11[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdp15[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdp19[is.na(input_df$leadFeelNdpALL)]


  input_df$leadFeelBlocALL      <- input_df$leadFeelBlocpre93
  input_df$leadFeelBlocALL[is.na(input_df$leadFeelBlocALL)] <- input_df$leadFeelBlocpre97[is.na(input_df$leadFeelBlocALL)]
  input_df$leadFeelBlocALL[is.na(input_df$leadFeelBlocALL)] <- input_df$leadFeelBlocpre00[is.na(input_df$leadFeelBlocALL)]
  input_df$leadFeelBlocALL[is.na(input_df$leadFeelBlocALL)] <- input_df$leadFeelBlocpre04[is.na(input_df$leadFeelBlocALL)]
  input_df$leadFeelBlocALL[is.na(input_df$leadFeelBlocALL)] <- input_df$leadFeelBlocpre06[is.na(input_df$leadFeelBlocALL)]
  input_df$leadFeelBlocALL[is.na(input_df$leadFeelBlocALL)] <- input_df$leadFeelBlocpre08[is.na(input_df$leadFeelBlocALL)]
  input_df$leadFeelBlocALL[is.na(input_df$leadFeelBlocALL)] <- input_df$leadFeelBlocpre11[is.na(input_df$leadFeelBlocALL)]
  input_df$leadFeelBlocALL[is.na(input_df$leadFeelBlocALL)] <- input_df$leadFeelBloc15[is.na(input_df$leadFeelBlocALL)]
  input_df$leadFeelBlocALL[is.na(input_df$leadFeelBlocALL)] <- input_df$leadFeelBloc19[is.na(input_df$leadFeelBlocALL)]


  #### D. NDP ####

  input_df$leadFeelNdpALL      <- input_df$leadFeelNdppre65
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre68[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre74[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre79[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre84[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre88[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre93[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre97[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre00[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre04[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre06[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre08[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdppre11[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdp15[is.na(input_df$leadFeelNdpALL)]
  input_df$leadFeelNdpALL[is.na(input_df$leadFeelNdpALL)] <- input_df$leadFeelNdp19[is.na(input_df$leadFeelNdpALL)]


  # Green party
  input_df$leadFeelGrnALL      <- input_df$leadFeelGrnpre08
  input_df$leadFeelGrnALL[is.na(input_df$leadFeelGrnpALL)] <- input_df$leadFeelGrnpre11[is.na(input_df$leadFeelGrnALL)]
  input_df$leadFeelGrnALL[is.na(input_df$leadFeelGrnpALL)] <- input_df$leadFeelGrn15[is.na(input_df$leadFeelGrnALL)]
  input_df$leadFeelGrnALL[is.na(input_df$leadFeelGrnpALL)] <- input_df$leadFeelGrn19[is.na(input_df$leadFeelGrnALL)]


  # Reform

  input_df$leadFeelReformALL      <- input_df$leadFeelReformpre93
  input_df$leadFeelReformALL[is.na(input_df$leadFeelReformALL)] <- input_df$leadFeelReformpre97[is.na(input_df$leadFeelReformALL)]

  # Alliance
  input_df$leadFeelAllianceALL      <- input_df$leadFeelAlliancepre00

  # P-Cons
  input_df$leadFeelPConsALL      <- input_df$leadFeelPConspre68
  input_df$leadFeelPConsALL[is.na(input_df$leadFeelPConsALL)] <- input_df$leadFeelPConspre74[is.na(input_df$leadFeelPConsALL)]
  input_df$leadFeelPConsALL[is.na(input_df$leadFeelPConsALL)] <- input_df$leadFeelPConspre79[is.na(input_df$leadFeelPConsALL)]
  input_df$leadFeelPConsALL[is.na(input_df$leadFeelPConsALL)] <- input_df$leadFeelPConspre84[is.na(input_df$leadFeelPConsALL)]
  input_df$leadFeelPConsALL[is.na(input_df$leadFeelPConsALL)] <- input_df$leadFeelPConspre88[is.na(input_df$leadFeelPConsALL)]
  input_df$leadFeelPConsALL[is.na(input_df$leadFeelPConsALL)] <- input_df$leadFeelPConspre97[is.na(input_df$leadFeelPConsALL)]
  input_df$leadFeelPConsALL[is.na(input_df$leadFeelPConsALL)] <- input_df$leadFeelPConspre93[is.na(input_df$leadFeelPConsALL)]
  input_df$leadFeelPConsALL[is.na(input_df$leadFeelPConsALL)] <- input_df$leadFeelPConspre00[is.na(input_df$leadFeelPConsALL)]

  # New cons
  input_df$leadFeelConsNewALL      <- input_df$leadFeelConspre04
  input_df$leadFeelConsNewALL[is.na(input_df$leadFeelConsNewALL)] <- input_df$leadFeelConspre06[is.na(input_df$leadFeelConsNewALL)]
  input_df$leadFeelConsNewALL[is.na(input_df$leadFeelConsNewALL)] <- input_df$leadFeelConspre08[is.na(input_df$leadFeelConsNewALL)]
  input_df$leadFeelConsNewALL[is.na(input_df$leadFeelConsNewALL)] <- input_df$leadFeelConspre11[is.na(input_df$leadFeelConsNewALL)]
  input_df$leadFeelConsNewALL[is.na(input_df$leadFeelConsNewALL)] <- input_df$leadFeelCons15[is.na(input_df$leadFeelConsNewALL)]
  input_df$leadFeelConsNewALL[is.na(input_df$leadFeelConsNewALL)] <- input_df$leadFeelCons19[is.na(input_df$leadFeelConsNewALL)]


  #### 2.1.3. Idpre ####

  #### A. Liberal ####
  input_df$libIdpreALL <- NA
  input_df$libIdpreALL[input_df$libIdpre65==1 | input_df$libIdpre68==1 | input_df$libIdpre74==1 | input_df$libIdpre79==1 | input_df$libIdpre84==1 |
                             input_df$libIdpre88==1 | input_df$libIdpre93==1 | input_df$libIdpre97==1 | input_df$libIdpre00==1 | input_df$libIdpre04==1 |
                             input_df$libIdpre06==1 | input_df$libIdpre08==1 | input_df$libIdpre11==1 | input_df$libIdpre15==1 | input_df$libIdpre19==1] <- 1
  input_df$libIdpreALL[input_df$libIdpre65!=1 | input_df$libIdpre68!=1 | input_df$libIdpre74!=1 | input_df$libIdpre79!=1 | input_df$libIdpre84!=1 |
                             input_df$libIdpre88!=1 | input_df$libIdpre93!=1 | input_df$libIdpre97!=1 | input_df$libIdpre00!=1 | input_df$libIdpre04!=1 |
                             input_df$libIdpre06!=1 | input_df$libIdpre08!=1 | input_df$libIdpre11!=1 | input_df$libIdpre15!=1 | input_df$libIdpre19!=1] <- 0

  #### B. Conservative ####
  input_df$consIdpreALL <- NA
  input_df$consIdpreALL[input_df$pConsIdpre65==1 | input_df$pConsIdpre68==1 | input_df$pConsIdpre74==1 | input_df$pConsIdpre79==1 |
                              input_df$pConsIdpre84==1 | input_df$pConsIdpre88==1 | input_df$consIdpre93_MergePC==1 | input_df$consIdpre97_MergePC==1 |
                              input_df$consIdpre00_MergePC==1 | input_df$consIdpre04==1 | input_df$consIdpre06==1 | input_df$consIdpre08==1 |
                              input_df$consIdpre11==1 | input_df$consIdpre15==1 | input_df$consIdpre19==1] <- 1
  input_df$consIdpreALL[input_df$pConsIdpre65!=1 | input_df$pConsIdpre68!=1 | input_df$pConsIdpre74!=1 | input_df$pConsIdpre79!=1 |
                              input_df$pConsIdpre84!=1 | input_df$pConsIdpre88!=1 | input_df$consIdpre93_MergePC!=1 | input_df$consIdpre97_MergePC!=1 |
                              input_df$consIdpre00_MergePC!=1 | input_df$consIdpre04!=1 | input_df$consIdpre06!=1 | input_df$consIdpre08!=1 |
                              input_df$consIdpre11!=1 | input_df$consIdpre15!=1 | input_df$consIdpre19!=1] <- 0


  #### C. NPD ####
  input_df$ndpIdpreALL <- NA
  input_df$ndpIdpreALL[input_df$ndpIdpre65==1 | input_df$ndpIdpre68==1 | input_df$ndpIdpre74==1 | input_df$ndpIdpre79==1 |
                             input_df$ndpIdpre84==1 | input_df$ndpIdpre88==1 | input_df$ndpIdpre93==1 | input_df$ndpIdpre97==1 |
                             input_df$ndpIdpre00==1 | input_df$ndpIdpre04==1 | input_df$ndpIdpre06==1 | input_df$ndpIdpre08==1 |
                             input_df$ndpIdpre11==1 | input_df$ndpIdpre15==1 | input_df$ndpIdpre19==1] <- 1
  input_df$ndpIdpreALL[input_df$ndpIdpre65!=1 | input_df$ndpIdpre68!=1 | input_df$ndpIdpre74!=1 | input_df$ndpIdpre79!=1 |
                             input_df$ndpIdpre84!=1 | input_df$ndpIdpre88!=1 | input_df$ndpIdpre93!=1 | input_df$ndpIdpre97!=1 |
                             input_df$ndpIdpre00!=1 | input_df$ndpIdpre04!=1 | input_df$ndpIdpre06!=1 | input_df$ndpIdpre08!=1 |
                             input_df$ndpIdpre11!=1 | input_df$ndpIdpre15!=1 | input_df$ndpIdpre19!=1] <- 0


  #### D. Bloc ####
  input_df$blocIdpreALL <- NA
  input_df$blocIdpreALL[input_df$blocIdpre93==1 | input_df$blocIdpre97==1 |
                              input_df$blocIdpre00==1 | input_df$blocIdpre04==1 | input_df$blocIdpre06==1 |
                              input_df$blocIdpre08==1 | input_df$blocIdpre11==1 | input_df$blocIdpre15==1 |
                              input_df$blocIdpre19==1] <- 1
  input_df$blocIdpreALL[input_df$blocIdpre93!=1 | input_df$blocIdpre97!=1 |
                              input_df$blocIdpre00!=1 | input_df$blocIdpre04!=1 | input_df$blocIdpre06!=1 |
                              input_df$blocIdpre08!=1 | input_df$blocIdpre11!=1 | input_df$blocIdpre15!=1 |
                              input_df$blocIdpre19!=1] <- 0

  #### E. Reform/Alliance ####
  input_df$refAllianceIdpreALL <- NA
  input_df$refAllianceIdpreALL[input_df$reformIdpre93==1 | input_df$reformIdpre97==1 | input_df$allianceIdpre00==1] <- 1
  input_df$refAllianceIdpreALL[input_df$reformIdpre93!=1 | input_df$reformIdpre97!=1 | input_df$allianceIdpre00!=1] <- 0

  #### F. Green ####
  #input_df$grnIdpreALL <- NA
  #input_df$grnIdpreALL[input_df$grnIdpre06==1 | input_df$grnIdpre08==1 | input_df$grnIdpre11==1 | input_df$grnIdpre15==1] <- 1
  #input_df$grnIdpreALL[input_df$grnIdpre06!=1 | input_df$grnIdpre08!=1 | input_df$grnIdpre11!=1 | input_df$grnIdpre15!=1] <- 0

  #### G. No ID ####
  input_df$noIdpreALL <- NA
  input_df$noIdpreALL[input_df$noIdpre65==1 | input_df$noIdpre68==1 | input_df$noIdpre74==1 | input_df$noIdpre79==1 |
                            input_df$noIdpre84==1 | input_df$noIdpre88==1 | input_df$noIdpre93==1 | input_df$noIdpre97==1 |
                            input_df$noIdpre00==1 | input_df$noIdpre04==1 | input_df$noIdpre06==1 | input_df$noIdpre08==1 |
                            input_df$noIdpre11==1 | input_df$noIdpre15==1] <- 1
  input_df$noIdpreALL[input_df$noIdpre65!=1 | input_df$noIdpre68!=1 | input_df$noIdpre74!=1 | input_df$noIdpre79!=1 |
                            input_df$noIdpre84!=1 | input_df$noIdpre88!=1 | input_df$noIdpre93!=1 | input_df$noIdpre97!=1 |
                            input_df$noIdpre00!=1 | input_df$noIdpre04!=1 | input_df$noIdpre06!=1 | input_df$noIdpre08!=1 |
                            input_df$noIdpre11!=1 | input_df$noIdpre15!=1] <- 0


  input_df$grnIdpreALL <- input_df$grnIdpre06
  input_df$grnIdpreALL[is.na(input_df$grnIdpreALL)] <- input_df$grnIdpre11[is.na(input_df$grnIdpreALL)]
  input_df$grnIdpreALL[is.na(input_df$grnIdpreALL)] <- input_df$grnIdpre15[is.na(input_df$grnIdpreALL)]
  input_df$grnIdpreALL[is.na(input_df$grnIdpreALL)] <- input_df$grnIdpre19[is.na(input_df$grnIdpreALL)]


  #### H. Strength ID ####

  input_df$strengthIdpreALL <- input_df$strengthIdpre65
  input_df$strengthIdpreALL[is.na(input_df$strengthIdpreALL)] <- input_df$strengthIdpre68[is.na(input_df$strengthIdpreALL)]
  input_df$strengthIdpreALL[is.na(input_df$strengthIdpreALL)] <- input_df$strengthIdpre74[is.na(input_df$strengthIdpreALL)]
  input_df$strengthIdpreALL[is.na(input_df$strengthIdpreALL)] <- input_df$strengthIdpre79[is.na(input_df$strengthIdpreALL)]
  input_df$strengthIdpreALL[is.na(input_df$strengthIdpreALL)] <- input_df$strengthIdpre84[is.na(input_df$strengthIdpreALL)]
  input_df$strengthIdpreALL[is.na(input_df$strengthIdpreALL)] <- input_df$strengthIdpre88[is.na(input_df$strengthIdpreALL)]
  input_df$strengthIdpreALL[is.na(input_df$strengthIdpreALL)] <- input_df$strengthIdpre93[is.na(input_df$strengthIdpreALL)]
  input_df$strengthIdpreALL[is.na(input_df$strengthIdpreALL)] <- input_df$strengthIdpre97[is.na(input_df$strengthIdpreALL)]
  input_df$strengthIdpreALL[is.na(input_df$strengthIdpreALL)] <- input_df$strengthIdpre00[is.na(input_df$strengthIdpreALL)]
  input_df$strengthIdpreALL[is.na(input_df$strengthIdpreALL)] <- input_df$strengthIdpre11[is.na(input_df$strengthIdpreALL)]
  input_df$strengthIdpreALL[is.na(input_df$strengthIdpreALL)] <- input_df$strengthIdpre15[is.na(input_df$strengthIdpreALL)]
  input_df$strengthIdpreALL[is.na(input_df$strengthIdpreALL)] <- input_df$strengthIdpre19[is.na(input_df$strengthIdpreALL)]


  #### 2.1.4. voted ####

  #### X. Not voted ####
  input_df$votedNotALL <- NA
  input_df$votedNotALL[input_df$votedNot65==1 | input_df$votedNot68==1 | input_df$votedNot74==1 |input_df$votedNot79==1 |
                             input_df$votedNot84==1 | input_df$votedNot88==1 | input_df$votedNot93==1 |input_df$votedNot97==1 |
                             input_df$votedNot00==1 | input_df$votedNot04==1 | input_df$votedNot06==1 |input_df$votedNot08==1 |
                             input_df$votedNot11==1 | input_df$votedNot15==1 | input_df$votedNot19==1] <- 1
  input_df$votedNotALL[input_df$votedNot65!=1 | input_df$votedNot68!=1 | input_df$votedNot74!=1 |input_df$votedNot79!=1 |
                             input_df$votedNot84!=1 | input_df$votedNot88!=1 | input_df$votedNot93!=1 |input_df$votedNot97!=1 |
                             input_df$votedNot00!=1 | input_df$votedNot04!=1 | input_df$votedNot06!=1 |input_df$votedNot08!=1 |
                             input_df$votedNot11!=1 | input_df$votedNot15!=1 | input_df$votedNot19!=1] <- 0

  #### A. Liberal ####
  input_df$votedLibALL <- NA
  input_df$votedLibALL[input_df$votedLib65==1 | input_df$votedLib68==1 | input_df$votedLib74==1 | input_df$votedLib79==1 | input_df$votedLib84==1 |
                             input_df$votedLib88==1 | input_df$votedLib93==1 | input_df$votedLib97==1 | input_df$votedLib00==1 | input_df$votedLib04==1 |
                             input_df$votedLib06==1 | input_df$votedLib08==1 | input_df$votedLib11==1 | input_df$votedLib15==1 | input_df$votedLib19==1] <- 1
  input_df$votedLibALL[input_df$votedLib65!=1 | input_df$votedLib68!=1 | input_df$votedLib74!=1 | input_df$votedLib79!=1 | input_df$votedLib84!=1 |
                             input_df$votedLib88!=1 | input_df$votedLib93!=1 | input_df$votedLib97!=1 | input_df$votedLib00!=1 | input_df$votedLib04!=1 |
                             input_df$votedLib06!=1 | input_df$votedLib08!=1 | input_df$votedLib11!=1 | input_df$votedLib15!=1 | input_df$votedLib19!=1] <- 0

  #### B. Conservative #####
  input_df$votedConsALL <- NA
  input_df$votedConsALL[input_df$votedPCons65==1 | input_df$votedPCons68==1 | input_df$votedPCons74==1 | input_df$votedPCons79==1 |
                              input_df$votedPCons84==1 | input_df$votedPCons88==1 | input_df$votedCons93_MergePC==1 | input_df$votedCons97_MergePC==1 |
                              input_df$votedCons00_MergePC==1 | input_df$votedCons04==1 | input_df$votedCons06==1 | input_df$votedCons08==1 |
                              input_df$votedCons11==1 | input_df$votedCons15==1 | input_df$votedCons19==1] <- 1
  input_df$votedConsALL[input_df$votedPCons65!=1 | input_df$votedPCons68!=1 | input_df$votedPCons74!=1 | input_df$votedPCons79!=1 |
                              input_df$votedPCons84!=1 | input_df$votedPCons88!=1 | input_df$votedCons93_MergePC!=1 | input_df$votedCons97_MergePC!=1 |
                              input_df$votedCons00_MergePC!=1 | input_df$votedCons04!=1 | input_df$votedCons06!=1 | input_df$votedCons08!=1 |
                              input_df$votedCons11!=1 | input_df$votedCons15!=1 | input_df$votedCons19!=1] <- 0

  #### C. NDP ####
  input_df$votedNdpALL <- NA
  input_df$votedNdpALL[input_df$votedNdp65==1 | input_df$votedNdp68==1 | input_df$votedNdp74==1 | input_df$votedNdp79==1 |
                             input_df$votedNdp84==1 | input_df$votedNdp88==1 | input_df$votedNdp93==1 | input_df$votedNdp97==1 |
                             input_df$votedNdp00==1 | input_df$votedNdp04==1 | input_df$votedNdp06==1 | input_df$votedNdp08==1 |
                             input_df$votedNdp11==1 | input_df$votedNdp15==1 | input_df$votedNdp19==1] <- 1
  input_df$votedNdpALL[input_df$votedNdp65!=1 | input_df$votedNdp68!=1 | input_df$votedNdp74!=1 | input_df$votedNdp79!=1 |
                             input_df$votedNdp84!=1 | input_df$votedNdp88!=1 | input_df$votedNdp93!=1 | input_df$votedNdp97!=1 |
                             input_df$votedNdp00!=1 | input_df$votedNdp04!=1 | input_df$votedNdp06!=1 | input_df$votedNdp08!=1 |
                             input_df$votedNdp11!=1 | input_df$votedNdp15!=1 | input_df$votedNdp19!=1] <- 0

  #### D. Bloc ####
  input_df$votedBlocALL <- NA
  input_df$votedBlocALL[input_df$votedBloc93==1 | input_df$votedBloc97==1 |
                              input_df$votedBloc00==1 | input_df$votedBloc04==1 |
                              input_df$votedBloc06==1 | input_df$votedBloc08==1 |
                              input_df$votedBloc11==1 | input_df$votedBloc15==1 |
                              input_df$votedBloc19==1] <- 1
  input_df$votedBlocALL[input_df$votedBloc93!=1 | input_df$votedBloc97!=1 |
                              input_df$votedBloc00!=1 | input_df$votedBloc04!=1 |
                              input_df$votedBloc06!=1 | input_df$votedBloc08!=1 |
                              input_df$votedBloc11!=1 | input_df$votedBloc15!=1 |
                              input_df$votedBloc19!=1] <- 0


  #### E. Reform/Alliance ####
  input_df$votedRefAllianceALL <- NA
  input_df$votedRefAllianceALL[input_df$votedReform93==1 | input_df$votedReform97==1 | input_df$votedAlliance00==1] <- 1
  input_df$votedRefAllianceALL[input_df$votedReform93!=1 | input_df$votedReform97!=1 | input_df$votedAlliance00!=1] <- 0

  #### F. Green ####
  input_df$votedGrnALL <- NA
  input_df$votedGrnALL[input_df$votedGrn06==1 | input_df$votedGrn08==1 |
                             input_df$votedGrn11==1 | input_df$votedGrn15==1 |
                             input_df$votedGrn19==1] <- 1
  input_df$votedGrnALL[input_df$votedGrn06!=1 | input_df$votedGrn08!=1 |
                             input_df$votedGrn11!=1 | input_df$votedGrn15!=1 |
                             input_df$votedGrn19!=1] <- 0

  # Incumbent
  input_df$votedIncumbALL <- NA
  input_df$votedIncumbALL[input_df$votedLib65==1 |   input_df$votedLib68==1 |   input_df$votedLib74==1 | input_df$votedLib79==1 | input_df$votedLib84==1 |
                                input_df$votedPCons88==1 | input_df$votedPCons93==1 | input_df$votedLib97==1 | input_df$votedLib00==1 | input_df$votedLib04==1 | input_df$votedLib06==1 |
                                input_df$votedCons08==1 |  input_df$votedCons11==1 |  input_df$votedCons15==1 | input_df$votedLib19==1] <- 1
  input_df$votedIncumbALL[input_df$votedLib65!=1 |   input_df$votedLib68!=1 |   input_df$votedLib74!=1 | input_df$votedLib79!=1 | input_df$votedLib84!=1 |
                                input_df$votedPCons88!=1 | input_df$votedPCons93!=1 | input_df$votedLib97!=1 | input_df$votedLib00!=1 | input_df$votedLib04!=1 | input_df$votedLib06!=1 |
                                input_df$votedCons08!=1 |  input_df$votedCons11!=1 |  input_df$votedCons15!=1 | input_df$votedLib19!=1] <- 0

  # Winner
  input_df$votedWinnerALL <- NA
  input_df$votedWinnerALL[input_df$votedLib65==1 |   input_df$votedLib68==1 | input_df$votedLib74==1 | input_df$votedPCons79==1 | input_df$votedPCons84==1 |
                                input_df$votedPCons88==1 | input_df$votedLib93==1 | input_df$votedLib97==1 | input_df$votedLib00==1 |   input_df$votedLib04==1 | input_df$votedCons06==1 |
                                input_df$votedCons08==1 |  input_df$votedCons11==1 | input_df$votedLib15==1 | input_df$votedLib19==1] <- 1
  input_df$votedWinnerALL[input_df$votedLib65!=1 |   input_df$votedLib68!=1 | input_df$votedLib74!=1 | input_df$votedPCons79!=1 | input_df$votedPCons84!=1 |
                                input_df$votedPCons88!=1 | input_df$votedLib93!=1 | input_df$votedLib97!=1 | input_df$votedLib00!=1 |   input_df$votedLib04!=1 |input_df$votedCons06!=1 |
                                input_df$votedCons08!=1 |  input_df$votedCons11!=1 | input_df$votedLib15!=1 | input_df$votedLib19!=1] <- 0



  #### 2.1.5 vote intent ####

  input_df$voteIntentLibALL <- input_df$voteIntentLib88
  input_df$voteIntentLibALL[is.na(input_df$voteIntentLibALL)] <- input_df$voteIntentLib93[is.na(input_df$voteIntentLibALL)]
  input_df$voteIntentLibALL[is.na(input_df$voteIntentLibALL)] <- input_df$voteIntentLib97[is.na(input_df$voteIntentLibALL)]
  input_df$voteIntentLibALL[is.na(input_df$voteIntentLibALL)] <- input_df$voteIntentLib00[is.na(input_df$voteIntentLibALL)]
  input_df$voteIntentLibALL[is.na(input_df$voteIntentLibALL)] <- input_df$voteIntentLib04[is.na(input_df$voteIntentLibALL)]
  input_df$voteIntentLibALL[is.na(input_df$voteIntentLibALL)] <- input_df$voteIntentLib06[is.na(input_df$voteIntentLibALL)]
  input_df$voteIntentLibALL[is.na(input_df$voteIntentLibALL)] <- input_df$voteIntentLib08[is.na(input_df$voteIntentLibALL)]
  input_df$voteIntentLibALL[is.na(input_df$voteIntentLibALL)] <- input_df$voteIntentLib11[is.na(input_df$voteIntentLibALL)]
  input_df$voteIntentLibALL[is.na(input_df$voteIntentLibALL)] <- input_df$voteIntentLib15[is.na(input_df$voteIntentLibALL)]
  input_df$voteIntentLibALL[is.na(input_df$voteIntentLibALL)] <- input_df$voteIntentLib19[is.na(input_df$voteIntentLibALL)]

  input_df$voteIntentNdpALL <- input_df$voteIntentNdp88
  input_df$voteIntentNdpALL[is.na(input_df$voteIntentNdpALL)] <- input_df$voteIntentNdp93[is.na(input_df$voteIntentNdpALL)]
  input_df$voteIntentNdpALL[is.na(input_df$voteIntentNdpALL)] <- input_df$voteIntentNdp97[is.na(input_df$voteIntentNdpALL)]
  input_df$voteIntentNdpALL[is.na(input_df$voteIntentNdpALL)] <- input_df$voteIntentNdp00[is.na(input_df$voteIntentNdpALL)]
  input_df$voteIntentNdpALL[is.na(input_df$voteIntentNdpALL)] <- input_df$voteIntentNdp04[is.na(input_df$voteIntentNdpALL)]
  input_df$voteIntentNdpALL[is.na(input_df$voteIntentNdpALL)] <- input_df$voteIntentNdp06[is.na(input_df$voteIntentNdpALL)]
  input_df$voteIntentNdpALL[is.na(input_df$voteIntentNdpALL)] <- input_df$voteIntentNdp08[is.na(input_df$voteIntentNdpALL)]
  input_df$voteIntentNdpALL[is.na(input_df$voteIntentNdpALL)] <- input_df$voteIntentNdp11[is.na(input_df$voteIntentNdpALL)]
  input_df$voteIntentNdpALL[is.na(input_df$voteIntentNdpALL)] <- input_df$voteIntentNdp15[is.na(input_df$voteIntentNdpALL)]
  input_df$voteIntentNdpALL[is.na(input_df$voteIntentNdpALL)] <- input_df$voteIntentNdp19[is.na(input_df$voteIntentNdpALL)]

  input_df$voteIntentBlocALL <- input_df$voteIntentBloc93
  input_df$voteIntentBlocALL[is.na(input_df$voteIntentBlocALL)] <- input_df$voteIntentBloc97[is.na(input_df$voteIntentBlocALL)]
  input_df$voteIntentBlocALL[is.na(input_df$voteIntentBlocALL)] <- input_df$voteIntentBloc00[is.na(input_df$voteIntentBlocALL)]
  input_df$voteIntentBlocALL[is.na(input_df$voteIntentBlocALL)] <- input_df$voteIntentBloc04[is.na(input_df$voteIntentBlocALL)]
  input_df$voteIntentBlocALL[is.na(input_df$voteIntentBlocALL)] <- input_df$voteIntentBloc06[is.na(input_df$voteIntentBlocALL)]
  input_df$voteIntentBlocALL[is.na(input_df$voteIntentBlocALL)] <- input_df$voteIntentBloc08[is.na(input_df$voteIntentBlocALL)]
  input_df$voteIntentBlocALL[is.na(input_df$voteIntentBlocALL)] <- input_df$voteIntentBloc11[is.na(input_df$voteIntentBlocALL)]
  input_df$voteIntentBlocALL[is.na(input_df$voteIntentBlocALL)] <- input_df$voteIntentBloc15[is.na(input_df$voteIntentBlocALL)]
  input_df$voteIntentBlocALL[is.na(input_df$voteIntentBlocALL)] <- input_df$voteIntentBloc19[is.na(input_df$voteIntentBlocALL)]

  input_df$voteIntentGrnALL <- input_df$voteIntentGreen00
  input_df$voteIntentGrnALL[is.na(input_df$voteIntentGrnALL)] <- input_df$voteIntentGrn04[is.na(input_df$voteIntentGrnALL)]
  input_df$voteIntentGrnALL[is.na(input_df$voteIntentGrnALL)] <- input_df$voteIntentGrn06[is.na(input_df$voteIntentGrnALL)]
  input_df$voteIntentGrnALL[is.na(input_df$voteIntentGrnALL)] <- input_df$voteIntentGrn08[is.na(input_df$voteIntentGrnALL)]
  input_df$voteIntentGrnALL[is.na(input_df$voteIntentGrnALL)] <- input_df$voteIntentGreen11[is.na(input_df$voteIntentGrnALL)]
  input_df$voteIntentGrnALL[is.na(input_df$voteIntentGrnALL)] <- input_df$voteIntentGreen15[is.na(input_df$voteIntentGrnALL)]
  input_df$voteIntentGrnALL[is.na(input_df$voteIntentGrnALL)] <- input_df$voteIntentGreen19[is.na(input_df$voteIntentGrnALL)]

  input_df$voteIntentConsALL <- input_df$voteIntentCons88
  input_df$voteIntentConsALL[is.na(input_df$voteIntentConsALL)] <- input_df$voteIntentCons93[is.na(input_df$voteIntentConsALL)]
  input_df$voteIntentConsALL[is.na(input_df$voteIntentConsALL)] <- input_df$voteIntentCons97[is.na(input_df$voteIntentConsALL)]
  input_df$voteIntentConsALL[is.na(input_df$voteIntentConsALL)] <- input_df$voteIntentCons00[is.na(input_df$voteIntentConsALL)]
  input_df$voteIntentConsALL[is.na(input_df$voteIntentConsALL)] <- input_df$voteIntentCons04[is.na(input_df$voteIntentConsALL)]
  input_df$voteIntentConsALL[is.na(input_df$voteIntentConsALL)] <- input_df$voteIntentCons06[is.na(input_df$voteIntentConsALL)]
  input_df$voteIntentConsALL[is.na(input_df$voteIntentConsALL)] <- input_df$voteIntentCons08[is.na(input_df$voteIntentConsALL)]
  input_df$voteIntentConsALL[is.na(input_df$voteIntentConsALL)] <- input_df$voteIntentCons11[is.na(input_df$voteIntentConsALL)]
  input_df$voteIntentConsALL[is.na(input_df$voteIntentConsALL)] <- input_df$voteIntentCons15[is.na(input_df$voteIntentConsALL)]
  input_df$voteIntentConsALL[is.na(input_df$voteIntentConsALL)] <- input_df$voteIntentCons19[is.na(input_df$voteIntentConsALL)]


  #### 2.3. Feeling about issues ####

  input_df$issImmgRaceFeelALL <- input_df$issImmgRaceFeel68
  input_df$issImmgRaceFeelALL[is.na(input_df$issImmgRaceFeelALL)] <- input_df$issImmgRaceFeel84[is.na(input_df$issImmgRaceFeelALL)]
  input_df$issImmgRaceFeelALL[is.na(input_df$issImmgRaceFeelALL)] <- input_df$issImmgRaceFeel88[is.na(input_df$issImmgRaceFeelALL)]
  #input_df$issImmgRaceFeelALL[is.na(input_df$issImmgRaceFeelALL)] <- input_df$issImmgRaceFeel93[is.na(input_df$issImmgRaceFeelALL)]
  input_df$issImmgRaceFeelALL[is.na(input_df$issImmgRaceFeelALL)] <- input_df$issImmgRaceFeel97[is.na(input_df$issImmgRaceFeelALL)]
  input_df$issImmgRaceFeelALL[is.na(input_df$issImmgRaceFeelALL)] <- input_df$issImmgRaceFeel11[is.na(input_df$issImmgRaceFeelALL)]
  input_df$issImmgRaceFeelALL[is.na(input_df$issImmgRaceFeelALL)] <- input_df$issImmgRaceFeel11[is.na(input_df$issImmgRaceFeelALL)]

  input_df$issImmgDoneALL <- input_df$issImmgDone93
  input_df$issImmgDoneALL[is.na(input_df$issImmgDoneALL)] <- input_df$issImmgDone97[is.na(input_df$issImmgDoneALL)]
  input_df$issImmgDoneALL[is.na(input_df$issImmgDoneALL)] <- input_df$issImmgDone00[is.na(input_df$issImmgDoneALL)]
  input_df$issImmgDoneALL[is.na(input_df$issImmgDoneALL)] <- input_df$issImmgDone11[is.na(input_df$issImmgDoneALL)]

  input_df$issWhiteFeelALL <- input_df$issWhiteFeel68
  input_df$issWhiteFeelALL[is.na(input_df$issWhiteFeelALL)] <- input_df$issWhiteFeel84[is.na(input_df$issWhiteFeelALL)]
  input_df$issWhiteFeelALL[is.na(input_df$issWhiteFeelALL)] <- input_df$issWhiteFeel11[is.na(input_df$issWhiteFeelALL)]

  input_df$issCathoFeelALL <- input_df$issCathoFeel68
  input_df$issCathoFeelALL[is.na(input_df$issCathoFeelALL)] <- input_df$issCathoFeel11[is.na(input_df$issCathoFeelALL)]

  input_df$issEngCanFeelALL <- input_df$issEngCanFeel68
  input_df$issEngCanFeelALL[is.na(input_df$issEngCanFeelALL)] <- input_df$issEngCanFeel84[is.na(input_df$issEngCanFeelALL)]
  input_df$issEngCanFeelALL[is.na(input_df$issEngCanFeelALL)] <- input_df$issEngCanFeel88[is.na(input_df$issEngCanFeelALL)]

  #input_df$issAborigFeelALL <- input_df$issAborigFeel88
  #input_df$issAborigFeelALL[is.na(input_df$issAborigFeelALL)] <- input_df$issAborigFeel93[is.na(input_df$issAborigFeelALL)]
  input_df$issAborigFeelALL[is.na(input_df$issAborigFeelALL)] <- input_df$issAborigFeel97[is.na(input_df$issAborigFeelALL)]
  input_df$issAborigFeelALL[is.na(input_df$issAborigFeelALL)] <- input_df$issAborigFeel00[is.na(input_df$issAborigFeelALL)]
  input_df$issAborigFeelALL[is.na(input_df$issAborigFeelALL)] <- input_df$issAborigFeel04[is.na(input_df$issAborigFeelALL)]
  input_df$issAborigFeelALL[is.na(input_df$issAborigFeelALL)] <- input_df$issAborigFeel08[is.na(input_df$issAborigFeelALL)]
  input_df$issAborigFeelALL[is.na(input_df$issAborigFeelALL)] <- input_df$issAborigFeel11[is.na(input_df$issAborigFeelALL)]

  input_df$issGayNegFeelALL <- input_df$issSSexGayNegFeel04
  input_df$issGayNegFeelALL[is.na(input_df$issGayNegFeelALL)] <- input_df$issSSexGayNegFeel08[is.na(input_df$issGayNegFeelALL)]
  input_df$issGayNegFeelALL[is.na(input_df$issGayNegFeelALL)] <- input_df$issSSexGayNegFeel15[is.na(input_df$issGayNegFeelALL)]


  input_df$issImmgAdmitALL <- input_df$issImmgAdmit68
  #input_df$issImmgAdmitALL[is.na(input_df$issImmgAdmitALL)] <- input_df$issImmgAdmit88[is.na(input_df$issImmgAdmitALL)]
  input_df$issImmgAdmitALL[is.na(input_df$issImmgAdmitALL)] <- input_df$issImmgAdmit93[is.na(input_df$issImmgAdmitALL)]
  input_df$issImmgAdmitALL[is.na(input_df$issImmgAdmitALL)] <- input_df$issImmgAdmit97[is.na(input_df$issImmgAdmitALL)]
  input_df$issImmgAdmitALL[is.na(input_df$issImmgAdmitALL)] <- input_df$issImmgAdmit00[is.na(input_df$issImmgAdmitALL)]
  input_df$issImmgAdmitALL[is.na(input_df$issImmgAdmitALL)] <- input_df$issImmgAdmit04[is.na(input_df$issImmgAdmitALL)]
  input_df$issImmgAdmitALL[is.na(input_df$issImmgAdmitALL)] <- input_df$issImmgAdmit06[is.na(input_df$issImmgAdmitALL)]
  input_df$issImmgAdmitALL[is.na(input_df$issImmgAdmitALL)] <- input_df$issImmgAdmit08[is.na(input_df$issImmgAdmitALL)]
  input_df$issImmgAdmitALL[is.na(input_df$issImmgAdmitALL)] <- input_df$issImmgAdmit11[is.na(input_df$issImmgAdmitALL)]
  input_df$issImmgAdmitALL[is.na(input_df$issImmgAdmitALL)] <- input_df$issImmgAdmit15[is.na(input_df$issImmgAdmitALL)]
  input_df$issImmgAdmitALL[is.na(input_df$issImmgAdmitALL)] <- input_df$issImmgAdmit19[is.na(input_df$issImmgAdmitALL)]


  input_df$issQcFeelALL <- input_df$issQcFeel68
  input_df$issQcFeelALL[is.na(input_df$issQcFeelALL)] <- input_df$issQcFeel84[is.na(input_df$issQcFeelALL)]
  input_df$issQcFeelALL[is.na(input_df$issQcFeelALL)] <- input_df$issQcFeel88[is.na(input_df$issQcFeelALL)]
  #input_df$issQcFeelALL[is.na(input_df$issQcFeelALL)] <- input_df$issQcFeel93[is.na(input_df$issQcFeelALL)]
  input_df$issQcFeelALL[is.na(input_df$issQcFeelALL)] <- input_df$issQcFeel97[is.na(input_df$issQcFeelALL)]
  input_df$issQcFeelALL[is.na(input_df$issQcFeelALL)] <- input_df$issQcFeel00[is.na(input_df$issQcFeelALL)]
  input_df$issQcFeelALL[is.na(input_df$issQcFeelALL)] <- input_df$issQcFeel04[is.na(input_df$issQcFeelALL)]
  input_df$issQcFeelALL[is.na(input_df$issQcFeelALL)] <- input_df$issQcFeel08[is.na(input_df$issQcFeelALL)]
  input_df$issQcFeelALL[is.na(input_df$issQcFeelALL)] <- input_df$issQcFeel11[is.na(input_df$issQcFeelALL)]
  input_df$issQcFeelALL[is.na(input_df$issQcFeelALL)] <- input_df$issQcFeel15[is.na(input_df$issQcFeelALL)]
  input_df$issQcFeelALL[is.na(input_df$issQcFeelALL)] <- input_df$issQuebecFeel19[is.na(input_df$issQcFeelALL)]


  input_df$issQcSovBinALL <- input_df$issQcSovBin68
  input_df$issQcSovBinALL[is.na(input_df$issQcSovBinALL)] <- input_df$issQcSovBin79[is.na(input_df$issQcSovBinALL)]
  input_df$issQcSovBinALL[is.na(input_df$issQcSovBinALL)] <- input_df$issQcSovBin88[is.na(input_df$issQcSovBinALL)]
  #input_df$issQcSovBinALL[is.na(input_df$issQcSovBinALL)] <- input_df$issQcSovBin93[is.na(input_df$issQcSovBinALL)]
  input_df$issQcSovBinALL[is.na(input_df$issQcSovBinALL)] <- input_df$issQcSovBin97[is.na(input_df$issQcSovBinALL)]
  input_df$issQcSovBinALL[is.na(input_df$issQcSovBinALL)] <- input_df$issQcSovBin00[is.na(input_df$issQcSovBinALL)]
  input_df$issQcSovBinALL[is.na(input_df$issQcSovBinALL)] <- input_df$issQcSovBin04[is.na(input_df$issQcSovBinALL)]
  input_df$issQcSovBinALL[is.na(input_df$issQcSovBinALL)] <- input_df$issQcSovBin06[is.na(input_df$issQcSovBinALL)]
  input_df$issQcSovBinALL[is.na(input_df$issQcSovBinALL)] <- input_df$issQcSovBin08[is.na(input_df$issQcSovBinALL)]
  input_df$issQcSovBinALL[is.na(input_df$issQcSovBinALL)] <- input_df$issQcSovBin11[is.na(input_df$issQcSovBinALL)]

  input_df$issQcDoneALL <- input_df$issQcDone93
  input_df$issQcDoneALL[is.na(input_df$issQcDoneALL)] <- input_df$issQcDone97[is.na(input_df$issQcDoneALL)]
  input_df$issQcDoneALL[is.na(input_df$issQcDoneALL)] <- input_df$issQcDone00[is.na(input_df$issQcDoneALL)]
  input_df$issQcDoneALL[is.na(input_df$issQcDoneALL)] <- input_df$issQcDone04[is.na(input_df$issQcDoneALL)]
  input_df$issQcDoneALL[is.na(input_df$issQcDoneALL)] <- input_df$issQcDone06[is.na(input_df$issQcDoneALL)]
  input_df$issQcDoneALL[is.na(input_df$issQcDoneALL)] <- input_df$issQcDone08[is.na(input_df$issQcDoneALL)]
  input_df$issQcDoneALL[is.na(input_df$issQcDoneALL)] <- input_df$issQcDone11[is.na(input_df$issQcDoneALL)]
  input_df$issQcDoneALL[is.na(input_df$issQcDoneALL)] <- input_df$issDoneQC15[is.na(input_df$issQcDoneALL)]
  #input_df$issQcDoneALL[is.na(input_df$issQcDoneALL)] <- input_df$issDoneQC19[is.na(input_df$issQcDoneALL)]


  input_df$issFrgnUsFeelALL <- input_df$issAmericansFeel68
  input_df$issFrgnUsFeelALL[is.na(input_df$issFrgnUsFeelALL)] <- input_df$issFrgnUsFeel74[is.na(input_df$issFrgnUsFeelALL)]
  input_df$issFrgnUsFeelALL[is.na(input_df$issFrgnUsFeelALL)] <- input_df$issAmericansFeel88[is.na(input_df$issFrgnUsFeelALL)]
  input_df$issFrgnUsFeelALL[is.na(input_df$issFrgnUsFeelALL)] <- input_df$issFrgnUsFeel97[is.na(input_df$issFrgnUsFeelALL)]
  input_df$issFrgnUsFeelALL[is.na(input_df$issFrgnUsFeelALL)] <- input_df$issFrgnUsFeel00[is.na(input_df$issFrgnUsFeelALL)]
  input_df$issFrgnUsFeelALL[is.na(input_df$issFrgnUsFeelALL)] <- input_df$issFrgnUsFeel04[is.na(input_df$issFrgnUsFeelALL)]
  input_df$issFrgnUsFeelALL[is.na(input_df$issFrgnUsFeelALL)] <- input_df$issFrgnUsFeel08[is.na(input_df$issFrgnUsFeelALL)]
  input_df$issFrgnUsFeelALL[is.na(input_df$issFrgnUsFeelALL)] <- input_df$issFrgnUsFeel11[is.na(input_df$issFrgnUsFeelALL)]
  input_df$issFrgnUsFeelALL[is.na(input_df$issFrgnUsFeelALL)] <- input_df$issFrgnUsFeel15[is.na(input_df$issFrgnUsFeelALL)]

  input_df$issCanadaFeelALL <- input_df$issCanadaFeel74
  input_df$issCanadaFeelALL[is.na(input_df$issCanadaFeelALL)] <- input_df$issCanadaFeel79[is.na(input_df$issCanadaFeelALL)]
  input_df$issCanadaFeelALL[is.na(input_df$issCanadaFeelALL)] <- input_df$issCanadaFeel97[is.na(input_df$issCanadaFeelALL)]
  input_df$issCanadaFeelALL[is.na(input_df$issCanadaFeelALL)] <- input_df$issCanadaFeel00[is.na(input_df$issCanadaFeelALL)]
  input_df$issCanadaFeelALL[is.na(input_df$issCanadaFeelALL)] <- input_df$issCanadaFeel08[is.na(input_df$issCanadaFeelALL)]
  input_df$issCanadaFeelALL[is.na(input_df$issCanadaFeelALL)] <- input_df$issCanadaFeel11[is.na(input_df$issCanadaFeelALL)]
  input_df$issCanadaFeelALL[is.na(input_df$issCanadaFeelALL)] <- input_df$issCanadaFeel15[is.na(input_df$issCanadaFeelALL)]
  input_df$issCanadaFeelALL[is.na(input_df$issCanadaFeelALL)] <- input_df$issCanadaFeel19[is.na(input_df$issCanadaFeelALL)]


  input_df$issOwnProvFeelALL <- input_df$issOwnProvFeel74
  input_df$issOwnProvFeelALL[is.na(input_df$issOwnProvFeelALL)] <- input_df$issOwnProvFeel79[is.na(input_df$issOwnProvFeelALL)]
  input_df$issOwnProvFeelALL[is.na(input_df$issOwnProvFeelALL)] <- input_df$issOwnProvFeel97[is.na(input_df$issOwnProvFeelALL)]
  input_df$issOwnProvFeelALL[is.na(input_df$issOwnProvFeelALL)] <- input_df$issOwnProvFeel00[is.na(input_df$issOwnProvFeelALL)]

  input_df$issQcVsBilingALL <- input_df$issQcVsBiling97
  input_df$issQcVsBilingALL[is.na(input_df$issQcVsBilingALL)] <- input_df$issQcVsBiling00[is.na(input_df$issQcVsBilingALL)]
  input_df$issQcVsBilingALL[is.na(input_df$issQcVsBilingALL)] <- input_df$issQcVsBiling11[is.na(input_df$issQcVsBilingALL)]

  # input_df$issPoliceFeelALL <- input_df$issPoliceFeel68
  # input_df$issPoliceFeelALL[is.na(input_df$issPoliceFeelALL)] <- input_df$issPoliceFeel97[is.na(input_df$issPoliceFeelALL)]
  # input_df$issPoliceFeelALL[is.na(input_df$issPoliceFeelALL)] <- input_df$issPoliceFeel00[is.na(input_df$issPoliceFeelALL)]

  input_df$issBigCieFeelALL <- input_df$issBigCieFeel68
  input_df$issBigCieFeelALL[is.na(input_df$issBigCieFeelALL)] <- input_df$issBigCieFeel84[is.na(input_df$issBigCieFeelALL)]
  input_df$issBigCieFeelALL[is.na(input_df$issBigCieFeelALL)] <- input_df$issBigCieFeel97[is.na(input_df$issBigCieFeelALL)]

  input_df$issLabourFeelALL <- input_df$issLabourFeel68
  input_df$issLabourFeelALL[is.na(input_df$issLabourFeelALL)] <- input_df$issLabourFeel84[is.na(input_df$issLabourFeelALL)]
  input_df$issLabourFeelALL[is.na(input_df$issLabourFeelALL)] <- input_df$issLabourFeel88[is.na(input_df$issLabourFeelALL)]
  input_df$issLabourFeelALL[is.na(input_df$issLabourFeelALL)] <- input_df$issLabourFeel97[is.na(input_df$issLabourFeelALL)]

  input_df$issWomDoneALL <- input_df$issWomDone93
  input_df$issWomDoneALL[is.na(input_df$issWomDoneALL)] <- input_df$issWomDone97[is.na(input_df$issWomDoneALL)]
  input_df$issWomDoneALL[is.na(input_df$issWomDoneALL)] <- input_df$issWomDone00[is.na(input_df$issWomDoneALL)]
  input_df$issWomDoneALL[is.na(input_df$issWomDoneALL)] <- input_df$issWomDone04[is.na(input_df$issWomDoneALL)]
  input_df$issWomDoneALL[is.na(input_df$issWomDoneALL)] <- input_df$issWomDone08[is.na(input_df$issWomDoneALL)]
  input_df$issWomDoneALL[is.na(input_df$issWomDoneALL)] <- input_df$issWomDone11[is.na(input_df$issWomDoneALL)]
  input_df$issWomDoneALL[is.na(input_df$issWomDoneALL)] <- input_df$issDoneWom15[is.na(input_df$issWomDoneALL)]
  input_df$issWomDoneALL[is.na(input_df$issWomDoneALL)] <- input_df$issDoneWom19[is.na(input_df$issWomDoneALL)]


  input_df$issEcnBORWALL <- input_df$issEcnBORW97
  input_df$issEcnBORWALL[is.na(input_df$issEcnBORWALL)] <- input_df$issEcnBORW00[is.na(input_df$issEcnBORWALL)]
  input_df$issEcnBORWALL[is.na(input_df$issEcnBORWALL)] <- input_df$issEcnBORW04[is.na(input_df$issEcnBORWALL)]
  input_df$issEcnBORWALL[is.na(input_df$issEcnBORWALL)] <- input_df$issEcnBORW06[is.na(input_df$issEcnBORWALL)]
  input_df$issEcnBORWALL[is.na(input_df$issEcnBORWALL)] <- input_df$issEcnBORW08[is.na(input_df$issEcnBORWALL)]
  input_df$issEcnBORWALL[is.na(input_df$issEcnBORWALL)] <- input_df$issEcnBORW11[is.na(input_df$issEcnBORWALL)]
  input_df$issEcnBORWALL[is.na(input_df$issEcnBORWALL)] <- input_df$issEcnBORW15[is.na(input_df$issEcnBORWALL)]
  input_df$issEcnBORWALL[is.na(input_df$issEcnBORWALL)] <- input_df$issEcnBORW19[is.na(input_df$issEcnBORWALL)]


  input_df$issGapRichPoorALL <- input_df$issEcnBORW97
  input_df$issGapRichPoorALL[is.na(input_df$issGapRichPoorALL)] <- input_df$issGapRichPoor00[is.na(input_df$issGapRichPoorALL)]
  input_df$issGapRichPoorALL[is.na(input_df$issGapRichPoorALL)] <- input_df$issGapRichPoor04[is.na(input_df$issGapRichPoorALL)]
  input_df$issGapRichPoorALL[is.na(input_df$issGapRichPoorALL)] <- input_df$issGapRichPoor06[is.na(input_df$issGapRichPoorALL)]
  input_df$issGapRichPoorALL[is.na(input_df$issGapRichPoorALL)] <- input_df$issGapRichPoor08[is.na(input_df$issGapRichPoorALL)]
  input_df$issGapRichPoorALL[is.na(input_df$issGapRichPoorALL)] <- input_df$issGapRichPoor11[is.na(input_df$issGapRichPoorALL)]
  input_df$issGapRichPoorALL[is.na(input_df$issGapRichPoorALL)] <- input_df$issGapRichPoor15[is.na(input_df$issGapRichPoorALL)]
  input_df$issGapRichPoorALL[is.na(input_df$issGapRichPoorALL)] <- input_df$issGapRichPoor93[is.na(input_df$issGapRichPoorALL)]


  #### CRIME ####
  input_df$issCrimVsYngALL <- input_df$issCrimVsYng04
  input_df$issCrimVsYngALL[is.na(input_df$issCrimVsYngALL)] <- input_df$issCrimVsYng08[is.na(input_df$issCrimVsYngALL)]
  input_df$issCrimVsYngALL[is.na(input_df$issCrimVsYngALL)] <- input_df$issCrimVsYng11[is.na(input_df$issCrimVsYngALL)]
  input_df$issCrimVsYngALL[is.na(input_df$issCrimVsYngALL)] <- input_df$issCrimVsYng00[is.na(input_df$issCrimVsYngALL)]
  input_df$issCrimVsYngALL[is.na(input_df$issCrimVsYngALL)] <- input_df$issCrimVsYng97[is.na(input_df$issCrimVsYngALL)]
  input_df$issCrimVsYngALL[is.na(input_df$issCrimVsYngALL)] <- input_df$issCrimVsYng06[is.na(input_df$issCrimVsYngALL)]


  input_df$issCrimVsRightALL <- input_df$issCrimVsRight04
  input_df$issCrimVsRightALL[is.na(input_df$issCrimVsRightALL)] <- input_df$issCrimVsRight08[is.na(input_df$issCrimVsRightALL)]
  input_df$issCrimVsRightALL[is.na(input_df$issCrimVsRightALL)] <- input_df$issCrimVsRight11[is.na(input_df$issCrimVsRightALL)]
  input_df$issCrimVsRightALL[is.na(input_df$issCrimVsRightALL)] <- input_df$issCrimVsRight00[is.na(input_df$issCrimVsRightALL)]
  input_df$issCrimVsRightALL[is.na(input_df$issCrimVsRightALL)] <- input_df$issCrimVsRight93[is.na(input_df$issCrimVsRightALL)]


  input_df$issCrimGoalALL <- input_df$issCrimGoal04
  input_df$issCrimGoalALL[is.na(input_df$issCrimGoalALL)] <- input_df$issCrimGoal08[is.na(input_df$issCrimGoalALL)]
  input_df$issCrimGoalALL[is.na(input_df$issCrimGoalALL)] <- input_df$issCrimGoal11[is.na(input_df$issCrimGoalALL)]
  input_df$issCrimGoalALL[is.na(input_df$issCrimGoalALL)] <- input_df$issCrimGoal00[is.na(input_df$issCrimGoalALL)]
  input_df$issCrimGoalALL[is.na(input_df$issCrimGoalALL)] <- input_df$issCrimGoal06[is.na(input_df$issCrimGoalALL)]
  input_df$issCrimGoalALL[is.na(input_df$issCrimGoalALL)] <- input_df$issCrimGoal93[is.na(input_df$issCrimGoalALL)]


  input_df$issCrimSpdALL <- input_df$issCrimSpd11
  input_df$issCrimSpdALL[is.na(input_df$issCrimSpdALL)] <- input_df$issCrimSpd15[is.na(input_df$issCrimSpdALL)]
  input_df$issCrimSpdALL[is.na(input_df$issCrimSpdALL)] <- input_df$issCrimSpd19[is.na(input_df$issCrimSpdALL)]

  input_df$issAttnCrimALL <- input_df$issAttnCrim11
  input_df$issAttnCrimALL[is.na(input_df$issAttnCrimALL)] <- input_df$issAttnCrim15[is.na(input_df$issAttnCrimALL)]
  input_df$issAttnCrimALL[is.na(input_df$issAttnCrimALL)] <- input_df$issAttnCrim97[is.na(input_df$issAttnCrimALL)]
  input_df$issAttnCrimALL[is.na(input_df$issAttnCrimALL)] <- input_df$issImpCrim08[is.na(input_df$issAttnCrimALL)]



  #### 2.4. Political interest ####

  input_df$polInter1FedALL <- input_df$polInter1Fed88
  input_df$polInter1FedALL[is.na(input_df$polInter1FedALL)] <- input_df$polInter1Fed97[is.na(input_df$polInter1FedALL)]
  input_df$polInter1FedALL[is.na(input_df$polInter1FedALL)] <- input_df$polInter1Fed00[is.na(input_df$polInter1FedALL)]
  input_df$polInter1FedALL[is.na(input_df$polInter1FedALL)] <- input_df$polInter1Fed04[is.na(input_df$polInter1FedALL)]
  input_df$polInter1FedALL[is.na(input_df$polInter1FedALL)] <- input_df$polInter1Fed06[is.na(input_df$polInter1FedALL)]
  input_df$polInter1FedALL[is.na(input_df$polInter1FedALL)] <- input_df$polInter1Fed08[is.na(input_df$polInter1FedALL)]
  input_df$polInter1FedALL[is.na(input_df$polInter1FedALL)] <- input_df$polInter1Fed11[is.na(input_df$polInter1FedALL)]
  input_df$polInter1FedALL[is.na(input_df$polInter1FedALL)] <- input_df$polInter1Fed15[is.na(input_df$polInter1FedALL)]


  input_df$polInter2GenALL <- input_df$polInter2Gen97
  input_df$polInter2GenALL[is.na(input_df$polInter2GenALL)] <- input_df$polInter2Gen00[is.na(input_df$polInter2GenALL)]
  input_df$polInter2GenALL[is.na(input_df$polInter2GenALL)] <- input_df$polInter2Gen04[is.na(input_df$polInter2GenALL)]
  input_df$polInter2GenALL[is.na(input_df$polInter2GenALL)] <- input_df$polInter2Gen06[is.na(input_df$polInter2GenALL)]
  input_df$polInter2GenALL[is.na(input_df$polInter2GenALL)] <- input_df$polInter2Gen08[is.na(input_df$polInter2GenALL)]
  input_df$polInter2GenALL[is.na(input_df$polInter2GenALL)] <- input_df$polInter2Gen11[is.na(input_df$polInter2GenALL)]
  input_df$polInter2GenALL[is.na(input_df$polInter2GenALL)] <- input_df$polInter2Fed15[is.na(input_df$polInter2GenALL)]
  input_df$polInter2GenALL[is.na(input_df$polInter2GenALL)] <- input_df$polInter2Fed19[is.na(input_df$polInter2GenALL)]


  input_df$x_polInterestALL <-  input_df$x_polInterest88
  input_df$x_polInterestALL[is.na(input_df$x_polInterestALL)] <- input_df$x_polInterest93[is.na(input_df$x_polInterestALL)]
  input_df$x_polInterestALL[is.na(input_df$x_polInterestALL)] <- input_df$x_polInterest97[is.na(input_df$x_polInterestALL)]
  input_df$x_polInterestALL[is.na(input_df$x_polInterestALL)] <- input_df$x_polInterest00[is.na(input_df$x_polInterestALL)]
  input_df$x_polInterestALL[is.na(input_df$x_polInterestALL)] <- input_df$x_polInterest04[is.na(input_df$x_polInterestALL)]
  input_df$x_polInterestALL[is.na(input_df$x_polInterestALL)] <- input_df$x_polInterest06[is.na(input_df$x_polInterestALL)]
  input_df$x_polInterestALL[is.na(input_df$x_polInterestALL)] <- input_df$x_polInterest08[is.na(input_df$x_polInterestALL)]
  input_df$x_polInterestALL[is.na(input_df$x_polInterestALL)] <- input_df$x_polInterest11[is.na(input_df$x_polInterestALL)]
  input_df$x_polInterestALL[is.na(input_df$x_polInterestALL)] <- input_df$x_polInterest15[is.na(input_df$x_polInterestALL)]


  #### 2.5 Big Five ####

  input_df$persoExtravertALL    <- input_df$persoExtravert11
  input_df$persoExtravertALL[is.na(input_df$persoExtravertALL)] <- input_df$persoExtravert15[is.na(input_df$persoExtravertALL)]
  input_df$persoExtravertALL[is.na(input_df$persoExtravertALL)] <- input_df$persoExtravert19[is.na(input_df$persoExtravertALL)]


  input_df$persoCriticalALL    <- input_df$persoCritical11
  input_df$persoCriticalALL[is.na(input_df$persoCriticalALL)] <- input_df$persoCritical15[is.na(input_df$persoCriticalALL)]
  input_df$persoCriticalALL[is.na(input_df$persoCriticalALL)] <- input_df$persoCritical19[is.na(input_df$persoCriticalALL)]


  input_df$persoDisciplinedALL    <- input_df$persoDisciplined11
  input_df$persoDisciplinedALL[is.na(input_df$persoDisciplinedALL)] <- input_df$persoDisciplined15[is.na(input_df$persoDisciplinedALL)]
  input_df$persoDisciplinedALL[is.na(input_df$persoDisciplinedALL)] <- input_df$persoDisciplined19[is.na(input_df$persoDisciplinedALL)]


  input_df$persoAnxiousALL    <- input_df$persoAnxious11
  input_df$persoAnxiousALL[is.na(input_df$persoAnxiousALL)] <- input_df$persoAnxious15[is.na(input_df$persoAnxiousALL)]
  input_df$persoAnxiousALL[is.na(input_df$persoAnxiousALL)] <- input_df$persoAnxious19[is.na(input_df$persoAnxiousALL)]

  input_df$persoOpenComplexALL    <- input_df$persoOpenComplex11
  input_df$persoOpenComplexALL[is.na(input_df$persoOpenComplexALL)] <- input_df$persoOpenComplex15[is.na(input_df$persoOpenComplexALL)]
  input_df$persoOpenComplexALL[is.na(input_df$persoOpenComplexALL)] <- input_df$persoOpenComplex19[is.na(input_df$persoOpenComplexALL)]


  input_df$persoQuietALL    <- input_df$persoQuiet11
  input_df$persoQuietALL[is.na(input_df$persoQuietALL)] <- input_df$persoQuiet15[is.na(input_df$persoQuietALL)]
  input_df$persoQuietALL[is.na(input_df$persoQuietALL)] <- input_df$persoQuiet19[is.na(input_df$persoQuietALL)]


  input_df$persoWarmALL    <- input_df$persoWarm11
  input_df$persoWarmALL[is.na(input_df$persoWarmALL)] <- input_df$persoWarm15[is.na(input_df$persoWarmALL)]
  input_df$persoWarmALL[is.na(input_df$persoWarmALL)] <- input_df$persoWarm19[is.na(input_df$persoWarmALL)]


  input_df$persoDisorganizedALL    <- input_df$persoDisorganized11
  input_df$persoDisorganizedALL[is.na(input_df$persoDisorganizedALL)] <- input_df$persoDisorganized15[is.na(input_df$persoDisorganizedALL)]


  input_df$persoCalmALL    <- input_df$persoCalm11
  input_df$persoCalmALL[is.na(input_df$persoCalmALL)] <- input_df$persoCalm15[is.na(input_df$persoCalmALL)]
  input_df$persoCalmALL[is.na(input_df$persoCalmALL)] <- input_df$persoCalm19[is.na(input_df$persoCalmALL)]

  input_df$persoConventionalALL    <- input_df$persoConventional11
  input_df$persoConventionalALL[is.na(input_df$persoConventionalALL)] <- input_df$persoConventional15[is.na(input_df$persoConventionalALL)]
  input_df$persoConventionalALL[is.na(input_df$persoConventionalALL)] <- input_df$persoConventional19[is.na(input_df$persoConventionalALL)]


  input_df$issHlthVSPrivALL    <- input_df$issHlthVSPriv11
  #input_df$issHlthVSPrivALL[is.na(input_df$issHlthVSPrivALL)] <- input_df$issHlthVSPriv15[is.na(input_df$issHlthVSPrivALL)]
  input_df$issHlthVSPrivALL[is.na(input_df$issHlthVSPrivALL)] <- input_df$issHlthVSPriv08[is.na(input_df$issHlthVSPrivALL)]
  input_df$issHlthVSPrivALL[is.na(input_df$issHlthVSPrivALL)] <- input_df$issHlthVSPriv04[is.na(input_df$issHlthVSPrivALL)]
  input_df$issHlthVSPrivALL[is.na(input_df$issHlthVSPrivALL)] <- input_df$issHlthVSPriv00[is.na(input_df$issHlthVSPrivALL)]

  input_df$issAborigFeelALL    <- input_df$issAborigFeel15
  input_df$issAborigFeelALL[is.na(input_df$issAborigFeelALL)] <- input_df$issAborigFeel11[is.na(input_df$issAborigFeelALL)]
  input_df$issAborigFeelALL[is.na(input_df$issAborigFeelALL)] <- input_df$issAborigFeel08[is.na(input_df$issAborigFeelALL)]
  input_df$issAborigFeelALL[is.na(input_df$issAborigFeelALL)] <- input_df$issAborigFeel04[is.na(input_df$issAborigFeelALL)]
  input_df$issAborigFeelALL[is.na(input_df$issAborigFeelALL)] <- input_df$issAborigFeel00[is.na(input_df$issAborigFeelALL)]
  input_df$issAborigFeelALL[is.na(input_df$issAborigFeelALL)] <- input_df$issAborigFeel97[is.na(input_df$issAborigFeelALL)]
  input_df$issAborigFeelALL[is.na(input_df$issAborigFeelALL)] <- input_df$issAborigFeel93[is.na(input_df$issAborigFeelALL)]
  input_df$issAborigFeelALL[is.na(input_df$issAborigFeelALL)] <- input_df$issAborigFeel88[is.na(input_df$issAborigFeelALL)]
  input_df$issAborigFeelALL[is.na(input_df$issAborigFeelALL)] <- input_df$issAborigFeel19[is.na(input_df$issAborigFeelALL)]

  input_df$issAbortALL    <- input_df$issAbort19
  input_df$issAbortALL[is.na(input_df$issAbortALL)] <- input_df$issAbort15[is.na(input_df$issAbortALL)]
  input_df$issAbortALL[is.na(input_df$issAbortALL)] <- input_df$issAbortNoAccess04[is.na(input_df$issAbortALL)]
  input_df$issAbortALL[is.na(input_df$issAbortALL)] <- input_df$issAbortNoAccess08[is.na(input_df$issAbortALL)]
  input_df$issAbortALL[is.na(input_df$issAbortALL)] <- input_df$issAbortNoAccess11[is.na(input_df$issAbortALL)]
  input_df$unique_id <- seq_len(nrow(input_df))
  return(input_df)
}

# Function to upload data
upload_warehouse_globales <- function(input_df) {
  for (i in seq_len(nrow(input_df))) {
    row <- as.list(input_df[i, ])

    clessnverse::commit_warehouse_row(
      table = warehouse_table_name,
      key = input_df$unique_id[i], ### Clé des données GlobalES
      row,
      mode = "refresh",
      credentials
    )
  }
}

###############################################################################
######################            Functions to           ######################
######################  Get Data Sources from DataLake   ######################
######################              HUB 3.0              ######################
###############################################################################

# Function to list lake items for GlobalES data by country
#
# Search all the files under the subdirectory corresponding to the country
# given in parameter in the GlobalES files in the lake
#
# Return a list of data.frame corresponding to the content of all files of the
# country subdirectory
get_lake_items_globales <- function(country) {
  # Note pour l'implémentation :
  # log for each file processed
  lake_items_selection_metadata <-
    list(
      metadata__content_type = "globales_survey",
      metadata__storage_class = "lake",
      metadata__country = country
    )
  hublot_list <- hublot::filter_lake_items(credentials, lake_items_selection_metadata)
  list_tables <- list()
  for (i in seq_len(length(hublot_list[[1]]))){
    df <- utils::read.csv(hublot_list[[1]][[i]]$file)
    list_tables[[i]] <- df
    clessnverse::logit(
      scriptname,
      paste0("      ", hublot_list[[1]][[i]]$key, " added to list_tables"),
      logger
    )
  }
  return(list_tables)
}

###############################################################################
######################            Functions to           ######################
######################   Get Data Sources from HUB 2.0   ######################
###############################################################################


###############################################################################
######################            Functions to          ######################
######################   Get Data Sources from Dropbox   ######################
###############################################################################


###############################################################################
########################               Main              ######################
## This function is the core of your script. It can use global R objects and ##
## variables that you can define in the tryCatch section of the script below ##
###############################################################################

main <- function() {

    ###########################################################################
    # Define local objects of your core algorithm here
    # Ex: parties_list <- list("CAQ", "PLQ", "QS", "PCQ", "PQ")

    ###########################################################################
    # Start your main script here using the best practices in activity logging
    #
    # Ex: warehouse_items_list <- clessnverse::get_warehouse_table(warehouse_table, credentials, nbrows = 0)
    #     clessnverse::logit(scriptname, "Getting political parties press releases from the datalake", logger)
    #     lakes_items_list <- get_lake_press_releases(parties_list)

    # Initial logging
    clessnverse::logit(
      scriptname,
      "Getting the GlobalES data for Canada from year 1965 to 2019.",
      logger
    )

    # Get lake items
    clessnverse::logit(
      scriptname,
      "Lake items downloading beginning...",
      logger
    )
    globales_list_tables <- get_lake_items_globales("CAN")
    clessnverse::logit(
      scriptname,
      paste(length(globales_list_tables),
            "tables downloaded from the lake. End of downloading."
      ),
      logger
    )

    # Process lake items (return input_df)
    clessnverse::logit(
      scriptname,
      "Data processing beginning...",
      logger
    )
    input_df <- merge_all_ces(globales_list_tables)
    clessnverse::logit(
      scriptname,
      "End of data processing.",
      logger
    )

    # Load items to the warehouse
    clessnverse::logit(
      scriptname,
      "Warehouse uploading beginning...",
      logger
    )
    upload_warehouse_globales(input_df)
    clessnverse::logit(
      scriptname,
      paste0(nrow(input_df),
            " lines uploaded in the Global ES warehouse table : ",
            warehouse_table_name,
            ". End of uploading."
      ),
      logger
    )

    # Final logging
    clessnverse::logit(
      scriptname,
      "GlobalES data for Canada from year 1965 to 2019 were uploaded to the warehouse.",
      logger
    )

}


###############################################################################
########################  Error handling wrapper of the   #####################
########################   main script, allowing to log   #####################
######################## the error and warnings in case   #####################
######################## Something goes wrong during the  #####################
######################## automated execution of this code #####################
###############################################################################

tryCatch(
  withCallingHandlers({

    # Package dplyr for the %>%
    # All other packages must be invoked by specifying its name
    # in front ot the function to be called
    library(dplyr)

    # Globals
    # Here you must define all objects (variables, arrays, vectors etc that you
    # want to make global to this entire code and that will be accessible to
    # functions you define above.  Defining globals avoids having to pass them
    # as arguments across your functions in thei code
    #
    # The objects scriptname, opt, logger and credentials *must* be set and
    # used throught your code.
    #
    lake_items_selection_metadata <-
      list(
        metadata__content_type = "globales_survey",
        metadata__storage_class = "lake",
        metadata__country = "CAN"
      )
    warehouse_table_name <- "globales_canada"

    #########################################################################
    # Define your global variables here
    # Ex: lake_path <- "political_party_press_releases"
    #     lake_items_selection_metadata <- list(metadata__province_or_state="QC", metadata__country="CAN", metadata__storage_class="lake")
    #     warehouse_table <- "political_parties_press_releases"

    # scriptname, opt, logger, credentials are mandatory global objects
    # for them we use the <<- assignment so that they are available in
    # all the tryCatch context ("error", "warning", "finally")
    if (!exists("scriptname")) scriptname <<- "l_globales_canada"

    # Uncomment the line below to hardcode the command line option passed to
    # this script when it runs
    # This is particularly useful while developping your script but it's wiser
    # to use real command-line options when puting your script in production in
    # an automated container.
    # opt <- list(dataframe_mode = "refresh", log_output = c("file", "console"), hub_mode = "refresh", download_data = FALSE, translate=FALSE)

    if (!exists("opt")) {
        opt <- clessnverse::process_command_line_options()
    }

    # Initialize the log file
    if (!exists("logger") || is.null(logger) || logger == 0) {
      logger <<-
        clessnverse::log_init(
          scriptname,
          opt$log_output,
          Sys.getenv("LOG_PATH")
        )
    }

    # login to hublot
    clessnverse::logit(scriptname, "Connecting to hub3", logger)
    credentials <- hublot::get_credentials(
        Sys.getenv("HUB3_URL"),
        Sys.getenv("HUB3_USERNAME"),
        Sys.getenv("HUB3_PASSWORD"))

    clessnverse::logit(
      scriptname,
      paste("Execution of",  scriptname, "starting"),
      logger
    )

    status <<- 0

    # Call main script
    main()
  },

  warning = function(w) {
    clessnverse::logit(scriptname, paste(w, collapse = ' '), logger)
    print(w)
    status <<- 2
  }),

  # Handle an error or a call to stop function in the code
  error = function(e) {
    clessnverse::logit(scriptname, paste(e, collapse = ' '), logger)
    print(e)
    status <<- 1
  },

  # Terminate gracefully whether error or not
  finally = {
    clessnverse::logit(
      scriptname,
      paste("Execution of",  scriptname, "program terminated"),
      logger
    )
    clessnverse::log_close(logger)

    # Cleanup
    closeAllConnections()
    rm(logger)

    quit(status = status)
  }
)
