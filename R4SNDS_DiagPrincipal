#-------------------------------------------------------#
#                                                       #
#     Requête SNDS pour établir une base de données     #
#         patients sur la base d'années de sortie et    #
#         de diagnostic principal.                      #
#                                                       #
#     Auteur : Nicolas MINIER                           #
#     Version : 2023-06-13                              #
#                                                       #
#-------------------------------------------------------#


##### Chargements de packages #####
library(DBI)      # Communication avec bases de données relationnelles
library(ROracle)  # Package spécifique Oracle
library(dplyr)    # Requêtes SQL
library(dbplyr)   # Bis
library(haven)    # Import/lecture de bases de données SAS
# library(tidyr)    # Manipulation d'objets
# library(purrr)    # Working with lists and vectors


##### Connection au SNDS (ORACLE) #####
drv <- dbDriver("Oracle") # obtenir le pilote pour se connecter à une base oracle
conn <- dbConnect(drv, dbname = "IPIAMPR2.WORLD") # se connecter à la base IPIAMPR2.WORLD
Sys.setenv(TZ = "Europe/Paris") # fuseaux horaires
Sys.setenv(ORA_SDTZ = "Europe/Paris")


##### FONCTION DE REQUETE #####
# Fonction de requête
request_safe <- function(minyear, maxyear, diaglist, varlist) {
  
  cat("\f")
  
  yearlist <- sprintf('%02d', minyear:maxyear %% 100)
  
  diagsqlrequest <- paste(paste(c("DGN_PAL LIKE ('",rep(c("') OR DGN_PAL LIKE ('"), times=length(diaglist)-1)),diaglist, collapse=''),"')", collapse='')
  diagsqlrequest <- gsub("' ","'",diagsqlrequest)
  diagsqlrequest <- gsub(" '","'",diagsqlrequest)
  
  for (i in 1:length(yearlist)) {
    MCOxxA <- tbl(conn, paste0("T_MCO",yearlist[i],"A"))
    MCOxxB <- tbl(conn, paste0("T_MCO",yearlist[i],"B"))
    MCOxxC <- tbl(conn, paste0("T_MCO",yearlist[i],"C"))
    MCOxxD <- tbl(conn, paste0("T_MCO",yearlist[i],"D"))
    MCOxxE <- tbl(conn, paste0("T_MCO",yearlist[i],"E"))
    
    tables <- list("MCOxxA" = MCOxxA,
                   "MCOxxB" = MCOxxB,
                   "MCOxxC" = MCOxxC,
                   "MCOxxD" = MCOxxD,
                   "MCOxxE" = MCOxxE)
    
    # Listes des variables obligatoires (clés de jointure entre tables)
    mandatory_var <- list("MCOxxA" = c("ETA_NUM", "RSA_NUM"),
                          "MCOxxB" = c("ETA_NUM", "RSA_NUM"),
                          "MCOxxC" = c("ETA_NUM", "RSA_NUM", "NIR_ANO_17"),
                          "MCOxxD" = c("ETA_NUM", "RSA_NUM"),
                          "MCOxxE" = c("ETA_NUM"))
    
    # Cas particulier des tables qui n'ont pas toujours existé, ou dont le nom a changé dans le temps.
    
    # Pour la table T_MCO05UM, je ne créé qu'une liste vide qui possède tout de même les éléments architecturaux auxquels je peut être amené à faire référence plus tard, pour éviter que la fonction en plante.
    if (yearlist[i]!="05") {
      MCOxxUM <- tbl(conn, paste0("T_MCO",yearlist[i],"UM"))
    } else {
      MCOxxUM <- list("ops" = list("vars"=""))
    }
    tables$MCOxxUM <- MCOxxUM
    mandatory_var$MCOxxUM <- c("ETA_NUM", "RSA_NUM")
    
    # Creation de la liste finale des variables
    varlist_final <- varlist
    for (j in 1:length(tables)) {
      if (varlist[[j]][1]=="ALL") {
        varlist_final[[j]] <- tables[[j]]$ops$vars
      } else if(varlist[[j]][1]=="NONE") {
        varlist_final[[j]] <- 0
      } else {
        varlist_final[[j]] <- unique(append(varlist[[j]],mandatory_var[[j]]))
        varlist_final[[j]] <- varlist_final[[j]][varlist_final[[j]] %in% tables[[j]]$ops$vars]
      }
    }
    
    year <- minyear-1+i
    cat(paste0("Chaînage en cours pour l'année ", year,".\n"))
    
    
    if(yearlist[i]=="05"){
      df_temp <-
        tables$MCOxxB %>%
        filter(sql(diagsqlrequest)) %>%
        select(varlist_final$B)
    } else {
      df_temp <-
        tables$MCOxxB %>%
        filter(sql(diagsqlrequest)) %>%
        select(varlist_final$B) %>%
        full_join(
          tables$MCOxxUM %>%
            filter(sql(diagsqlrequest)) %>%
            select(varlist_final$UM),
          by = c("ETA_NUM", "RSA_NUM"),
          suffix = c("_B", "_UM"))
    }
    
    if (varlist_final$A[1]!=0) {
      df_temp <-
        df_temp %>%
        left_join(tables$MCOxxA %>%
                    select(varlist_final$A),
                  by=c("ETA_NUM", "RSA_NUM"))
    }
    if (varlist_final$C[1]!=0) {
      df_temp <-
        df_temp %>%
        left_join(tables$MCOxxC %>%
                    select(varlist_final$C),
                  by=c("ETA_NUM", "RSA_NUM"))
    }
    if (varlist_final$D[1]!=0) {
      df_temp <-
        df_temp %>%
        left_join(tables$MCOxxD %>%
                    select(varlist_final$D),
                  by=c("ETA_NUM", "RSA_NUM"))
    }
    if (varlist_final$E[1]!=0) {
      df_temp <-
        df_temp %>%
        left_join(tables$MCOxxE %>%
                    select(varlist_final$E),
                  by=c("ETA_NUM"))
    }
    
    df_temp <- collect(df_temp)
    
    if (i==1) {
      df_final <- df_temp
    } else {
      df_final <- bind_rows(df_final, df_temp)
    }
    rm(df_temp)
    
    cat(paste0("Chaînage ", year, " OK.\n"))
    
  }
  
  df_final$sejour_id <- group_indices(df_final, ETA_NUM, RSA_NUM)
  nb_sej1 <- length(unique(df_final$sejour_id))
  
  
  cat(paste0("\n",
             "Base de données assemblée.\n",
             nb_sej1, " séjours identifiés.\n",
             "Nettoyage en cours.\n"))
  
  # Liste des établissements à écarter
  # Jusqu'en 2017, certains établissements ont remonté leurs données en doublon.
  # La liste ci-dessous les recense, afin de pouvoir par la suite éliminer les données dupliquées.
  doublelist <- c("130780521", "130783236", "130783293", "130784234", "130804297",
                  "600100101", "750041543", "750100018", "750100042", "750100075",
                  "750100083", "750100091", "750100109", "750100125", "750100166",
                  "750100208", "750100216", "750100232", "750100273", "750100299",
                  "750801441", "750803447", "750803454", "910100015", "910100023",
                  "920100013", "920100021", "920100039", "920100047", "920100054",
                  "920100062", "930100011", "930100037", "930100045", "940100027",
                  "940100035", "940100043", "940100050", "940100068", "950100016",
                  "690783154", "690784137", "690784152", "690784178", "690787478",
                  "830100558")
  
  df_final <-
    df_final %>%
    filter(!ETB_EXE_FIN %in% doublelist)
  nb_sej2 <- length(unique(df_final$sejour_id))
  cat(paste0(nb_sej1-nb_sej2, " doublons éliminés.\n"))
  
  df_final <-
    df_final %>%
    subset(!grepl("^90",df_final$GRG_GHM) | SOR_MOD==9)
  # subset(!grepl("^90",df_final$GRG_GHM))
  # Cette ligne de code alternative ne "sauve" pas dans la base de données les personnnes décédées.
  # Le projet initial ayant justifié la création de cette requête s'intéressant à la gravité d'évènements médicaux (mortalité incluse),
  # les patients décédés étaient considérés comme n'étant pas en "erreur" mais bien porteurs d'une information utile pour le projet.
  
  nb_sej3 <- length(unique(df_final$sejour_id))
  cat(paste0(nb_sej2-nb_sej3, " séjours 'erreurs' éliminés.\n"))
  
  if (maxyear>=2013) {
    df_final <-
      df_final %>%
      subset(NIR_RET==0 & NAI_RET==0 & SEX_RET==0 & SEJ_RET==0 & FHO_RET==0 & PMS_RET==0 & DAT_RET==0 & COH_NAI_RET==0 & COH_SEX_RET==0)
    nb_sej4 <- length(unique(df_final$sejour_id))
    cat(paste0(nb_sej3-nb_sej4, " séjours avec incohérences éliminés.\n", "\n"))
  } else {
    nb_sej4 <- nb_sej3
    cat(paste0("Aucune donnée sur les incohérences. Aucun séjour éliminé sur cette base.\n", "\n"))
  }
  
  
  nb_pat <- length(unique(df_final$NIR_ANO_17))
  if (length(yearlist)==1) {
    cat(paste0("Base de données assemblée et nettoyée.\n",
               nb_sej4, " séjours identifiés sur l'année ", year, ", concernant ", nb_pat, " patients.\n"))
  } else {
    cat(paste0("Base de données assemblée et nettoyée.\n",
               nb_sej4, " séjours identifiés entre ", minyear, " et ", maxyear, ", concernant ", nb_pat, " patients.\n"))
  }
  return(df_final)
  
}

request_wip <- function(minyear, maxyear, diaglist, varlist) {
  
  cat("\f")
  
  yearlist <- sprintf('%02d', minyear:maxyear %% 100)
  
  diagsqlrequest <- paste(paste(c("DGN_PAL LIKE ('",rep(c("') OR DGN_PAL LIKE ('"), times=length(diaglist)-1)),diaglist, collapse=''),"')", collapse='')
  diagsqlrequest <- gsub("' ","'",diagsqlrequest)
  diagsqlrequest <- gsub(" '","'",diagsqlrequest)
  
  for (i in 1:length(yearlist)) {
    year <- minyear-1+i
    
    # Certaines tables n'apparaissent que plus tard, et d'autre changent de structure au fil du temps.
    # Lorsqu'une table n'existe pas encore, je ne créé qu'une liste vide qui possède tout de même les éléments architecturaux auxquels je peut être amené à faire référence plus tard, pour éviter que la fonction ne plante.
    tables = list("MCO_A" = tbl(conn, paste0("T_MCO",yearlist[i],"A")),
                  "MCO_B" = tbl(conn, paste0("T_MCO",yearlist[i],"B")),
                  "MCO_C" = tbl(conn, paste0("T_MCO",yearlist[i],"C")),
                  "MCO_D" = tbl(conn, paste0("T_MCO",yearlist[i],"D")),
                  "MCO_E" = tbl(conn, paste0("T_MCO",yearlist[i],"E")),
                  "MCO_UM" = list("ops" = list("vars"="")),
                  "MCO_FBSTC" = list("ops" = list("vars"="")),
                  "MCO_FMSTC" = list("ops" = list("vars"="")),
                  "ER_PRS_F" = list("ops" = list("vars"="")),
                  "IR_BEN_R" <- tbl(conn, paste0("IR_BEN_R")),
                  "IR_IMB_R" <- tbl(conn, paste0("IR_IMB_R")))
    
    # En 2006 apparaît la table MCO_UM, qui ne change pas de structure par la suite
    if (year>=2006) {
      tables$MCO_UM <- tbl(conn, paste0("T_MCO",yearlist[i],"UM"))
    }
    
    # La table MCO FMSTC apparaît en 2007
    if (year>=2007) {
      tables$MCO_FMSTC <- tbl(conn, paste0("T_MCO",yearlist[i],"FMSTC"))
    }
    
    # La table MCO FBSTC apparaît en 2008
    if (year>=2008) {
      tables$MCO_FBSTC <- tbl(conn, paste0("T_MCO",yearlist[i],"FBSTC"))
    }
    
    # La table ER_PRS_F apparaît en 2006, et change de structure en 2013
    if (year>=2006 & year<=2012) {
      tables$ER_PRS_F <- tbl(conn, paste0("ER_PRS_F_",yearlist[i]))
    } else if (year>=2013) {
      tables$ER_PRS_F <- tbl(conn, paste0("ER_PRS_F"))
    }
    
    # Listes des variables obligatoires.
    # Si l'utilisateur ne les a pas mentionnées dans la liste des variables d'intérêt, elles y sont rajoutées d'office.
    # Correspond aux clés de jointures, ainsi qu'aux variables permettant d'opérer un nettoyage de la base de données.
    mandatory_var <- list("MCO_A" = c("ETA_NUM", "RSA_NUM"),
                          "MCO_B" = c("ETA_NUM", "RSA_NUM"),
                          "MCO_C" = c("ETA_NUM", "RSA_NUM", "NIR_ANO_17", "NIR_RET", "NAI_RET", "SEX_RET", "SEJ_RET", "FHO_RET", "PMS_RET", "DAT_RET", "COH_NAI_RET", "COH_SEX_RET"),
                          "MCO_D" = c("ETA_NUM", "RSA_NUM"),
                          "MCO_E" = c("ETA_NUM", "ETB_EXE_FIN"),
                          "MCO_UM" = c("ETA_NUM", "RSA_NUM"),
                          "MCO_FBSTC" = c("ETA_NUM", "SEQ_NUM"),
                          "MCO_FMSTC" = c("ETA_NUM", "SEQ_NUM"),
                          "ER_PRS_F" = c("BEN_NIR_PSA"),
                          "IR_BEN_R" = c("BEN_NIR_PSA"),
                          "IR_IMB_R" = c("BEN_NIR_PSA"))
    
    # Creation de la liste finale des variables
    varlist_final <- varlist
    for (j in 1:length(tables)) {
      if (varlist[[j]][1]=="ALL") {
        varlist_final[[j]] <- tables[[j]]$ops$vars
      } else if(varlist[[j]][1]=="NONE") {
        varlist_final[[j]] <- 0
      } else {
        varlist_final[[j]] <- unique(append(varlist[[j]],mandatory_var[[j]]))
        varlist_final[[j]] <- varlist_final[[j]][varlist_final[[j]] %in% tables[[j]]$ops$vars]
      }
    }
    
    
    cat(paste0("Chaînage en cours pour l'année ", year,".\n"))
    
    
    # Identification des séjours concernés
    df_temp <-
      tables$MCO_B %>%
      filter(sql(diagsqlrequest)) %>%
      select(varlist_final$B)
    
    
    # Jointures successives à la table MCO B (ou table intermédiaire)
    for (j in length(tables)) {
      if (tables[[j]][["ops"]][["vars"]][1]!=0) {
        if (j %in% c(1,3,4,6)) { # Jointures sur la base de RSA_NUM & ETA_NUM
          df_temp <- df_temp %>%
                     left_join(tables[[j]] %>%
                               select(varlist_final[[j]]),
                     by = c("ETA_NUM", "RSA_NUM"),
                     suffix = c("_B", "_UM")) # Ne concernera que la jointure avec la table MCO UM qui ont des diagnostics principaux (DGN_PAL) différents
        } else if (j == 5) { # Jointure uniquement sur la base de ETA_NUM
          df_temp <- df_temp %>%
                     left_join(tables[[j]]%>%
                               filter(sql(diagsqlrequest)) %>%
                               select(varlist_final[[j]]),
                     by = c("ETA_NUM"))
        } else if (j %in% c(7,8)) { # Jointure sur base de ETA_NUM & RSA_NUM = SEQ_NUM
          df_temp <- df_temp %>%
                     left_join(tables[[j]]%>%
                               filter(sql(diagsqlrequest)) %>%
                               select(varlist_final[[j]]),
                     by = c("ETA_NUM", "NIR_ANO_17" = "BEN_NIR_PSA"))
        } else if (j %in% c(9:11)) { # Jointure sur base de NIR_ANO_17 = BEN_NIR_PSA
          df_temp <- df_temp %>%
                     left_join(tables[[j]]%>%
                               filter(sql(diagsqlrequest)) %>%
                               select(varlist_final[[j]]),
                     by = c("NIR_ANO_17" = "BEN_NIR_PSA"))
        }
      }
      
    }
    # On a maintenant la commande complète à requêter à la base de données hébergée par Oracle
    
    df_temp <- collect(df_temp)
    
    if (i==1) {
      df_final <- df_temp
    } else {
      df_final <- bind_rows(df_final, df_temp)
    }
    rm(df_temp)
    
    cat(paste0("Chaînage ", year, " OK.\n"))
    
  }
  
  df_final$sejour_id <- group_indices(df_final, ETA_NUM, RSA_NUM)
  nb_sej1 <- length(unique(df_final$sejour_id))
  
  
  cat(paste0("\n",
             "Base de données assemblée.\n",
             nb_sej1, " séjours identifiés.\n",
             "Nettoyage en cours.\n"))
  
  # Liste des établissements à écarter
  # Jusqu'en 2017, certains établissements ont remonté leurs données en doublon.
  # La liste ci-dessous les recense, afin de pouvoir par la suite éliminer les données dupliquées.
  doublelist <- c("130780521", "130783236", "130783293", "130784234", "130804297",
                  "600100101", "750041543", "750100018", "750100042", "750100075",
                  "750100083", "750100091", "750100109", "750100125", "750100166",
                  "750100208", "750100216", "750100232", "750100273", "750100299",
                  "750801441", "750803447", "750803454", "910100015", "910100023",
                  "920100013", "920100021", "920100039", "920100047", "920100054",
                  "920100062", "930100011", "930100037", "930100045", "940100027",
                  "940100035", "940100043", "940100050", "940100068", "950100016",
                  "690783154", "690784137", "690784152", "690784178", "690787478",
                  "830100558")
  
  df_final <-
    df_final %>%
    filter(!ETB_EXE_FIN %in% doublelist)
  nb_sej2 <- length(unique(df_final$sejour_id))
  cat(paste0(nb_sej1-nb_sej2, " doublons éliminés.\n"))
  
  df_final <-
    df_final %>%
    subset(!grepl("^90",df_final$GRG_GHM) | SOR_MOD==9)
  # subset(!grepl("^90",df_final$GRG_GHM))
  # Cette ligne de code alternative ne "sauve" pas dans la base de données les personnnes décédées.
  # Le projet initial ayant justifié la création de cette requête s'intéressant à la gravité d'évènements médicaux (mortalité incluse),
  # les patients décédés étaient considérés comme n'étant pas en "erreur" mais bien porteurs d'une information utile pour le projet.
  
  nb_sej3 <- length(unique(df_final$sejour_id))
  cat(paste0(nb_sej2-nb_sej3, " séjours 'erreurs' éliminés.\n"))
  
  if (maxyear>=2013) {
    df_final <-
      df_final %>%
      subset(NIR_RET==0 & NAI_RET==0 & SEX_RET==0 & SEJ_RET==0 & FHO_RET==0 & PMS_RET==0 & DAT_RET==0 & COH_NAI_RET==0 & COH_SEX_RET==0)
    nb_sej4 <- length(unique(df_final$sejour_id))
    cat(paste0(nb_sej3-nb_sej4, " séjours avec incohérences éliminés.\n", "\n"))
  } else {
    nb_sej4 <- nb_sej3
    cat(paste0("Aucune donnée sur les incohérences. Aucun séjour éliminé sur cette base.\n", "\n"))
  }
  
  
  nb_pat <- length(unique(df_final$NIR_ANO_17))
  if (length(yearlist)==1) {
    cat(paste0("Base de données assemblée et nettoyée.\n",
               nb_sej4, " séjours identifiés sur l'année ", year, ", concernant ", nb_pat, " patients.\n"))
  } else {
    cat(paste0("Base de données assemblée et nettoyée.\n",
               nb_sej4, " séjours identifiés entre ", minyear, " et ", maxyear, ", concernant ", nb_pat, " patients.\n"))
  }
  return(df_final)
  
}

##### PARAMETRAGE DE LA REQUETE #####

# Liste des diagnostics d'intérêt
# N.B. : Cette liste sera comparée à la variable "diagnostic principal" (DGN_PAL) présente dans les tables B et UM.
#        Vous pouvez n'indiquer que le début du code ICM 10
# Si vous voulez requêter tous les codes commençant par "I60", vous pouvez écrire "I60%"
diaglist <- c("I60%", "I61%", "I62%", "I63%", "I64%")

                                                
                                                
# Liste des variables d'intérêt dans chacune des tables PMSI MCO
# N.B. 1 : Lister entre guillemets les noms de variables d'intérêt.
#          Indiquez "ALL" dans le vecteur si toutes les variables vous intéressent
#          Iniquez "NONE" dans le vecteur si vous ne souhaitez pas effectuer la jointure avec cette table.
# N.B. 2 : Certaines variables ont été introduites après la création du PMSI MCO, quand d'autres ont changé de nom, on été scindées en 2 nouvelles variables, etc.
#          Par exemple, dans la table T_MCOxxUM, UM_TYP est introduite en 2006, puis scindée en AUT_TYP1_UM et AUT_TYP2_UM en 2010.
#          Si des variables qui vous intéressent ont changé de nom au cours du temps, pensez à bien lister toutes leurs appellations successives pour la période couverte par votre requête.
varlist <- list(
  "MCO_A" = c("NONE"),
  "MCO_B" = c("BDI_DEP", "AGE_ANN", "COD_SEX", "DGN_PAL", "GRG_GHM", "SOR_MOD", "ETA_NUM", "RSA_NUM"),
  "MCO_C" = c("NIR_ANO_17", "NIR_RET", "NAI_RET", "SEX_RET", "SEJ_RET", "FHO_RET", "PMS_RET", "DAT_RET", "COH_NAI_RET", "COH_SEX_RET", "ETA_NUM", "RSA_NUM"),
  "MCO_D" = c("ASS_DGN", "ETA_NUM", "RSA_NUM", "VAR1", "VAR2"),
  "MCO_E" = c("ETB_EXE_FIN", "ETA_NUM"),
  "MCO_UM" = c("DGN_PAL", "ETA_NUM", "RSA_NUM", "UM_TYP", "AUT_TYP1_UM"),
  "MCO_FBSTC" = c("NONE"),
  "MCO_FMSTC" = c("NONE"),
  "ER_PRS_F" = c("NONE"),
  "IR_BEN_R" = c("NONE"),
  "IR_IMB_R" = c("NONE")
)


# Lancement de la requête
df <- request_safe(2014,2014,diaglist,varlist)
df <- request_wip(2013,2014,diaglist,varlist)


