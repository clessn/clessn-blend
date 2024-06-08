###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             
#                                                                             
#                                  e_radar+                        
#                                                                             
# This extractor extracts data from 13 medias the most interesting for the 
# CLESSN 
# - First it extracts the html page from the headline (home) page of each media
#   and stores the html as a file in the CLESSN data lake
# - Second, it takes all the urls from the articles contained in each page
#   and then stores them in the dtaalake also
#                                                                             
###############################################################################


###############################################################################
########################      Functions and Globals      ######################
###############################################################################

find_headline <- function(r, m){
  clessnverse::logit(scriptname, paste("[find_headline]", "Finding headline for", m$short_name), logger)
  found_supported_media <- FALSE

  if (m$short_name == "RCI") {
    RCI_extracted_headline <- r %>% rvest::html_nodes(xpath = '//*[@class="item--1"]') %>% rvest::html_nodes("a") %>% rvest::html_attr("href")

    if(length(RCI_extracted_headline) == 0){
      RCI_extracted_headline <- r %>% rvest::html_nodes(xpath = '//*[contains(concat(" ", @class, "="), "MainAsideGrid")]') %>% rvest::html_nodes("a") %>% rvest::html_attr("href")
      RCI_extracted_headline <- RCI_extracted_headline[1]
    }

    if (length(RCI_extracted_headline) == 0 || is.na(RCI_extracted_headline)) {
      spans_with_ala <- r %>%
        rvest::html_nodes("span") %>%
        .[grepl("À la", rvest::html_text(.))]

      links <- lapply(spans_with_ala, function(span) {
        span %>%
        rvest::html_node(xpath = "ancestor::header/following-sibling::*//a[@href]") %>%
        rvest::html_attr("href")

      })

      # get the first that is not NA
      target_url <- links[sapply(links, Negate(is.na))][1]
      target_url <- target_url[[1]]

      if (length(target_url) > 0) {
        RCI_extracted_headline <- target_url
      }
    }

    if(length(RCI_extracted_headline) > 0){
      if (grepl("^http.*", RCI_extracted_headline[[1]])) {
        url <- RCI_extracted_headline[[1]]
      } else {
        url <- paste(m$base, RCI_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if (m$short_name == "JDM") {
    JDM_extracted_headline <- r %>%
      rvest::html_elements(xpath = "//a") %>% 
      rvest::html_element("span") %>% 
      rvest::html_attr("data-story-url") %>%
      na.omit()


    if(length(JDM_extracted_headline) > 0){
      url <- JDM_extracted_headline[[1]]
      found_supported_media <- TRUE
    }
  }

  if (m$short_name == "CBC") {
    CBC_extracted_headline <<- r %>%
        rvest::html_nodes(xpath = '//*[contains(concat(" ", @class, "="), "card cardFeatured cardFeaturedReversed")]') %>%
        rvest::html_nodes('a') %>%
        rvest::html_attr("href")

    if(length(CBC_extracted_headline) == 0){
      clessnverse::logit(scriptname, paste("[find_headline]", "CBC: Scraping with card cardFeatured cardFeaturedReversed sclt-featurednewsprimarytopstoriescontentlistcard0 failed, trying with primaryHeadline desktopHeadline"), logger)
      CBC_extracted_headline <<- r %>%
        rvest::html_nodes(xpath = '//*[@class="primaryHeadline desktopHeadline"]') %>%
        rvest::html_nodes('a') %>%
        rvest::html_attr("href")
    }

    if(length(CBC_extracted_headline) > 0){
      if (grepl("^http.*", CBC_extracted_headline[[1]])) {
        url <- CBC_extracted_headline[[1]]
      } else {
        url <- paste(m$base, CBC_extracted_headline[[1]], sep="")
      } 
      found_supported_media <- TRUE
    }
    
  }
  
  if(m$short_name == "NP"){
    NP_extracted_headline <<- r %>%
      rvest::html_nodes(xpath = '//*[contains(concat(" ", @class, "="), "hero-feed__hero-col")]') %>%
      rvest::html_nodes(xpath = '//a[@class="article-card__link"]') %>%
      rvest::html_attr("href")

    if(length(NP_extracted_headline) > 0){
      if (grepl("^http.*", NP_extracted_headline[[1]])) {
        url <- NP_extracted_headline[[1]]
      } else {
        url <- paste(m$base, NP_extracted_headline[[1]], sep="")
      } 
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "TVA"){
    TVA_extracted_headline <- r %>% rvest::html_nodes(xpath = '//*[@class="home-top-story"]') %>% rvest::html_nodes(xpath = '//*[@class="news_unit-link"]') %>% rvest::html_attr("href")
  
    if(length(TVA_extracted_headline) > 0){
      if (grepl("^http.*", TVA_extracted_headline[[1]])) {
        url <- TVA_extracted_headline[[1]]
      } else {
        url <- paste(m$base, TVA_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "GAM"){
    GAM_extracted_headline <- r %>% 
      #rvest::html_nodes(xpath = '//div[@class="Container__StyledContainer-sc-15gjlsr-0 hWPJHz Grid__StyledGrid-sc-140zq2o-0 fRHjkJ TopPackageBigStory__StyledWrapper-sc-1gza93-1 zUdgM big-story-content"]') %>%
      #rvest::html_nodes(xpath = '//a[@class="CardLink__StyledCardLink-sc-2nzf9p-0 fowrAa TopPackageBigStory__StyledCardLink-sc-1gza93-3 jSWywI top-package-premium-media"]') %>%
      rvest::html_nodes(xpath = '//div[contains(@class,"TopPackageBigStory__StyledTopPackageBigStory")]') %>%
      rvest::html_children() %>%
      rvest::html_nodes(xpath = './a[contains(@class, "top-package-premium-media")]') %>%
      rvest::html_attr("href")

    if (length(GAM_extracted_headline) == 0) {  
      GAM_extracted_headline <- r %>% 
        rvest::html_nodes(xpath = '//div[contains(@class,"TopPackageBigStory__StyledTopPackageBigStory")]') %>%
        rvest::html_nodes(xpath = './a[contains(@class, "CardLink__StyledCardLink")]') %>%
        rvest::html_attr("href")
    }

    if (length(GAM_extracted_headline) == 0) {  
      GAM_extracted_headline <- r %>% 
        rvest::html_nodes(xpath = '//div[contains(@class,"LayoutTopPackageCard__StyledContainer")]') %>%
        rvest::html_nodes(xpath = './a[contains(@class, "CardLink__StyledCardLink")]') %>%
        rvest::html_attr("href")

      GAM_extracted_headline <- GAM_extracted_headline[1]
    }


    if(length(GAM_extracted_headline) > 0){
      headlineIndex <- 1

      if(grepl("/podcasts/the-decibel/", GAM_extracted_headline[[headlineIndex]])){
        headlineIndex <- headlineIndex + 1
      }

      if (grepl("^http.*", GAM_extracted_headline[[headlineIndex]])) {
        url <- GAM_extracted_headline[[headlineIndex]]
      } else {
        url <- paste(m$base, GAM_extracted_headline[[headlineIndex]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "VS"){
    VS_extracted_headline <- r %>% 
      rvest::html_nodes(xpath = '//div[@class="article-card__details"]') %>%
      rvest::html_nodes(xpath = '//a[@class="article-card__link"]') %>%
      rvest::html_attr("href")

    if(length(VS_extracted_headline) == 0){
      clessnverse::logit(scriptname, paste("[find_headline]", "VS: scraping with article-card__link failed, trying with article-card__image-link"), logger)
      VS_extracted_headline <- r %>% 
        rvest::html_nodes(xpath = '//div[@class="article-card__details"]') %>%
        rvest::html_nodes(xpath = './a[@class="article-card__image-link"]') %>%
        rvest::html_attr("href")
    }

    if(length(VS_extracted_headline) > 0){
      if (grepl("^http.*", VS_extracted_headline[[1]])) {
        url <- VS_extracted_headline[[1]]
      } else {
        url <- paste(m$base, VS_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "LAP"){
    LAP_extracted_headline <- r %>%
      rvest::html_nodes(xpath = '//div[@class="homeHeadlinesRow__main"]') %>%
      rvest::html_nodes(xpath = '//article[@data-position="1"]') %>%
      rvest::html_nodes(xpath = '//a[@class="storyCard__cover homeHeadlinesCard__cover"]') %>%
      rvest::html_attr("href")

    if(length(LAP_extracted_headline) == 0){
      LAP_extracted_headline <- r %>%
        rvest::html_nodes(xpath = '//div[@class="homeHeadlinesRow__main"]') %>%
        rvest::html_nodes(xpath = '//article[@data-position="1"]') %>%
        rvest::html_nodes(xpath = '//a[@class="storyCard__cover"]') %>%
        rvest::html_attr("href")
    }

    if(length(LAP_extracted_headline) > 0){
      if (grepl("^http.*", LAP_extracted_headline[[1]])) {
        url <- LAP_extracted_headline[[1]]
      } else {
        url <- paste(m$base, LAP_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "LED"){
    LED_extracted_headline <- r %>%
      rvest::html_nodes(xpath = '//a[@class="card-click"]') %>%
      rvest::html_attr("href")

    if(length(LED_extracted_headline) > 0){
      if (grepl("^http.*", LED_extracted_headline[[1]])) {
        url <- LED_extracted_headline[[1]]
      } else {
        url <- paste(m$base, LED_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "MG"){
    MG_extracted_headline <- r %>%
      rvest::html_nodes(xpath = '//div[contains(concat(" ", @class, "="), "hero-feed__hero-col")]') %>%
      rvest::html_nodes(xpath = '//a[@class="article-card__link"]') %>%
      rvest::html_attr("href")

    if(length(MG_extracted_headline) > 0){
      if (grepl("^http.*", MG_extracted_headline[[1]])) {
        url <- MG_extracted_headline[[1]]
      } else {
        url <- paste(m$base, MG_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "CTV"){
    CTV_extracted_headline <- r %>%
      rvest::html_nodes("h3") %>%
      rvest::html_nodes("a") %>%
      rvest::html_attr("href")

    if(length(CTV_extracted_headline) == 0){
      clessnverse::logit(scriptname, paste("[find_headline]", "CTV: Initial attempt failed, trying thorugh xpaths."), logger)
      CTV_extracted_headline <- r %>%
        rvest::html_nodes(xpath = '//div[@class="c-list__item__block"]') %>%
        rvest::html_nodes(xpath = '//a[@class="c-list__item__image"]') %>%
        rvest::html_attr("href")
    }

    if(length(CTV_extracted_headline) > 0){
      if (grepl("^http.*", CTV_extracted_headline[[1]])) {
        url <- CTV_extracted_headline[[1]]
      } else {
        url <- paste(m$base, CTV_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "GN"){
    GN_extracted_headline <- r %>%
      rvest::html_nodes(xpath = '//a[@class="c-posts__headlineLink"]') %>%
      rvest::html_attr("href")

    if(length(GN_extracted_headline) == 0){  
      clessnverse::logit(scriptname, paste("[find_headline]", "a.c-posts__headlineLink failed, trying a.l-highImpact__headlineLink"), logger)
      GN_extracted_headline <- r %>%
        rvest::html_nodes(xpath = '//a[@class="l-highImpact__headlineLink"]') %>%
        rvest::html_attr("href")
    }

    if(length(GN_extracted_headline) == 0){
      clessnverse::logit(scriptname, paste("[find_headline]", "a.l-highImpact__headlineLink failed, trying div.l-section__main a.c-posts__inner"), logger)
      GN_extracted_headline <- r %>%
        rvest::html_nodes(xpath = '//div[@class="l-section__main"]') %>%
        rvest::html_nodes(xpath = '//a[@class="c-posts__inner"]') %>%
        rvest::html_attr("href")
    }

    if(length(GN_extracted_headline) > 0){
      if (grepl("^http.*", GN_extracted_headline[[1]])) {
        url <- GN_extracted_headline[[1]]
      } else {
        url <- paste(m$base, GN_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
    
  }

  if(m$short_name == "TTS"){
    TTS_extracted_headline <- r %>%
      #rvest::html_nodes(xpath = '//article[@class="tnt-asset-type-article packStory1 letterbox-style-white  tnt-section-news tnt-sub-section-world"]') %>%
      #rvest::html_nodes(xpath = '//article[contains(concat(" ", @class, "="), "packStory1")]')
      #rvest::html_nodes(xpath = '//a[contains(concat(" ", @class, "="), "tnt-headline")]') %>%
      rvest::html_nodes(xpath = './/article[contains(@class, "packStory1")]') %>%
      rvest::html_nodes("a") %>%
      rvest::html_attr("href")
    
    TTS_extracted_headline <- TTS_extracted_headline[1]
    
    if(length(TTS_extracted_headline) > 0){
      if (grepl("^http.*", TTS_extracted_headline[[1]])) {
        url <- TTS_extracted_headline[[1]]
      } else {
        url <- paste(m$base, TTS_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
  }


  # if(m$short_name == "NYT"){
  #   NYT_extracted_headline <- r %>%
  #     #rvest::html_nodes(xpath = '//article[@class="tnt-asset-type-article packStory1 letterbox-style-white  tnt-section-news tnt-sub-section-world"]') %>%
  #     #rvest::html_nodes(xpath = '//article[contains(concat(" ", @class, "="), "packStory1")]')
  #     #rvest::html_nodes(xpath = '//a[contains(concat(" ", @class, "="), "tnt-headline")]') %>%
  #     rvest::html_nodes(xpath = '//main[@id="site-content"]') %>%
  #     rvest::html_nodes("a") %>%
  #     rvest::html_attr("href")
    
  #   NYT_extracted_headline <- NYT_extracted_headline[1]
    
  #   if(length(NYT_extracted_headline) > 0){
  #     if (grepl("^http.*", NYT_extracted_headline[[1]])) {
  #       url <- NYT_extracted_headline[[1]]
  #     } else {
  #       url <- paste(m$base, NYT_extracted_headline[[1]], sep="")
  #     }
  #     found_supported_media <- TRUE
  #   }
  # }
  
  if (!found_supported_media) {
    clessnverse::logit(scriptname, paste("[find_headline]", "no supported media found for", m$short_name), logger)
    warning(paste("[find_headline]", "no supported media found for", m$short_name))
    return("")
  }

  return(url)
}





get_content <- function(url, r, m){
  if(m$short_name == "CBC"){
    article_content <<- r %>%
      #rvest::html_nodes(xpath = '//main[@class="feed-content content"]') %>%
      rvest::html_nodes(xpath = '//div[contains(@class, "BodyHtmlForMainContent")]') %>%
      rvest::html_children() %>%
      #rvest::html_nodes(":not(.formattedDate):not(.video-time-stamp):not(.timestamp):not(.byline):not(.bylineDetails):not(.video-time-container)") %>%
      rvest::html_text()

    if (!is.null(article_content)) article_content <- paste(article_content, collapse = "\n")

    if(!is.null(article_content) && length(article_content) > 0){
      return(article_content[[1]])
    } else {
      msg <- paste("[get_content]", m$short_name, ": Empty content on url", url)
      clessnverse::logit(scriptname, msg, logger)
      warning(msg)
    }
  }

  if(m$short_name == "JDM"){
    article_content <<- r %>%
      rvest::html_nodes(xpath = '//article[@class="article-container"]') %>%
      rvest::html_children() %>%
      rvest::html_nodes(":not(.bigbox-container):not(.before-comments):not(#paywallArticleOffer)") %>%
      rvest::html_text()

    if(length(article_content) > 0){
      return(article_content[[1]])
    } else {
      article_content <<- r %>%
      rvest::html_nodes(xpath = '//div[@class="article-main-txt composer-content"]') %>%
      rvest::html_children() %>%
      rvest::html_nodes(":not(.bigbox-container):not(.before-comments):not(#paywallArticleOffer)") %>%
      rvest::html_text()

      if(length(article_content) > 0){
        return(article_content[[1]])
      } else {
        msg <- paste("[get_content]", m$short_name, ": Empty content on url", url)
        clessnverse::logit(scriptname, msg, logger)
        warning(msg)
      }
    }
  }

  if(m$short_name == "RCI"){
    # article_content <<- r %>%
    #   rvest::html_nodes(xpath = '//section[@class="redactionals document-content-style"]') %>%
    #   rvest::html_children() %>%
    #   rvest::html_nodes(":not(.sc-1b0qtbq-0):not(.eDaVCP):not(.styled__AdAttachmentWrapperNoPrint)") %>%
    #   rvest::html_text()

    # if(length(article_content) == 0){
    #   article_content <<- r %>%
    #     rvest::html_nodes(xpath = '//section[@class="sc-3sri0n-1 dIVQob"]') %>%
    #     rvest::html_children() %>%
    #     rvest::html_nodes(":not(.sc-1b0qtbq-0):not(.eDaVCP):not(.styled__AdAttachmentWrapperNoPrint)") %>%
    #     rvest::html_text()
    # }

    article_content <<- r %>%
      #rvest::html_nodes(xpath = '//main[@class="feed-content content"]') %>%
      rvest::html_nodes(xpath = '//div[contains(@class, "BodyHtmlForMainContent")]') %>%
      rvest::html_children() %>%
      #rvest::html_nodes(":not(.formattedDate):not(.video-time-stamp):not(.timestamp):not(.byline):not(.bylineDetails):not(.video-time-container)") %>%
      rvest::html_text()

    if (!is.null(article_content) && length(article_content) > 0) article_content <- paste(article_content, collapse = "\n")


    if(!is.null(article_content) && length(article_content) > 0){
      return(article_content[[1]])
    } else {
      msg <- paste("[get_content]", m$short_name, ": Empty content on url", url)
      clessnverse::logit(scriptname, msg, logger)
      warning(msg)
    }
  }

  if(m$short_name == "NP"){
    # article_content <<- r %>%
    #   rvest::html_nodes(xpath = '//article[@class="article-content-story article-content-story--story"]') %>%
    #   rvest::html_children() %>%
    #   rvest::html_nodes(":not(.ad__section-border):not(.article-content__ad-group):not(.ad_counter_2)") %>%
    #   rvest::html_text()

    article_content <<- r %>%
      rvest::html_nodes(xpath = '//section[@class="article-content__content-group article-content__content-group--feature"]') %>%
      rvest::html_children() %>%
      #rvest::html_nodes(":not(.ad__section-border):not(.article-content__ad-group):not(.ad_counter_2)") %>%
      rvest::html_text()


    if (length(article_content) == 0) {
      article_content <<- r %>%
        rvest::html_nodes(xpath = '//section[@class="article-content__content-group article-content__content-group--story"]') %>%
        rvest::html_children() %>%
        #rvest::html_nodes(":not(.ad__section-border):not(.article-content__ad-group):not(.ad_counter_2)") %>%
        rvest::html_text()
    }


    if (!is.null(article_content) && length(article_content) > 0) {
      article_content <- paste(article_content, collapse = "\n")
    }

    if(!is.null(article_content) && length(article_content) > 0){
      return(article_content[[1]])
    } else {
      msg <- paste("[get_content]", m$short_name, ": Empty content on url", url)
      clessnverse::logit(scriptname, msg, logger)
      warning(msg)
    }
  }

  if(m$short_name == "TVA"){
    article_content <<- r %>%
      rvest::html_nodes(xpath = '//div[@class="row story-row"]') %>%
      rvest::html_children() %>%
      rvest::html_nodes(":not(.bigbox-container):not(.article):not(.dfp-container)") %>%
      rvest::html_text()

    if(length(article_content) > 0){
      return(article_content[[1]])
    } else {
      msg <- paste("[get_content]", m$short_name, ": Empty content on url", url)
      clessnverse::logit(scriptname, msg, logger)
      warning(msg)
    }
  }

  if(m$short_name == "GAM"){
    article_content <<- r %>%
      rvest::html_nodes(xpath = '//article[@class="l-article"]') %>%
      rvest::html_text()

    if(length(article_content) > 0){
      return(article_content[[1]])
    } else {
      msg <- paste("[get_content]", m$short_name, ": Empty content on url", url)
      clessnverse::logit(scriptname, msg, logger)
      warning(msg)
    }
  }

  if(m$short_name == "VS"){
    article_content <<- r %>%
      rvest::html_nodes(xpath = '//article[@class="article-content-story article-content-story--story"]') %>%
      rvest::html_children() %>%
      rvest::html_nodes(xpath = '//section[@class="article-content__content-group article-content__content-group--story"]') %>%
      rvest::html_text()

    if(length(article_content) > 0){
      return(article_content[[1]])
    } else {
      msg <- paste("[get_content]", m$short_name, ": Empty content on url", url)
      clessnverse::logit(scriptname, msg, logger)
      warning(msg)
    }
  }

  if(m$short_name == "LAP"){
    article_content <<- r %>%
      rvest::html_nodes(xpath = '//div[@class="articleBody"]') %>%
      rvest::html_children() %>%
      rvest::html_nodes(":not(.adsWrapper):not(.greyLineToppedBox)") %>%
      rvest::html_text()

    if(length(article_content) > 0){
      return(article_content[[1]])
    } else {
      msg <- paste("[get_content]", m$short_name, ": Empty content on url", url)
      clessnverse::logit(scriptname, msg, logger)
      warning(msg)
    }
  }

  if(m$short_name == "LED"){
    article_content <<- r %>%
      rvest::html_nodes(xpath = '//div[@class="editor scrolling-tracker"]') %>%
      rvest::html_children() %>%
      rvest::html_nodes(":not(.hidden-print):not(.read-also)") %>%
      rvest::html_text()

    if(length(article_content) > 0){
      return(article_content[[1]])
    } else {
      msg <- paste("[get_content]", m$short_name, ": Empty content on url", url)
      clessnverse::logit(scriptname, msg, logger)
      warning(msg)
    }
  }

  if(m$short_name == "MG"){
    article_content <<- r %>%
      rvest::html_nodes(xpath = '//article[@class="article-content-story article-content-story--story"]') %>%
      rvest::html_children() %>%
      rvest::html_nodes(xpath = '//section[@class="article-content__content-group article-content__content-group--story"]') %>%
      rvest::html_text()

    if(length(article_content) > 0){
      return(article_content[[1]])
    } else {
      msg <- paste("[get_content]", m$short_name, ": Empty content on url", url)
      clessnverse::logit(scriptname, msg, logger)
      warning(msg)
    }
  }

  if(m$short_name == "CTV"){
    article_content <<- r %>%
      rvest::html_nodes(xpath = '//div[@class="c-text"]') %>%
      rvest::html_text()

    if(length(article_content) > 0){
      return(article_content[[1]])
    } else {
      msg <- paste("[get_content]", m$short_name, ": Empty content on url", url)
      clessnverse::logit(scriptname, msg, logger)
      warning(msg)
    }
  }

  if(m$short_name == "GN"){
    article_content <<- r %>%
      rvest::html_nodes(xpath = '//article[@class="l-article__text js-story-text"]') %>%
      rvest::html_children() %>%
      rvest::html_nodes(':not(.c-ad):not(.c-ad--bigbox):not(.l-article__ad)') %>%
      rvest::html_text()

    if(length(article_content) > 0){
      return(article_content[[1]])
    } else {
      msg <- paste("[get_content]", m$short_name, ": Empty content on url", url)
      clessnverse::logit(scriptname, msg, logger)
      warning(msg)
    }
  }

  if(m$short_name == "TTS"){
    article_content <<- r %>%
      #rvest::html_nodes(xpath = '//div[@class="c-article-body__content"]') %>%
      rvest::html_nodes(xpath = '//div[@id="article-body"]') %>%
      rvest::html_children() %>%
      rvest::html_text()

    if (!is.null(article_content) && length(article_content) > 1) {
      article_content <- gsub("\n(\\s*)", "", article_content) %>%
        trimws()
    }

    if (!is.null(article_content) && length(article_content) > 1) {
      article_content <- article_content[-c(1:2)] %>%
        head(., -4) %>%
        paste(., collapse = "/n")
    }

  # if(m$short_name == "NYT"){
  #   article_content <<- r %>%
  #     #rvest::html_nodes(xpath = '//div[@class="c-article-body__content"]') %>%
  #     rvest::html_nodes(xpath = '//script') 

  #   grepl("articleBody", rvest::html_text(article_content))

  #   if (!is.null(article_content) && length(article_content) > 1) {
  #     article_content <- gsub("\n(\\s*)", "", article_content) %>%
  #       trimws()
  #   }

  #   if (!is.null(article_content) && length(article_content) > 1) {
  #     article_content <- article_content[-c(1:2)] %>%
  #       head(., -4) %>%
  #       paste(., collapse = "/n")
  #   }      


  #   if(!is.null(article_content) && length(article_content) > 0){
  #     return(article_content[[1]])
  #   } else {
  #     msg <- paste("[get_content]", m$short_name, ": Empty content on url", url)
  #     clessnverse::logit(scriptname, msg, logger)
  #     warning(msg)
  #   }
   }

  return(list())
}

harvest_headline <- function(r, m, url, root_key, frontpage_root_key) {
  clessnverse::logit(scriptname, paste("[harvest_headline]", "getting headline from", url), logger)

  r <- rvest::session(url)


  metadata <- list(
    format = "",
    start_timestamp = Sys.time(),
    end_timestamp = Sys.time(),
    tags = paste("news,headline,radar+", m$short_name, m$long_name, sep=","),
    pillar = "radar+",
    source = url,
    media = m$short_name,
    description = "Headline page of the medias",
    object_type = "raw_data",
    source_type = "website",
    content_type = "news_headline",
    storage_class = "lake",
    country = m$country,
    schema = opt$schema,
    hashed_html = NA,
    frontpage_root_key = frontpage_root_key,
    duplicated_count = 1
  )

  if (r$response$status_code == 200) {
    if (grepl("text/html", r$response$headers$`content-type`)) {
      metadata$format <- "html"
      doc <- httr::content(r$response, as = 'text')
    }

    clessnverse::logit(scriptname, paste("[harvest_headline]", "pushing headline", url, "to hub"), logger)

    key <- gsub(" |-|:|/|\\.", "_", paste(root_key, Sys.time(), sep="_"))

    clessnverse::logit(scriptname, key, logger)

    hashed_html <- digest(doc, algo = "md5", serialize = F)

    content <- get_content(url, r, m)

    if(length(content) > 0){
      hashed_html <- digest(content, algo = "md5", serialize = T)
    }

    metadata$hashed_html <- hashed_html

    clessnverse::logit(scriptname, paste("[harvest_headline] about to handleDuplicate with", root_key, hashed_html), logger)
    pushed <- handleDuplicate("headline", root_key, doc, credentials, m$short_name, hashed_html)
    clessnverse::logit(scriptname, paste("[harvest_headline] handleDuplicate with", root_key, hashed_html, "completed"), logger)

    if(!pushed){
      clessnverse::logit(scriptname, paste("[harvest_headline] not pushed", root_key, hashed_html), logger)

      clessnverse::logit(scriptname, "[harvest_headline] appending", logger)
      pushed_headlines <<- append(pushed_headlines, key)
      clessnverse::logit(scriptname, paste("[harvest_headline] pushing", key), logger)
      pushed <- push_to_lake("headline", key, metadata, credentials, doc)
    }
    
    if(!pushed){
      warning(paste("[harvest_headline]", "error while pushing headline", key, "to datalake"))
    }
  } else {
      clessnverse::logit(scriptname, paste("[harvest_headline]", "there was an error getting url", url), logger)
      warning(paste("[harvest_headline]", "there was an error getting url", url))
  }

  clessnverse::logit(scriptname, "[harvest_headline] exitting function", logger)


} #</my_function>


form_root_key <- function(url){
  keyUrl <- url
  if(substr(keyUrl, nchar(keyUrl) - 1 + 1, nchar(keyUrl)) == '/'){
    keyUrl <- substr(keyUrl, 1, nchar(keyUrl) - 1)
  }
  key <- gsub(" |-|:|/|\\.", "_", paste(m$short_name, stringr::str_match(keyUrl, "[^/]+$"), sep="_"))
  return(key)
}


push_to_lake <- function(type, key, metadata, credentials, doc){
  pushed <- FALSE
  counter <- 0
  
  while(!pushed && counter < 20){
    if(counter > 0){
      Sys.sleep(20)
    }

    hub_response <- FALSE

    hub_response <- tryCatch(
      expr = {
        clessnverse::commit_lake_item(
          data = list(
            key = key,
            path = paste("radarplus", type, sep = "/"),
            item = doc
          ),
          metadata = metadata,
          mode = if (opt$refresh_data) "refresh" else "newonly",
          credentials
        )
      },
      error = function(e) {
        msg <- paste("[push_to_lake]", e$message, "during attempt", counter+1, "at pushing a", type, "to the datalake")
        clessnverse::logit(scriptname, msg, logger)
      },
      warning = function(w) {
        msg <- paste("[push_to_lake]", w$message, "during attempt", counter+1, "at pushing a", type, "to the datalake")
        clessnverse::logit(scriptname, msg, logger)
      },
      finally = {}
    )

    if ((!is.null(hub_response) && length(hub_response) > 0) && (hub_response == 0 || hub_response == TRUE)) {
      pushed <- TRUE
      clessnverse::logit(scriptname, paste("[push_to_lake]", "successfuly pushed", type, key, "to datalake"), logger)
      if(type == "headline"){
        nb_headline <<- nb_headline + 1
      } else {
        nb_frontpage <<- nb_frontpage + 1
      }
      return(TRUE)
    } else {
      clessnverse::logit(scriptname, paste("[push_to_lake]", "error while pushing", type, key, "to datalake"), logger)
      counter <- counter + 1
    }
  } # </while(!pushed && counter < 20)>


  return(FALSE)
}


handleDuplicate <- function(path, key, doc, credentials, mediaSource, identifiant){
  # data <- hublot::filter_lake_items(credentials, filter = filter)

  # retrieve_lake_item 
  counter <- 1
  success <- FALSE
  r <- NULL
  while (counter < 20 && !success) {
    if (counter > 1) Sys.sleep(20)
    r <- tryCatch(
      # we're looking for any lake item with a key for this media for today
      expr = {
        clessnverse::logit(scriptname, paste("[handleDuplicate]", "checking if datalake object already exists"), logger)
        hublot::filter_lake_items(
          credentials, 
          list(
            path = paste("radarplus", path, sep = "/"), 
            #key__contains = key
            #key__contains =  substr(key, 1, nchar(key) - 9)
            key__contains = gsub(" |-|:|/|\\.", "_", paste(key, Sys.Date(), sep="_"))
          )
        )
      },
      warning = function(w) {
        msg <- paste("[handleDuplicate]", "warning filtering lake items on attempt", counter, ":", w$message)
        clessnverse::logit(scriptname, msg, logger)
      },
      error = function(e) {
        msg <- paste("[handleDuplicate]", "error filtering lake items on attempt", counter, ":", e$message)
        clessnverse::logit(scriptname, msg, logger)
      },
      finally = {
      }
    )
    if (!is.null(r)) success <- TRUE else counter <- counter + 1
  } 

  if(length(r$result) == 0){
    clessnverse::logit(scriptname, paste("[handleDuplicate]", "No results found with the same key"), logger)
    return(FALSE)
  }

  # Only look for results with same schema
  start_index <- length(r$result)
  metadata_index <<- 7

  if(path == "frontpage"){
    sorted_result <- r$result[order(sapply(r$result, '[[', 4))]
  } else {
    sorted_result <- r$result[order(sapply(r$result, '[[', 3))]
  }
  
  repeat{
    lake_item <- sorted_result[[start_index]]

    if(lake_item$metadata$schema == opt$schema){
      break
    }

    start_index <- start_index - 1

    if(start_index == 0){
      clessnverse::logit(scriptname, paste("[handleDuplicate]", key, ": no element with same schema found"), logger)
      return(FALSE)
    }
  }

  in_lake_id <- "NOTHING"

  if(path == "frontpage") {
    in_lake_id <- lake_item$metadata$headline_root_key
  } else {
    in_lake_id <- lake_item$metadata$hashed_html
  }

  clessnverse::logit(scriptname, paste("[handleDuplicate]", "Scraped id:", identifiant), logger)
  clessnverse::logit(scriptname, paste("[handleDuplicate]", "In lake id:", in_lake_id), logger)

  same_id <- !is.null(in_lake_id) && identifiant == in_lake_id

  if(same_id){
    clessnverse::logit(scriptname, paste("[handleDuplicate]", "Duplicated. Modifying existing object"), logger)
    lake_item$metadata$end_timestamp <- Sys.time()
    if(is.na(lake_item$metadata$duplicated_count) || is.null(lake_item$metadata$duplicated_count)){
      lake_item$metadata$duplicated_count <- 1
    }
    lake_item$metadata$duplicated_count <- lake_item$metadata$duplicated_count + 1

    clessnverse::logit(scriptname, paste("[handleDuplicate] about to push", key), logger)
    pushed <- push_to_lake(type = path, key = lake_item[[4]], metadata = lake_item$metadata, credentials, doc = doc)
    clessnverse::logit(scriptname, paste("[handleDuplicate] pushed", key), logger)

    if(path == "frontpage"){
      duplicate_frontpages <<- duplicate_frontpages + 1
    } else {
      duplicate_headlines <<- duplicate_headlines + 1
      #pushed_headlines <<- append(pushed_headlines, paste("*", lake_item[[4]], " DUPLICATED", "*", sep=""))
      pushed_headlines <<- append(pushed_headlines, paste(lake_item[[4]], "*", sep = ""))
    }

    clessnverse::logit(scriptname, paste("[handleDuplicate] exitting function", key), logger)

    return(pushed)
  }

  clessnverse::logit(scriptname, paste("[handleDuplicate]", "Not duplicated. Upload new one."), logger)
  clessnverse::logit(scriptname, paste("[handleDuplicate] exitting function", key), logger)

  return(FALSE)
}





###############################################################################
########################               Main              ######################
###############################################################################

main <- function() {
    
  clessnverse::logit(scriptname, paste("[main]", "starting main function", logger))
  
  for (m in medias_urls) {
    url <- paste(m$base, m$front, sep="")

    metadata <- list(
      format = "",
      start_timestamp = Sys.time(),
      end_timestamp = Sys.time(),
      tags = paste("news,headline,radar+", m$short_name, m$long_name, sep=","),
      pillar = "radar+",
      source = url,
      media = m$short_name,
      description = "Headline page of the medias",
      object_type = "raw_data",
      source_type = "website",
      content_type = "news_headline",
      storage_class = "lake",
      country = m$country,
      schema = opt$schema,
      headline_root_key = NA,
      duplicated_count = 1
    )

    r <<- rvest::session(url)
    m <<- m

    if (r$response$status_code == 200) {
      
      clessnverse::logit(scriptname, paste("[main]", "collected frontpage from", url, "with code", r$response$status_code), logger)

      headline_url <- find_headline(r, m)

      if(headline_url == ""){
        clessnverse::logit(scriptname, paste("[main]", "No headline found for ", m$short_name, ", trying again at the end."), logger)
        failed_headlines <<- append(failed_headlines, list(m))
        next
      }

      if (grepl("text/html", r$response$headers$`content-type`)) {
        metadata$format <- "html"
        doc <- httr::content(r$response, as = 'text')
      }

      clessnverse::logit(scriptname, paste("[main]", "pushing frontpage", url, "to hub"), logger)

      root_key <- form_root_key(url)

      headline_key <- form_root_key(headline_url)

      metadata$headline_root_key <- headline_key
      
      clessnverse::logit(scriptname, paste("about to handle duplicates with", root_key, headline_key), logger)

      pushed <- handleDuplicate("frontpage", root_key, doc, credentials, m$short_name, headline_key)

      clessnverse::logit(scriptname, paste("duplicates handled for", root_key, headline_key), logger)

      if(!pushed){
        clessnverse::logit(scriptname, paste("data *NOT* pushed for", root_key, headline_key), logger)

        key <- gsub(" |-|:|/|\\.", "_", paste(root_key, Sys.time(), sep="_"))

        clessnverse::logit(scriptname, paste("[main]", key), logger)

        if (opt$refresh_data) mode <- "refresh" else mode <- "newonly"

        clessnverse::logit(scriptname, paste("pushing again with ", root_key, key), logger)

        pushed <- push_to_lake("frontpage", key, metadata, credentials, doc)
      }

      if(pushed){
          clessnverse::logit(scriptname, "data pushed", logger)

          harvest_headline(r, m, headline_url, headline_key, root_key)
      } else {
          failed_headlines <<- append(failed_headlines, list(m))
          warning(paste("error while pushing frontpage", key, "to datalake"))
      }

    } else {
       clessnverse::logit(scriptname, paste("[main]", "there was an error getting url", url), logger)
       failed_headlines <<- append(failed_headlines, list(m))
       warning(paste("there was an error getting url", url))
    }
  }#</for>

    
  repeat {
    if(length(failed_headlines) == 0){
      break
    }

    clessnverse::logit(scriptname, "Trying again to retrived failed headlines of the first pass", logger)
    Sys.sleep(30)

    minutes <- format(as.POSIXct(Sys.time()), format = "%M")
    minutes <- stringr::str_sub(minutes, 2, 2)

    #Hard coded, probably a much better way of doing this
    if(minutes == "8" || minutes == "0" || minutes == "9"){
      warning(paste("There are", length(failed_headlines), " failed headlines"))
      break
    }

    failed_headlines_copy <- list()

    for(m in failed_headlines){
      url <- paste(m$base, m$front, sep="")
      clessnverse::logit(scriptname, paste("getting frontpage from", url), logger)

      metadata <- list(
        format = "",
        start_timestamp = Sys.time(),
        end_timestamp = Sys.time(),
        tags = paste("news,headline,radar+", m$short_name, m$long_name, sep=","),
        pillar = "radar+",
        source = url,
        media = m$short_name,
        description = "Headline page of the medias",
        object_type = "raw_data",
        source_type = "website",
        content_type = "news_headline",
        storage_class = "lake",
        country = m$country,
        schema = opt$schema,
        headline_root_key = NA
      )

      r <<- rvest::session(url)
      m <<- m

      if (r$response$status_code == 200) {
        headline_url <- find_headline(r, m)

        if(headline_url == ""){
          clessnverse::logit(scriptname, paste("Still no headline found for ", m$short_name, "."))
          failed_headlines_copy <<- append(failed_headlines_copy, list(m))
          next
        }

        if (grepl("text/html", r$response$headers$`content-type`)) {
          metadata$format <- "html"
          doc <- httr::content(r$response, as = 'text')
        }

        clessnverse::logit(scriptname, paste("pushing headline", url, "to hub"), logger)

        key <- form_root_key(url)

        headline_key <- form_root_key(headline_url)

        metadata$headline_root_key <- headline_key
        
        clessnverse::logit(scriptname, paste("[main] about to handleDuplicate with", key, headline_key), logger)
        pushed <- handleDuplicate("frontpage", key, doc, credentials, m$short_name, headline_key)
        clessnverse::logit(scriptname, paste("[main] about to handleDuplicate with", key, headline_key), logger)


        if(!pushed){
          key <- gsub(" |-|:|/|\\.", "_", paste(key, Sys.time(), sep="_"))
          headline_key <- gsub(" |-|:|/|\\.", "_", paste(headline_key, Sys.time(), sep="_"))

          clessnverse::logit(scriptname, key, logger)

          if (opt$refresh_data) mode <- "refresh" else mode <- "newonly"

          pushed <- push_to_lake("frontpage", key, metadata, credentials, doc)
        }      

        if(pushed){
            harvest_headline(r, m, headline_url, headline_key, key)
        } else {
            failed_headlines_copy <<- append(failed_headlines_copy, list(m))
            warning(paste("error while pushing headline", key, "to datalake"))
        }

      } else {
        clessnverse::logit(scriptname, paste("there was an error getting url", url), logger)
        failed_headlines_copy <<- append(failed_headlines_copy, list(m))
        warning(paste("there was an error getting url", url))
      } #<\if (r$response$status_code == 200)>
    } # <\for(m in failed_headlines)>

    failed_headlines <- failed_headlines_copy
  } #<\repeat>
  
} # <\main>




###################################################
# Execution starting point

medias_urls <- list(
   cbcnews = list(
     long_name  = "CBC News",
     short_name = "CBC",
     country    = "CAN",
     base       = "https://www.cbc.ca",
     front      = "/news"
   ),
   jdm = list(
     long_name  = "Le Journal de Montréal",
     short_name = "JDM",
     country    = "CAN",
     base  = "https://www.journaldemontreal.com",
     front = "/"
   ),
   radiocan = list(
     long_name  = "Radio-Canada Info",
     short_name = "RCI",
     country    = "CAN",
     base  = "https://ici.radio-canada.ca",
     front = "/info"
   ),
  nationalPost = list(
    long_name  = "National Post",
    short_name = "NP",
    country    = "CAN",
    base  = "https://nationalpost.com",
    front = "/"
  ),
  tvaNouvelles = list(
    long_name  = "TVA Nouvelles",
    short_name = "TVA",
    country    = "CAN",
    base  = "https://www.tvanouvelles.ca",
    front = "/"
  ),
  globeAndMail = list(
    long_name  = "The Globe and Mail",
    short_name = "GAM",
    country    = "CAN",
    base  = "https://www.theglobeandmail.com",
    front = "/"
  ),
  vancouverSun = list(
    long_name  = "Vancouver Sun",
    short_name = "VS",
    country    = "CAN",
    base  = "https://vancouversun.com",
    front = "/"
  ),
  laPresse = list(
    long_name  = "La Presse",
    short_name = "LAP",
    country    = "CAN",
    base  = "https://www.lapresse.ca",
    front = "/"
  ),
  leDevoir = list(
    long_name  = "Le Devoir",
    short_name = "LED",
    country    = "CAN",
    base  = "https://www.ledevoir.com",
    front = "/"
  ),
  montrealGazette = list(
    long_name  = "Montreal Gazette",
    short_name = "MG",
    country    = "CAN",
    base  = "https://montrealgazette.com",
    front = "/"
  ),
  CTVNews = list(
    long_name  = "CTV News",
    short_name = "CTV",
    country    = "CAN",
    base  = "https://www.ctvnews.ca",
    front = "/"
  ),
  globalNews = list(
    long_name  = "Global News",
    short_name = "GN",
    country    = "CAN",
    base  = "https://globalnews.ca",
    front = "/"
  ),
  theStar = list(
    long_name  = "The Toronto Star",
    short_name = "TTS",
    country    = "CAN",
    base  = "https://www.thestar.com",
    front = "/"
  )#,
  # nyTimes = list(
  #   long_name = "The New York Times",
  #   short_name = "NYT",
  #   country = "USA",
  #   base = "https://www.nytimes.com",
  #   front = "/"
  # )
)

pushed_headlines <<- list()
failed_headlines <<- list()

duplicate_frontpages <<- 0
duplicate_headlines <<- 0



tryCatch( 
  withCallingHandlers(
  {
    library(dplyr)
    library(digest)

    status <<- 0
    final_message <<- ""
    nb_frontpage <<- 0
    nb_headline <<- 0

    if (!exists("scriptname")) scriptname <<- "e_radar+"

    # valid options for this script are
    #    log_output = c("file","console","hub")
    #    scrapind_method = c("range", "start_date", num_days, start_parliament, num_parliament) | "frontpage" (default)
    #    hub_mode = "refresh" | "skip" | "update"
    #    translate = "TRUE" | "FALSE"
    #    refresh_data = "TRUE" | "FALSE"
    #    
    #    you can use log_output = c("console") to debug your script if you want
    #    but set it to c("file") before putting in automated containerized production

    # opt <<- list(
    #   log_output = c("file", "console"),
    #   method = "frontpage",
    #   refresh_data = TRUE,
    #   translate = TRUE,
    #   schema = "prod"
    # )

    if (!exists("opt")) {
      opt <<- clessnverse::process_command_line_options()
    }

    if (!exists("logger") || is.null(logger) || logger == 0) {
      logger <<- clessnverse::log_init(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
    }

    # login to hublot
    clessnverse::logit(scriptname, "connecting to hub", logger)

    clessnverse::logit(scriptname, "OPT:", logger)
    clessnverse::logit(scriptname, opt, logger)

    # connect to hublot
    credentials <<- hublot::get_credentials(
      Sys.getenv("HUB3_URL"), 
      Sys.getenv("HUB3_USERNAME"), 
      Sys.getenv("HUB3_PASSWORD")
    )

    # or connect to hub2
    #clessnhub::login(
    #    Sys.getenv("HUB_USERNAME"),
    #    Sys.getenv("HUB_PASSWORD"),
    #    Sys.getenv("HUB_URL"))

    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)

    main()
  },

  warning = function(w) {
    clessnverse::logit(scriptname, w$message, logger)
    print(w)
    warnOutput <<- paste("\nWARNING: ", w$message)
    final_message <<- if (final_message == "") warnOutput else paste(final_message, "\n", warnOutput, sep="")    
    status <<- 2
  }),
    
  error = function(e) {
    clessnverse::logit(scriptname, e$message, logger)
    print(e)
    errorOutput <<- paste("\nERROR: ", e$message)
    final_message <<- if (final_message == "") errorOutput else paste(final_message, "\n", errorOutput, sep="")    
    status <<- 1
  },
  
  finally={
    # if status == 0, no errors happened. Add the breakdown of every media source to final message
    if(status == 0){
      for (pushed_headline in pushed_headlines) { 
       final_message <<- if (final_message == "") pushed_headline else paste(final_message, "\n", pushed_headline, sep="")  
      }
    }

    #final_message <<- if (final_message == "") paste("Exit Status = ", status, sep="") else paste(final_message, "\n", "Exit Status = ", status, sep="")

    clessnverse::logit(scriptname, final_message, logger)

    clessnverse::logit(scriptname, 
      paste(
        nb_frontpage, 
        "frontpages were extracted and",
        nb_headline,
        "headlines were extracted"
      ),
      logger
    )

    clessnverse::logit(scriptname, 
      paste(
        duplicate_frontpages, 
        "frontpages were duplicates and",
        duplicate_headlines,
        "headlines were duplicates"
      ),
      logger
    )

    

    clessnverse::logit(scriptname, paste("Execution of", scriptname, "program terminated"), logger)
    clessnverse::log_close(logger)
    if (exists("logger")) rm(logger)
    quit(status = status)
  }
)

