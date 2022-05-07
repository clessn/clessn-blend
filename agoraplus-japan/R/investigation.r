event_url  <- "https://kokkai.ndl.go.jp/#/detail?minId=120815104X00520220407&spkNum=1&single"

r <- httr::GET(event_url)

if (r$status_code == 200) {
    doc_html <- httr::content(r)
    #parsed_html <- XML::htmlTreeParse(doc_html, asText = TRUE)
    parsed_xml <- XML::htmlParse(doc_html)
    #xml_root <- XML::xmlRoot(parsed_html)
    xml_root <- XML::xmlRoot(parsed_xml)
    xml_head <- xml_root[[1]]
    xml_core <- xml_root[[2]]
}


#Loading both the required libraries
library(rvest)
library(V8)

#URL with js-rendered content to be scraped
link <- "https://kokkai.ndl.go.jp/#/detail?minId=120815104X00520220407&spkNum=1&single"

#Read the html page content and extract all javascript codes that are inside a list
emailjs <- read_html(link) %>% html_nodes('li') %>% html_nodes('script') %>% html_text()

# Create a new v8 context
ct <- v8()
#parse the html content from the js output and print it as text
read_html(ct$eval(gsub('document.write','',emailjs))) %>% 
 html_text()
