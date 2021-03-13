# Create My Dropbox Token

# - Run this first and follow the instructions
# - Essentially it will launch a browser on your PC
# - From it, you will have to log in to your dropbox account
#   and authorize the application to access your account.
# - Once that is done copy/paste the token character string that
#   will appear in your R console and put it in the .env file
#   of your container (if you are running a container), or make it
#   a globa environment variable on your system in order to run the
#   python youtube extractor
# - Also take the token.rds file that was created and saved bu this script
#   on your hard drive and put it in the working directory of the R 
#   script agoraplus-youtube.R.  And if you plan to run this in a container
#   you must put the token file in the ./src directory of your container
# - In your dropbox account there must a folder at the root 
#   called clessn-blend (create it if it does not exist)
#   In it you must put the shared folder _SharedFolder_clessn-blend
#   (ask your CLESSN admin to share the folder to you)

devtools::install_github("karthik/rdrop2")
install.packages("httpuv")

library(httpuv)
library(rdrop2)

token <- rdrop2::drop_auth()
saveRDS(token, "token.rds")

print(token$credentials$access_token)

# - Put this token file (token.rds - which was just saved on your hard drive)
#   in the current R working directory (typically in the same directory as your rproj)
# - Note the token (long character string) that was saved on your hard drive
# - Then create an environment variable (system-wide) called DROPBOX_TOKEN on your system 
#   that contains the value of that token string
# - You may now run the agoraplus-youtube.R and the agoraplusmstranscribeautomated.py scripts

# set dropbox oauth2 endpoints
dropbox <- httr::oauth_endpoints(
  authorize = "https://www.dropbox.com/oauth2/authorize",
  access = "https://api.dropbox.com/oauth2/token"
)

# registered dropbox app's key & secret
dropbox_app <- httr::oauth_app("agora+", "oan106vfhg7gnxa", "vl4e1ujwfynpzwf")

# get the token
dropbox_token <- httr::oauth2.0_token(endpoint = dropbox, app = dropbox_app, cache = TRUE, use_oob = FALSE)

mytoken <- rdrop2::drop_auth(key = "oan106vfhg7gnxa", secret = "vl4e1ujwfynpzwf")

r <- POST("https://api.dropbox.com/oauth2/token",
          config = list(),
          body = list(
            grant_type="client_credentials",
            client_id="oan106vfhg7gnxa",
            client_secret="vl4e1ujwfynpzwf",
            scope="api_suburbperformance_read"
          ),
          encode = "form"
)
warn_for_status(r)          
content(r)

# curl -X POST https://api.dropboxapi.com/2/files/list_folder \
# --header "Authorization: Bearer cgx3e0Jqng0AAAAAAAAAAVJ2Obxnvk-5t4sjAV56zHSLClD0bGqwmhTAhTkD87vU" \
# --header "Content-Type: application/json" \
# --data "{\"path\": \"/Homework/math\",\"recursive\": false,\"include_media_info\": false,\"include_deleted\": false,\"include_has_explicit_shared_members\": false,\"include_mounted_folders\": true,\"include_non_downloadable_files\": true}"

body <- paste('{\"path\": \"/',
              "clessn-blend",
              '\",\"recursive\": false,\"include_media_info\": false,\"include_deleted\": false,\"include_has_explicit_shared_members\": false,\"include_mounted_folders\": true,\"include_non_downloadable_files\": true}',
              sep='')

s <- httr::POST(url = 'https://api.dropboxapi.com/2/files/list_folder',
                httr::add_headers('Authorization' = paste("Bearer", "aVPJM4fsRpIAAAAAAAAAAdKU15GLcG9JrZBr5m9sj2P14OusKL5UUqjIY0VtU5cw"),
                            'Content-Type' = 'application/json'),
                body = body,
                #encode = "form",
                httr::verbose(info = FALSE))

httr::content(s)
httr::
