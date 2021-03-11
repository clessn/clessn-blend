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