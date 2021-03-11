# Create My Dropbox Token

# Run this first and follow the instructions
install.package("rdrop2")

token <- rdrop2::drop_auth()
saveRDS(token, "token.rds")

print(token$credentials$access_token)

# - Put this token file (token.rds - which was just saved on your hard drive)
#   in the current R working directory (typically in the same directory as your rproj)
# - Note the token (long character string) that was saved on your hard drive
# - Then create an environment variable (system-wide) called DROPBOX_TOKEN on your system 
#   that contains the value of that token string
# - You may now run the agoraplus-youtube.R and the agoraplusmstranscribeautomated.py scripts