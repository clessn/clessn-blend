# Create My Dropbox Token

# Run this first and follow the instructions
install.package("rdrop2")

token <- rdrop2::drop_auth()
saveRDS(token, "token.rds")

print(token$credentials$access_token)

# Put this token file (token.rds) which was just saved on your hard drive
# at the getcw() (typically in the same directory as your rproj)
# Then create en environment variable called DROPBOX_TOKEN on your system (system-wide)
# You may now run the agoraplus-youtube.R and the agoraplusmstranscribeautomated.py scripts