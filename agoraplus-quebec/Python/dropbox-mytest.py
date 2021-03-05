import dropbox

TOKEN = 'sl.AsWT2K7bxUHF_edcI-E0aXMUVrOHtOVxe4A_1eYDGb1o3ncSQ71KwQ8ks0Lk6R0IFVEwdRAUVrO6dT8FoglwDzMsS1MUXD4w77QX41hcmMlUltc4edO8F-TbmO0uqVn2FWX6p0w'

def stopwatch(message):
    """Context manager to print how long a block of code took."""
    t0 = time.time()
    try:
        yield
    finally:
        t1 = time.time()
        print('Total elapsed time for %s: %.3f' % (message, t1 - t0))

def list_folder(dbx, folder, subfolder):
    """List a folder.
    Return a dict mapping unicode filenames to
    FileMetadata|FolderMetadata entries.
    """
    if (folder == "" and subfolder == ""):
        path=''
        print('emptypath')
    else:
        path = '/%s/%s' % (folder, subfolder.replace(os.path.sep, '/'))
        while '//' in path:
            path = path.replace('//', '/')
        path = path.rstrip('/')
    try:
        with stopwatch('list_folder'):
            print('listing folder')
            res = dbx.files_list_folder(path)
    except dropbox.exceptions.ApiError as err:
        print('Folder listing failed for', path, '-- assumed empty:', err)
        return {}
    else:
        rv = {}
        for entry in res.entries:
            rv[entry.name] = entry
        return rv

def main():
    dbx = dropbox.Dropbox(oauth2_access_token=TOKEN)
    response = list_folder(dbx,"","")
    for file in response.entries:
        print(file.name)

if __name__ == "__main__":
    main()
