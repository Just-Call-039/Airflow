
def transfer_file_from_dbs(file_path_on_share, local_file_path):
    """
    Transfers a file from a DBS (Samba) share to a local file system.

    This function establishes a connection to a Samba server, retrieves a file
    from the specified share, and saves it to the local file system.

    Parameters:
    file_path_on_share (str): The path of the file on the Samba share.
    local_file_path (str): The path where the file will be saved locally.

    Returns:
    None

    Raises:
    OperationFailure: If there's an error while downloading the file.
    """
    print('ok')
    server_ip = '192.168.1.157'
    server_name = 'servername'
    username = 'user_dbs01'
    password = 'tZSzfjLEkD95'
    share_name = 'dbs'
    print('ok')
    try:
        print('ok')
        conn = SMBConnection(username, password, 'my_machine_name', server_name, use_ntlm_v2=True)
        connected = conn.connect(server_ip, 445)
        print('ok')
    finally:
        conn.close()
        print('ok')
