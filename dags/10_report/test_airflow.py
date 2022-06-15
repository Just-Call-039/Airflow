from smb.SMBConnection import SMBConnection

conn = SMBConnection(username="dbs01", password="S@LeS*41011", my_name="AlexanderBrezhnev", remote_name="samba", use_ntlm_v2=True)

smb_file = '/10_report/testv.csv'

if conn.connect("10.88.22.128", 445):
    print(conn.listShares())
    with open('/root/airflow/dags/10_report/Files/All_users.csv', 'rb') as f:
        conn.storeFile('dbs', smb_file, f)
