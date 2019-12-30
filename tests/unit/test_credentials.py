from snowshu.core.models.credentials import Credentials

def tests_urlencode_creds():

    creds=Credentials(  user='some_user_name@some_domain.com',
                        password='jW@%^2EwLgDcNy{?5h',
                        host='database.some_domain.com')
    creds.urlencode()

    assert creds.password=='jW%40%25%5E2EwLgDcNy%7B%3F5h'
                        
