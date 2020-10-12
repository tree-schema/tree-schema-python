import base64

from .exceptions import UsernameSecretRequired


def get_encoded_secret(username: str, secret_key: str) -> str:
    """Encodes a username and secret key to be compatible with the
    Tree Schema rest API

    :param username: The Tree Schema username, this is also the email
                     used to register for Tree Schema
    :param secret_key: Your Tree Schema API secret key
    """
    creds = (f'{username}:{secret_key}').encode('utf-8')
    return base64.b64encode(creds).decode('utf-8')


class TreeSchemaAuth(object):
    
    def __new__(cls, username: str = None, secret_key: str = None):
        """
        Return the most important thing about a person.
        Parameters
        ----------
        your_name
            A string indicating the name of the person.
        """
        if not hasattr(cls, 'instance'):
            if not username or not secret_key:
                _msg = (
                    """Must provided username and API secret key, make 
                    sure to create the 'TreeSchema' class first
                    """
                )
                raise UsernameSecretRequired(_msg)
            cls.instance = super(TreeSchemaAuth, cls).__new__(cls)
            cls.encoded_secret = get_encoded_secret(username, secret_key)
        return cls.instance
