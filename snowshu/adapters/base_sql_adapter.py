from snowshu.core.models.credentials import Credentials

class BaseSQLAdapter:


    REQUIRED_CREDENTIALS:iter
    ALLOWED_CREDENTIALS:iter



    @property
    def credentials(self)->dict:
        return self._credentials

    @credentials.setter
    def credentials(self,value:Credentials)->None:
        for cred in self.REQUIRED_CREDENTIALS:
            if value.__dict__[cred] == None:
                raise KeyError(f"{self.__class__.__name__} requires missing credential {cred}.")
        ALL_CREDENTIALS = self.REQUIRED_CREDENTIALS+self.ALLOWED_CREDENTIALS
        for val in [val for val in value.__dict__.keys() if (val not in ALL_CREDENTIALS and value.__dict__[val] is not None)]:
            raise KeyError(f"{self.__class__.__name__} received extra argument {val} this is not allowed")

        self._credentials=value
