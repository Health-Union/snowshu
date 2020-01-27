
class BaseStorageAdapter:

    def __init__(self):   
        for attr in (
            'REGISTRY',
        ):
            if not hasattr(self, attr):
                raise NotImplementedError(
                    f'Target adapter requires attribute f{attr} but was not set.')
