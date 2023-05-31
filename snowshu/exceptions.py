class AdapterNotFound(Exception):
    pass


class InvalidRelationshipException(Exception):
    pass


class TooManyRecords(Exception):
    pass


class UnableToExecuteCopyReplicaCommand(Exception):
    pass

class UnableToStartPostgres(Exception):
    pass
