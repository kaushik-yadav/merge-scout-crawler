from mergescout.github.graphql import Gate, GraphQLResult, TokenRejected
from mergescout.github.identities import Identity
from mergescout.setup_check import check_identity, check_store


class Client:
    def __init__(self, limit=5000, reject=False):
        self.limit, self.reject = limit, reject

    def query(self, q, variables=None):
        if self.reject:
            raise TokenRejected("GitHub rejected the token (401)")
        data = {"rateLimit": {"limit": self.limit, "remaining": 4990, "cost": 1, "resetAt": "x"},
                "repository": {"stargazerCount": 53000}}
        return GraphQLResult(data, 1, 4990, "x")


class Store:
    def __init__(self, release=True, can_write=True):
        self.release, self.can_write, self.deleted = release, can_write, []

    def get(self, tag):
        return {"id": 1, "tag_name": tag} if self.release else None

    def upload(self, tag, path, name=None):
        if not self.can_write:
            raise RuntimeError("uploading setup-check.txt failed: HTTP 403")
        return {"id": 9, "name": name or path.name}

    def delete_asset(self, asset):
        self.deleted.append(asset["id"])


def test_identity_check_reports_its_hourly_limit():
    ok, detail = check_identity(Identity("app", lambda: Client(limit=5000), Gate()))
    assert ok and "5000 points/hour" in detail
    ok, detail = check_identity(Identity("app", lambda: Client(reject=True), Gate()))
    assert not ok and "rejected" in detail


def test_store_check_reads_then_writes_and_cleans_up():
    store = Store()
    ok, detail = check_store(store)
    assert ok and store.deleted == [9] and "write" in detail
    assert check_store(Store(release=False))[0] is False
    ok, detail = check_store(Store(can_write=False))
    assert not ok and "403" in detail
