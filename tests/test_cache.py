from v3iofs.fs import _Cache


def test_put_and_get():
    cache = _Cache(10, 100)
    cache.put("k1", "v1")
    cache.put("k2", "v2")
    cache.put("k3", "v3.1")
    cache.put("k3", "v3.2")
    assert cache.get("k1") == "v1"
    assert cache.get("k2") == "v2"
    assert cache.get("k3") == "v3.2"


def test_invalidation_and_gc():
    cache = _Cache(10, 0)
    cache.put("k1", "v1")
    cache.put("k2", "v2")
    cache.put("k3", "v3.1")
    cache.put("k3", "v3.2")
    assert cache.get("k1") is None
    assert cache.get("k2") is None
    assert cache.get("k3") is None
    assert cache._cache == {}
    assert cache._expiry_to_key == []
