"""测试替身必须保留租约的过期、所有权隔离与修订发布契约。"""

import pytest

from openviking.pyagfs.async_client import AsyncAGFSClient
from tests.utils.mock_agfs import MockLocalAGFS


@pytest.fixture
def clock(monkeypatch):
    current = [100.0]
    monkeypatch.setattr(MockLocalAGFS, "_coordination_now", staticmethod(lambda: current[0]))
    return current


@pytest.mark.asyncio
async def test_expired_holder_cannot_renew_or_release_successor(tmp_path, clock):
    first = MockLocalAGFS(root_path=tmp_path)
    second = MockLocalAGFS(root_path=tmp_path)
    await first.leader_validate_async()
    old = await first.leader_try_acquire_async("watch", "same-holder", 5)
    assert old is not None
    assert await second.leader_try_acquire_async("watch", "other", 5) is None
    clock[0] += 5
    successor = await second.leader_try_acquire_async("watch", "same-holder", 5)
    assert successor is not None and successor != old
    assert not await first.leader_renew_async(old, 5)
    await first.leader_release_async(old)
    assert await first.leader_try_acquire_async("watch", "other", 5) is None
    assert await second.leader_renew_async(successor, 10)
    clock[0] += 6
    assert await first.leader_try_acquire_async("watch", "other", 5) is None
    await second.leader_release_async(successor)
    assert await first.leader_try_acquire_async("watch", "other", 5) is not None


@pytest.mark.asyncio
async def test_lease_reference_belongs_to_acquiring_client(tmp_path):
    first = MockLocalAGFS(root_path=tmp_path)
    second = MockLocalAGFS(root_path=tmp_path)
    reference = await first.leader_try_acquire_async("watch", "owner", 10)
    assert not await second.leader_renew_async(reference, 10)
    await second.leader_release_async(reference)
    assert await second.leader_try_acquire_async("watch", "other", 10) is None
    isolated = MockLocalAGFS(root_path=tmp_path / "isolated")
    assert await isolated.leader_try_acquire_async("watch", "owner", 10) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("ttl", [0, -1, float("inf"), float("nan")])
async def test_invalid_lease_ttl_is_rejected(tmp_path, ttl):
    client = MockLocalAGFS(root_path=tmp_path)
    with pytest.raises(ValueError):
        await client.leader_try_acquire_async("watch", "owner", ttl)
    with pytest.raises(ValueError):
        await client.leader_renew_async("missing", ttl)


@pytest.mark.asyncio
async def test_revision_allocation_publication_expiry_and_isolation(tmp_path, clock):
    first = AsyncAGFSClient(MockLocalAGFS(root_path=tmp_path))
    second = AsyncAGFSClient(MockLocalAGFS(root_path=tmp_path))
    assert await first.revision_latest("work") == 0
    assert await first.revision_allocate("work") == 1
    assert await second.revision_allocate("other") == 2
    assert await first.revision_latest("work") == 0
    assert await second.revision_publish("work", 2) == 2
    assert await first.revision_publish("work", 1) == 2
    assert await first.revision_latest("work") == 2
    for invalid in (0, 3):
        with pytest.raises(ValueError):
            await first.revision_publish("work", invalid)
    clock[0] += 24 * 60 * 60
    assert await first.revision_latest("work") == 0
    assert await second.revision_allocate("work") == 3
    isolated = AsyncAGFSClient(MockLocalAGFS(root_path=tmp_path / "isolated"))
    assert await isolated.revision_allocate("work") == 1
    assert await isolated.revision_latest("work") == 0
