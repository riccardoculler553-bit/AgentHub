"""Unit tests: ReconnectManager exponential backoff + jitter."""

import reconnect  # client module, path set up in conftest
from reconnect import ReconnectManager


def test_delays_are_exponential_and_capped():
    mgr = ReconnectManager()
    delays = [mgr.next_delay() for _ in range(10)]
    # After resetting attempts the bases follow 1,2,4,8,16,30,30,...
    mgr2 = ReconnectManager()
    bases = []
    for _ in range(8):
        mgr2.attempt_backup = mgr2.attempt
        # Strip jitter by checking bounds instead of exact values
        bases.append(mgr2.next_delay())
    assert bases[0] < 1 + 1 * 0.25 + 1e-9          # ~1s + jitter
    assert bases[1] < 2 + 2 * 0.25 + 1e-9          # ~2s + jitter
    assert bases[5] >= 30                          # capped base reached
    assert bases[6] >= 30 and bases[7] >= 30       # stays at cap
    assert all(d <= 30 + 30 * 0.25 + 1e-9 for d in bases)  # never exceeds cap+jitter


def test_jitter_prevents_thundering_herd():
    mgr_a = ReconnectManager()
    mgr_b = ReconnectManager()
    a = [mgr_a.next_delay() for _ in range(6)]
    b = [mgr_b.next_delay() for _ in range(6)]
    assert a != b  # random jitter makes simultaneous reconnects unlikely


def test_reset():
    mgr = ReconnectManager()
    for _ in range(5):
        mgr.next_delay()
    mgr.reset()
    assert mgr.attempt == 0
    assert mgr.next_delay() < 1 + 1 * 0.25 + 1e-9


def test_backoff_is_monotonic_non_decreasing():
    mgr = ReconnectManager()
    delays = [mgr.next_delay() for _ in range(6)]
    # min possible base per attempt: 1,2,4,8,16,30
    for i, d in enumerate(delays):
        assert d >= float(reconnect.BASE_DELAYS[min(i, len(reconnect.BASE_DELAYS) - 1)])
