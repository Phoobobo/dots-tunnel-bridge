import sqlite3

import pytest

from dots_bridge.store import Store


class Clock:
    def __init__(self):
        self.now = 0.0
        self.waits = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.waits.append(seconds)
        self.now += seconds


class Connection:
    def __init__(self, error, *, failures=1, clock=None):
        self.error = error
        self.failures = failures
        self.clock = clock
        self.statements = []
        self.attempt_times = []

    def execute(self, statement):
        self.statements.append(statement)
        if self.clock is not None:
            self.attempt_times.append(self.clock.monotonic())
        if len(self.statements) <= self.failures:
            raise self.error


def database_error(code):
    error = sqlite3.OperationalError("database is locked")
    if code is not None:
        error.sqlite_errorcode = code
    return error


@pytest.fixture
def clock(monkeypatch):
    result = Clock()
    monkeypatch.setattr("dots_bridge.store.time", result)
    return result


def test_wal_lock_upgrade_retries_busy_then_succeeds(clock):
    connection = Connection(database_error(sqlite3.SQLITE_BUSY))
    Store._enable_wal(connection)
    assert connection.statements == ["PRAGMA journal_mode=WAL"] * 2
    assert clock.waits == [0.01]


@pytest.mark.parametrize("code", [sqlite3.SQLITE_LOCKED, sqlite3.SQLITE_ERROR, None])
def test_wal_does_not_retry_other_operational_errors(clock, code):
    error = database_error(code)
    connection = Connection(error)
    with pytest.raises(sqlite3.OperationalError) as caught:
        Store._enable_wal(connection)
    assert caught.value is error
    assert connection.statements == ["PRAGMA journal_mode=WAL"]
    assert clock.waits == []


def test_wal_does_not_retry_other_sqlite_errors(clock):
    error = sqlite3.DatabaseError("database is malformed")
    connection = Connection(error)
    with pytest.raises(sqlite3.DatabaseError) as caught:
        Store._enable_wal(connection)
    assert caught.value is error
    assert connection.statements == ["PRAGMA journal_mode=WAL"]
    assert clock.waits == []


def test_wal_stops_retrying_at_one_second_and_preserves_error(clock):
    error = database_error(sqlite3.SQLITE_BUSY)
    connection = Connection(error, failures=1000, clock=clock)
    with pytest.raises(sqlite3.OperationalError) as caught:
        Store._enable_wal(connection)
    assert caught.value is error
    assert clock.now == pytest.approx(1.0)
    assert sum(clock.waits) == pytest.approx(1.0)
    assert all(0 < delay <= 0.01 for delay in clock.waits)
    assert 100 <= len(connection.statements) <= 102
    assert all(attempt < 1 for attempt in connection.attempt_times)
