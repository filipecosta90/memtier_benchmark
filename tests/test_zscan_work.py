"""Useful-work counters checked against replayed Redis replies, not a page-size estimate."""
import json
import tempfile

from redis import Redis
from redis.exceptions import ResponseError

from test_collection_scan import _build, _capture, _preload, _read_file, _run


def _wire_size(reply):
    if isinstance(reply, list):
        return len('*{}\r\n'.format(len(reply))) + sum(_wire_size(x) for x in reply)
    return len('${}\r\n'.format(len(reply))) + len(reply) + 2


def _oracle(conn, records, cap=0, from_zero=True, stride=2):
    raw = Redis(connection_pool=conn.connection_pool)
    raw.set_response_callback('ZSCAN', lambda reply: reply)
    counts = dict(Responses=len(records), Pages=0, **{
        'Returned Members': 0, 'Response Bytes': 0, 'Completed Iterations': 0,
        'Capped Iterations': 0, 'Invalid or Error Replies': 0})
    iterations = {}
    for client, args in records:
        continuation = args[2] != b'0'
        iterations[client] = iterations.get(client, 0) + 1 if continuation else 0
        try:
            reply = raw.execute_command(*args)
        except ResponseError:
            counts['Invalid or Error Replies'] += 1
            continue
        counts['Pages'] += 1
        counts['Returned Members'] += len(reply[1]) // stride
        counts['Response Bytes'] += _wire_size(reply)
        terminal = reply[0] == b'0'
        counts['Completed Iterations'] += int(terminal and from_zero)
        counts['Capped Iterations'] += int(not terminal and cap > 0 and iterations[client] >= cap)
    return counts


def _check(env, stats, expected):
    work = stats['ZSCAN Work']
    for field, count in expected.items():
        env.assertEqual(work[field], count, message=field)
    env.assertEqual(work['Responses'], stats['Totals']['Count'])
    duration = work['Duration Seconds']
    env.assertGreater(duration, 0)
    for rate, count in [('Members/sec', 'Returned Members'),
                        ('Iterations/sec', 'Completed Iterations'),
                        ('Response Bytes/sec', 'Response Bytes')]:
        env.assertLess(abs(work[rate] - work[count] / duration), 0.00001)


def test_zscan_work_replay_and_thread_merge(env):
    conn = env.getConnection()
    _preload(conn, 'ZSCAN', ['work:zset'])
    for protocol in ('redis', 'resp3'):
        stats, records, _ = _run(env, 'ZSCAN work:zset 0 COUNT 10',
                                 requests=180, threads=2, clients=3, protocol=protocol)
        expected = _oracle(conn, records)
        env.assertGreater(expected['Completed Iterations'], 0)
        _check(env, stats, expected)


def test_zscan_work_empty_filtered_and_capped(env):
    conn = env.getConnection()
    _preload(conn, 'ZSCAN', ['work:zset'])
    for command, args, cap in [
            ('ZSCAN work:missing 0 COUNT 10', [], 0),
            ('ZSCAN work:zset 0 MATCH never-matches:* COUNT 10', [], 0),
            ('ZSCAN work:zset 0 COUNT 1', ['--scan-incremental-max-iterations', '2'], 2)]:
        conn.delete('work:missing')
        stats, records, _ = _run(env, command, args=args, requests=100)
        _check(env, stats, _oracle(conn, records, cap=cap))
        if cap:
            env.assertEqual(stats['ZSCAN Work']['Completed Iterations'], 0)
            env.assertGreater(stats['ZSCAN Work']['Capped Iterations'], 0)


def test_zscan_work_wrongtype_and_nonzero_start(env):
    conn = env.getConnection()
    conn.set('work:wrongtype', 'value')
    stats, records, _ = _run(env, 'ZSCAN work:wrongtype 0', requests=10)
    _check(env, stats, _oracle(conn, records))
    _preload(conn, 'ZSCAN', ['work:zset'])
    stats, records, _ = _run(env, 'ZSCAN work:zset 1 COUNT 100', requests=20, initial_cursor=b'1')
    _check(env, stats, _oracle(conn, records, from_zero=False))
    env.assertEqual(stats['ZSCAN Work']['Completed Iterations'], 0)


def test_zscan_work_noscores_and_literal_option_pattern(env):
    conn = env.getConnection()
    _preload(conn, 'ZSCAN', ['work:zset'])
    stats, records, _ = _run(env, 'ZSCAN work:zset 0 MATCH NOSCORES COUNT 20', requests=80)
    _check(env, stats, _oracle(conn, records))
    try:
        conn.execute_command('ZSCAN', 'work:zset', 0, 'NOSCORES')
    except ResponseError:
        env.skip()
    for command, stride in [('ZSCAN work:zset 0 COUNT 20 NOSCORES', 1),
                            ('ZSCAN work:zset 0 MATCH NOSCORES COUNT 20', 2)]:
        stats, records, _ = _run(env, command, requests=80)
        _check(env, stats, _oracle(conn, records, stride=stride))


def test_zscan_work_average_and_non_scan_absence(env):
    conn = env.getConnection()
    conn.delete('work:small')
    conn.zadd('work:small', {'one': 1, 'two': 2, 'three': 3})
    with tempfile.TemporaryDirectory() as directory:
        benchmark, config = _build(env, directory,
                                  ['--command', 'ZSCAN work:small 0', '--scan-incremental-iteration',
                                   '--run-count', '3'], 11, 2, 2)
        ok, _ = _capture(conn, benchmark)
        env.assertTrue(ok)
        result = json.loads(_read_file(config, 'mb.json'))
        average = result['AGGREGATED AVERAGE RESULTS (3 runs)']['ZSCAN Work']
        env.assertEqual(average['Responses'], 3 * 11 * 2 * 2)
        env.assertEqual(average['Completed Iterations'], average['Responses'])
        env.assertEqual(average['Returned Members'], average['Responses'] * 3)
        env.assertLess(abs(average['Members/sec'] - average['Returned Members'] /
                          average['Duration Seconds']), 0.00001)
    stats, _, _ = _run(env, 'ZSCAN work:small 0', incremental=False, requests=8)
    env.assertFalse('ZSCAN Work' in stats)
    conn.sadd('work:set', 'one')
    stats, _, _ = _run(env, 'SSCAN work:set 0', requests=8)
    env.assertFalse('ZSCAN Work' in stats)


def test_zscan_work_scripted_replies(env):
    """Malformed shapes and a broken chain must not inflate completed work."""
    import socket
    import subprocess
    import threading
    from include import MEMTIER_BINARY

    def page(cursor, body=b'*2\r\n$1\r\na\r\n$1\r\n1\r\n'):
        return b'*2\r\n$' + str(len(cursor)).encode() + b'\r\n' + cursor + b'\r\n' + body

    cases = [
        ('ZSCAN key 0', [page(b'9'), page(b'8', b'*1\r\n$1\r\na\r\n'), page(b'0')], 2, 2, 0, 1),
        ('ZSCAN key 0', [b'*2\r\n*0\r\n*0\r\n'], 0, 0, 0, 1),
        ('ZSCAN key 0', [page(b'bad')], 0, 0, 0, 1),
        ('ZSCAN key 0', [page(b'9'), b'-ERR broken\r\n', page(b'0')], 2, 2, 1, 1),
        ('ZSCAN key 0 NOSCORES', [page(b'0', b'*1\r\n$1\r\na\r\n')], 1, 1, 1, 0),
        ('ZSCAN key 0', [page(b'9'), b'-TRYAGAIN retry\r\n'],
         1, 1, 0, 0, ['--retry-on-error', '--max-retries', '1'], 2),
    ]
    for case in cases:
        command, replies, pages, members, completed, invalid = case[:6]
        extra = case[6] if len(case) > 6 else []
        requests = case[7] if len(case) > 7 else len(replies)
        errors = []
        with socket.socket() as listener, tempfile.TemporaryDirectory() as directory:
            listener.bind(('127.0.0.1', 0))
            listener.listen(1)
            listener.settimeout(10)

            def serve():
                try:
                    with listener.accept()[0] as peer:
                        peer.settimeout(10)
                        stream = peer.makefile('rb')
                        sent = 0
                        while sent < len(replies):
                            line = stream.readline()
                            assert line.startswith(b'*'), line
                            args = []
                            for _ in range(int(line[1:])):
                                size = int(stream.readline()[1:])
                                args.append(stream.read(size))
                                assert stream.read(2) == b'\r\n'
                            if args[0].upper() == b'ZSCAN':
                                peer.sendall(replies[sent])
                                sent += 1
                            else:
                                peer.sendall(b'+OK\r\n')
                except Exception as exc:
                    errors.append(repr(exc))

            worker = threading.Thread(target=serve, daemon=True)
            worker.start()
            path = directory + '/result.json'
            try:
                result = subprocess.run([
                    MEMTIER_BINARY, '-s', '127.0.0.1', '-p', str(listener.getsockname()[1]),
                    '-t', '1', '-c', '1', '-n', str(requests), '--pipeline', '1',
                    '--command', command, '--scan-incremental-iteration',
                    '--hide-histogram', '--json-out-file', path] + extra,
                    capture_output=True, text=True, timeout=15)
                env.assertEqual(result.returncode, 0, message=result.stderr)
                with open(path) as stream:
                    stats = json.load(stream)['ALL STATS']
                work = stats['ZSCAN Work']
                env.assertEqual(work['Responses'], pages + invalid)
                env.assertEqual(work['Responses'], stats['Totals']['Count'])
                env.assertEqual(work['Pages'], pages)
                env.assertEqual(work['Returned Members'], members)
                env.assertEqual(work['Completed Iterations'], completed)
                env.assertEqual(work['Invalid or Error Replies'], invalid)
            finally:
                worker.join(timeout=11)
            env.assertFalse(worker.is_alive())
            env.assertEqual(errors, [])



def test_zscan_work_generated_cursor_origins(env):
    conn = env.getConnection()
    conn.delete('work:missing')
    for protocol in ('redis', 'resp3'):
        with tempfile.TemporaryDirectory() as directory:
            # The normal generator has a fixed seed unless --randomize is set.
            # One-byte data samples include numeric and invalid cursor bytes.
            benchmark, config = _build(env, directory, [
                '--command', 'ZSCAN work:missing __data__', '--scan-incremental-iteration',
                '--protocol', protocol, '--random-data', '--data-size', '1'], 4096, 1, 1)
            ok, records = _capture(conn, benchmark)
            env.assertTrue(ok, message=_read_file(config, 'mb.stderr'))
            env.assertEqual(len(records), 4096)
            origins = [args[2] for _, args in records]
            env.assertGreater(origins.count(b'0'), 0)
            env.assertGreater(origins.count(b'1'), 0)
            stats = json.loads(_read_file(config, 'mb.json'))['ALL STATS']
            env.assertEqual(stats['Zscan 0s']['Count'], 4096)
            env.assertEqual(stats['ZSCAN Work']['Responses'], 4096)
            env.assertEqual(stats['ZSCAN Work']['Completed Iterations'], origins.count(b'0'))
