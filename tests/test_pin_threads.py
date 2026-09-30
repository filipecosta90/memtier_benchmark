"""Validate actual worker affinity and result output on a restricted Linux mask."""
import collections
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from include import (add_required_env_arguments, addTLSArgs, ensure_clean_benchmark_folder,
                     get_default_memtier_config)
from mb import Benchmark, RunConfig


def _affinity_run(env, pinned, single_cpu=False, run_count=1):
    if sys.platform != 'linux' or not shutil.which('taskset'):
        env.skip()
    available = sorted(os.sched_getaffinity(0))
    if len(available) < 2 and not single_cpu:
        env.skip()
    # Deliberately exercise non-contiguous IDs where the host permits them.
    cpus = [available[0]] if single_cpu else [available[0], available[-1]]
    specs = {'name': env.testName, 'args': ['--hide-histogram', '--run-count', str(run_count)]}
    if pinned:
        specs['args'].append('--pin-threads')
    addTLSArgs(specs, env)
    config = get_default_memtier_config(threads=4, clients=1, requests=None, test_time=2)
    add_required_env_arguments(specs, config, env, env.getMasterNodesList())
    with tempfile.TemporaryDirectory() as directory:
        config = RunConfig(directory, env.testName, config, {})
        ensure_clean_benchmark_folder(config.results_dir)
        benchmark = Benchmark.from_json(config, specs)
        args = ['taskset', '-c', ','.join(map(str, cpus))] + benchmark.args
        with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
            process = subprocess.Popen(args, stdout=stdout, stderr=stderr)
            observed = set()
            try:
                deadline = time.monotonic() + 20
                while process.poll() is None:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("memtier did not finish within 20 seconds")
                    tasks = Path('/proc/{}/task'.format(process.pid))
                    try:
                        tids = sorted(int(x.name) for x in tasks.iterdir() if int(x.name) != process.pid)
                        masks = [tuple(sorted(os.sched_getaffinity(tid))) for tid in tids]
                        main_mask = sorted(os.sched_getaffinity(process.pid))
                    except (FileNotFoundError, ProcessLookupError):
                        continue
                    if len(tids) >= 4:
                        env.assertEqual(main_mask, cpus)
                        expected = [(cpus[i % len(cpus)],) for i in range(4)] if pinned else [tuple(cpus)] * 4
                        current = dict(zip(tids, masks))
                        # Sanitizers can add a helper thread with the inherited
                        # mask. On a multi-CPU mask, identify the four pinned
                        # workers by their singleton masks. Keep checking any
                        # previously observed group even if a worker loses its
                        # affinity, so selection cannot hide a regression.
                        if pinned and len(cpus) > 1:
                            wanted = collections.Counter(expected)
                            for group in observed:
                                if all(tid in current for tid in group):
                                    env.assertEqual(collections.Counter(current[tid] for tid in group), wanted)
                            tids = [tid for tid in tids if len(current[tid]) == 1]
                            masks = [current[tid] for tid in tids]
                            if len(tids) != 4:
                                time.sleep(.01)
                                continue
                        else:
                            # Default placement and a one-CPU restriction apply
                            # to every thread, including runtime helpers.
                            env.assertTrue(all(mask == tuple(cpus) for mask in masks))
                            expected = [tuple(cpus)] * len(tids)
                        # Without pthread affinity attributes, a newly created
                        # worker may be observed before its entry function pins
                        # it. Require every run to reach the expected masks,
                        # then enforce them for the rest of that worker group.
                        actual = collections.Counter(masks)
                        wanted = collections.Counter(expected)
                        if tuple(tids) in observed or actual == wanted:
                            env.assertEqual(actual, wanted)
                            observed.add(tuple(tids))
                    time.sleep(.01)
                stderr.seek(0)
                env.assertEqual(process.returncode, 0, message=stderr.read().decode(errors='replace'))
                env.assertGreaterEqual(len(observed), run_count)
                with open(os.path.join(config.results_dir, 'mb.json')) as f:
                    data = json.load(f)
                env.assertEqual(data['configuration']['pin_threads'], pinned)
                if run_count == 1:
                    env.assertGreater(data['ALL STATS']['Totals']['Count'], 0)
                    env.assertEqual(data['ALL STATS']['Totals']['Connection Errors'], 0)
                else:
                    env.assertTrue(any('AGGREGATED' in key for key in data))
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)


def test_default_preserves_inherited_mask(env):
    _affinity_run(env, False)


def test_pin_workers_to_sparse_allowed_cpus(env):
    _affinity_run(env, True)


def test_pin_workers_with_one_allowed_cpu(env):
    _affinity_run(env, True, single_cpu=True)


def test_pin_workers_across_multiple_runs(env):
    _affinity_run(env, True, run_count=2)


def test_affinity_failure_exits_without_hanging(env):
    """A denied affinity request must fail visibly, not wait on an unstarted worker."""
    compiler = shutil.which('cc')
    if sys.platform != 'linux' or not compiler:
        env.skip()
    specs = {'name': env.testName, 'args': ['--pin-threads', '--hide-histogram']}
    addTLSArgs(specs, env)
    config = get_default_memtier_config(threads=4, clients=1, requests=100)
    add_required_env_arguments(specs, config, env, env.getMasterNodesList())
    with tempfile.TemporaryDirectory() as directory:
        source = Path(directory) / 'deny_affinity.c'
        library = Path(directory) / 'deny_affinity.so'
        source.write_text('''#define _GNU_SOURCE
#include <pthread.h>
#include <sched.h>
#include <errno.h>
int pthread_attr_setaffinity_np(pthread_attr_t *attr, size_t size, const cpu_set_t *mask) {
    return EINVAL;
}
int pthread_setaffinity_np(pthread_t thread, size_t size, const cpu_set_t *mask) {
    return EINVAL;
}
''')
        subprocess.run([compiler, '-shared', '-fPIC', str(source), '-o', str(library)],
                       check=True, timeout=30, capture_output=True)
        config = RunConfig(directory, env.testName, config, {})
        ensure_clean_benchmark_folder(config.results_dir)
        benchmark = Benchmark.from_json(config, specs)
        # Sanitizer runtimes must load before the injected library. Preserve any
        # caller-supplied preloads too, rather than silently dropping them.
        linked = subprocess.run(['ldd', benchmark.args[0]], capture_output=True,
                                text=True, timeout=10).stdout
        runtimes = [line.split()[2] for line in linked.splitlines()
                    if line.strip().startswith(('libasan.so', 'libtsan.so'))
                    and '=>' in line and len(line.split()) >= 3]
        preloads = runtimes + [str(library)]
        if os.environ.get('LD_PRELOAD'):
            preloads.append(os.environ['LD_PRELOAD'])
        child_env = dict(os.environ, LD_PRELOAD=':'.join(preloads))
        result = subprocess.run(benchmark.args, env=child_env, capture_output=True, timeout=10)
        env.assertEqual(result.returncode, 1)
        env.assertIn('failed to start thread', result.stderr.decode(errors='replace'))
        # A failed startup must not publish a successful run's statistics.
        output = Path(config.results_dir) / 'mb.json'
        if output.exists():
            try:
                data = json.loads(output.read_text())
            except json.JSONDecodeError:
                data = {}
            env.assertNotIn('ALL STATS', data)
