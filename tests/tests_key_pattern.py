import tempfile
import json
from include import *
from mbdirector.benchmark import Benchmark
from mbdirector.runner import RunConfig


def test_key_pattern_parallel(env):
    benchmark_specs = {"name": env.testName, "args": ['--key-pattern=P:P']}
    threads = 10
    clients = 5
    total_num_of_clients = threads * clients
    key_minimum = 0
    key_maximum = 10000000
    key_range_size = key_maximum - key_minimum
    client_range = key_range_size / total_num_of_clients + 1

    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads, clients)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env, memtier_ok)
    with open('{0}/mb.json'.format(config.results_dir)) as mn_json:
        memtier_json_output = json.load(mn_json)
        observed_max = assert_ParallelKeyspaceDistribution(clients, env, key_maximum, key_minimum, memtier_json_output,
                                                           threads)

        env.assertEqual(observed_max, key_maximum)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, merged_command_stats, overall_expected_request_count,
                                    overall_request_count)


def test_key_pattern_sequential(env):
    benchmark_specs = {"name": env.testName, "args": ['--key-pattern=S:S']}
    threads = 10
    clients = 5
    key_minimum = 0
    key_maximum = 10000000

    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads, clients)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env, memtier_ok)
    with open('{0}/mb.json'.format(config.results_dir)) as mn_json:
        memtier_json_output = json.load(mn_json)
        assert_SequentialKeyspaceDistribution(clients, env, key_maximum, key_minimum, memtier_json_output,
                                              threads)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, merged_command_stats, overall_expected_request_count,
                                    overall_request_count)


def test_key_pattern_parallel_small_key_range(env):
    threads = 10
    clients = 5
    key_minimum = 1
    key_maximum = 10
    benchmark_specs = {"name": env.testName, "args": ['--key-pattern=P:P', '--key-minimum={}'.format(key_minimum),
                                                      '--key-maximum={}'.format(key_maximum)]}
    key_range_size = key_maximum - key_minimum + 1

    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads, clients)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env, memtier_ok)
    with open('{0}/mb.json'.format(config.results_dir)) as mn_json:
        memtier_json_output = json.load(mn_json)
        assert_ParallelKeyspaceDistribution_RoundRobin(clients, env, key_minimum, key_range_size, memtier_json_output,
                                                       threads)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, merged_command_stats, overall_expected_request_count,
                                    overall_request_count)


def test_key_pattern_parallel_small_key_range_arbitrary_command(env):
    threads = 10
    clients = 5
    key_minimum = 1
    key_maximum = 10
    benchmark_specs = {"name": env.testName, "args": ['--command=SET __key__ __data__', '--command-key-pattern=P',
                                                      '--key-minimum={}'.format(key_minimum),
                                                      '--key-maximum={}'.format(key_maximum)]}
    key_range_size = key_maximum - key_minimum + 1

    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads, clients)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env, memtier_ok)
    with open('{0}/mb.json'.format(config.results_dir)) as mn_json:
        memtier_json_output = json.load(mn_json)
        assert_ParallelKeyspaceDistribution_RoundRobin(clients, env, key_minimum, key_range_size, memtier_json_output,
                                                       threads)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, merged_command_stats, overall_expected_request_count,
                                    overall_request_count)
