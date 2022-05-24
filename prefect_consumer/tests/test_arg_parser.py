import pytest

from prefect_consumer.consumer import get_arg_parser


def test_config_file_arg():
    arg_parser = get_arg_parser()

    # fail with no arguments
    with pytest.raises(SystemExit):
        arg_parser.parse_args()

    args = arg_parser.parse_args(["tst", "7cc167d3-737d-4187-85d8-d5e5a75fbd93"])
    assert args.beamline_name == "tst"
    assert args.flow_id == "7cc167d3-737d-4187-85d8-d5e5a75fbd93"
    assert args.kafka_config_file == "/etc/bluesky/kafka.yml"

    args = arg_parser.parse_args(
        ["tst", "7cc167d3-737d-4187-85d8-d5e5a75fbd93", "--kafka-config-file", "/etc/bluesky/another_kafka.yml"]
    )
    assert args.beamline_name == "tst"
    assert args.flow_id == "7cc167d3-737d-4187-85d8-d5e5a75fbd93"
    assert args.kafka_config_file == "/etc/bluesky/another_kafka.yml"
