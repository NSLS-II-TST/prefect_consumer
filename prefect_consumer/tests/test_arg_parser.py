import pytest

from prefect_consumer.message_to_workflow import get_arg_parser


def test_config_file_arg():
    arg_parser = get_arg_parser()

    # fail with no arguments
    with pytest.raises(SystemExit):
        arg_parser.parse_args()

    args = arg_parser.parse_args(["tst", "a-prefect-flow-name", "a-prefect-project-name"])
    assert args.beamline_name == "tst"
    assert args.flow_id == "a-prefect-flow-name"
    assert args.prefect_project_name == "a-prefect-project-name"
    assert args.kafka_config_file == "/etc/bluesky/kafka.yml"

    args = arg_parser.parse_args(
        ["tst", "a-prefect-flow-name", "a-prefect-project-name", "--kafka-config-file", "/etc/bluesky/another_kafka.yml"]
    )
    assert args.beamline_name == "tst"
    assert args.flow_id == "a-prefect-flow-name"
    assert args.prefect_project_name == "a-prefect-project-name"
    assert args.kafka_config_file == "/etc/bluesky/another_kafka.yml"
