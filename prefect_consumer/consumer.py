import argparse

from prefect import Client

from bluesky_kafka import RemoteDispatcher
from event_model import DocumentNames, RunRouter
from nslsii import _read_bluesky_kafka_config_file


def get_arg_parser():
    arg_parser = argparse.ArgumentParser(description="Run a Prefect workflow in response to a Kafka message.")
    arg_parser.add_argument("beamline_name")
    arg_parser.add_argument("flow_id")
    arg_parser.add_argument("--kafka-config-file", required=False, default="/etc/bluesky/kafka.yml")
    return arg_parser


def parse_bluesky_kafka_config_file(config_file_path):
    raw_bluesky_kafka_config = _read_bluesky_kafka_config_file(config_file_path=config_file_path)

    # convert the list of bootstrap servers into a comma-delimited string
    #   this is the format required by the confluent python api
    bootstrap_servers = ",".join(raw_bluesky_kafka_config["bootstrap_servers"])

    # extract security configuration
    #   it might be a good idea to explicitly specify consumer configuration(s)
    #   in /etc/bluesky/kafka.yml
    security_config = {
        k: raw_bluesky_kafka_config["runengine_producer_config"][k]
        for k in ("security.protocol", "sasl.mechanism", "sasl.username", "sasl.password", "ssl.ca.location")
    }

    return bootstrap_servers, security_config


if __name__ == "__main__":
    args = get_arg_parser().parse_args()

    bootstrap_servers, security_config = parse_bluesky_kafka_config_file(config_file_path=args.kafka_config_file)

    consumer_config = {"auto.offset.reset": "latest"}

    consumer_config.update(security_config)

    document_to_workflow_dispatcher = RemoteDispatcher(
        topics=[f"{args.beamline_name}.bluesky.runengine.documents"],
        bootstrap_servers=bootstrap_servers,
        group_id=f"{args.beamline_name}-workflow",
        consumer_config=consumer_config,
    )

    def consumer_factory(start_doc_name, start_doc):
        print(f"start uid: {start_doc['uid']}")

        def run_flow_on_stop_document(doc_name, doc):
            if doc_name == DocumentNames.stop:
                # kick off a Prefect workflow
                prefect_client = Client()
                prefect_client.create_flow_run(
                    flow_id=args.flow_id, flow_run_name=start_doc["uid"], parameters={"stop": doc}
                )
            else:
                pass

        return [run_flow_on_stop_document], []

    workflow_router = RunRouter(factories=[consumer_factory])

    document_to_workflow_dispatcher.subscribe(workflow_router)

    document_to_workflow_dispatcher.start()

    print("all done")