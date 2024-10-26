import os
import yaml
import time
import threading
from datetime import datetime
from wkafka.controller import Wkafka
from wkafka.controller.wkafka import Consumer_data
from wpipe.pipe import Pipeline
from wpipe.util import escribir_yaml, leer_yaml
from wpipe.log import new_logger
from wpipe.exception import ProcessError

"""
    ej. config.yaml
        microservice:
            name: Microservice_1
            version: v1.0.0
        data:
            model_path:  paht/to/model/best.pt
            num_channels: 3
            target_rows: 200
            # kafka
            kafka_server: 192.168.1.60:9092
            # pipeline
            worker_id_file: worker_id.yaml # yaml 
            pipeline_use: true
            pipeline_server: http://192.168.1.60:8418
            pipeline_token_server: mysecrettoken
            # sqlite
            sqlite_db_name: register.db
"""


class Microservice:
    complete_steps = False

    def __init__(self, config_file: str = "config.yaml"):

        # Read the configuration
        self.config_file = config_file

        self._update_config()

        self.config = leer_yaml(config_file)

        # Configure attributes
        self.worker_id_file = self.config.get("worker_id_file", "worker_id.yaml")
        self.kafka_server = self.config.get("kafka_server", "192.168.1.60:9092")
        self.pipeline_use = self.config.get("pipeline_use", False)
        self.pipeline_server = self.config.get(
            "pipeline_server", "http://192.168.1.60:8418"
        )
        self.pipeline_token_server = self.config.get(
            "pipeline_token_server", "mysecrettoken"
        )
        self.stop_event = threading.Event()

        self.kafka_instance = Wkafka(
            server=self.config["data"]["kafka_server"],
            name=self.config["microservice"]["name"],
        )

        # Start logger
        os.makedirs("logs", exist_ok=True)
        self.logger = new_logger(
            process_name=self.config["microservice"]["name"],
            path_file="logs/file_{time}.log",
        )

        #  Start Pipeline
        self.pipeline_server_data = (
            {"base_url": self.pipeline_server, "token": self.pipeline_token_server}
            if self.pipeline_use
            else None
        )
        self.microservice = Pipeline(api_config=self.pipeline_server_data)

    def _update_config(self):
        kafka_server = os.environ.get("kafka_server", None)
        pipeline_server = os.environ.get("pipeline_server", None)
        pipeline_use = os.environ.get("pipeline_use", None)

        if pipeline_use or pipeline_server or kafka_server:
            #  ---------------------- read old config
            with open(self.config_file, "r") as file:
                old_data = yaml.safe_load(file)

            #  ---------------------- update config
            if pipeline_use:
                old_data["data"]["pipeline_use"] = pipeline_use

            if pipeline_server:
                old_data["data"]["pipeline_server"] = pipeline_server

            if kafka_server:
                old_data["data"]["kafka_server"] = kafka_server

            #  ---------------------- save new config
            with open(self.config_file, "w") as file:
                yaml.safe_dump(old_data, file)

    def set_steps(self, steps: list):
        self.microservice.set_steps(steps)
        self.complete_steps = True

    def _worker_register(self, worker_id: dict = None):
        if not worker_id:
            worker_id = self.microservice.worker_register(
                name=self.config["microservice"]["name"],
                version=self.config["microservice"]["version"],
            )

        if worker_id and isinstance(worker_id, dict):
            timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            worker_id["update"] = timestamp_str
            escribir_yaml(self.worker_id_file, worker_id)

    def _worker_healthchecker(self):
        while not self.stop_event.is_set():
            if not os.path.exists(self.worker_id_file):
                worker_id = self._worker_register()

            worker_id = leer_yaml(self.worker_id_file)

            if "id" in worker_id:
                self.microservice.set_worker_id(worker_id.get("id"))

            status = self.microservice.healthcheck_worker(worker_id)
            if isinstance(status, dict):
                if status["health"] == 0:
                    #  If healthcheck fails, invoke a new record
                    os.remove(self.worker_id_file)
                    self.microservice.set_worker_id("-")
                else:
                    self._worker_register(worker_id)

            for _ in range(20):
                if not self.stop_event.is_set():
                    time.sleep(1)
                else:
                    break

    def start_api_healthchecker(self):
        if not self.complete_steps:
            raise Exception("Not step configured")

        if self.pipeline_use:

            self.healthchecker_thread = threading.Thread(
                target=self._worker_healthchecker
            )
            self.healthchecker_thread.start()
            # Time is given for registration and the first healthcheck.
            time.sleep(20)

        self.logger.info("[MICROSERVICE] Process started")

    def start_kafka_consumers(self):
        self.kafka_instance.run_consumers()

    def wait(self):
        if not self.pipeline_use:
            return

        self.healthchecker_thread.join()

    def run(self, data: dict):
        return self.microservice.run(data)

    def stop_healthchecker(self):
        if not self.pipeline_use:
            return

        self.stop_event.set()

    def send_response(
        self, results: dict, response_to: str, key: str = None, headers: dict = {}
    ):
        assert isinstance(results, dict), "Format <results> not permited"
        assert isinstance(response_to, str), "Format <response_to> not permited "
        assert isinstance(key, str), "Format <key> not permited "
        assert isinstance(headers, dict), "Format <header> not permited"

        with self.kafka_instance.producer() as producer:
            producer.send(
                topic=response_to,
                value=results,
                # headers=headers,
                key=key,  # se usa la misma key de entrada para responder
                value_type="json",
            )


class Auto_respond:
    _results: dict = None
    _errors: dict = None
    microservice: Microservice = None
    config_path: str = None

    def __init__(self, microservice: Microservice, data: Consumer_data = None) -> None:
        self.microservice = microservice
        self.data = data

    def run(self, args_dict: dict):
        return self._run(args_dict)

    def _run(self, args_dict: dict):
        return self.__run(args_dict)

    def __run(self, args_dict: dict):
        args_dict.update({"config_path": self.microservice.config_file})

        try:
            results = self.microservice.run(args_dict)
        except ProcessError as error:
            print("There was an error during the pipeline execution:")
            print(yaml.dump(error, default_flow_style=False))

            self.microservice.logger.error(f"Error: {str(error)}")

            raise Exception(error)

        return results

    @property
    def results(self):
        return self._results

    @results.setter
    def results(self, new_results: dict):
        assert isinstance(
            new_results, dict
        ), "No valid format in <results>, have to dict"

        self._results = new_results

    @property
    def error(self):
        return self._errors

    @error.setter
    def error(self, new_error: dict):
        assert isinstance(new_error, dict), "No valid format in <error>, have to dict"

        self._errors = new_error

    def __enter__(self) -> "Auto_respond":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self._results or self._errors:
            if self.data and "response_to" in self.data.header:
                self.microservice.send_response(
                    results={"answer": self._results, "error": self._errors},
                    response_to=self.data.header["response_to"],
                    key=self.data.key,
                    headers=self.data.header
                )