from copy import deepcopy

from confluent_kafka.schema_registry import SchemaRegistryClient

from .config import KafkaConfig

try:
    import django
    from django.conf import settings

    django_support = True
except ModuleNotFoundError:
    django_support = False


class Papfa:
    _instances = set()

    def __init__(self):
        self.__class__._instances.add(self)
        self._config = None
        self.django_support = django_support
        self.is_django_setup = False
        self.setup()

    @classmethod
    def get_instance(cls):
        if not cls._instances:
            cls()
        return list(cls._instances)[0]

    @classmethod
    def papfa_configured(cls):
        return len(cls._instances) > 0

    def setup(self, config: dict = None):
        if config is not None or django_support:
            self._config = {}
            broker = self.get_config("BROKER", config, default="kafka")
            if broker == "kafka":
                self._config["kafka_config"] = KafkaConfig(
                    bootstrap_servers=self.get_config(
                        "KAFKA_BOOTSTRAP_SERVERS", config
                    ),
                    sasl_password=self.get_config("KAFKA_SASL_PASSWORD", config),
                    sasl_username=self.get_config("KAFKA_SASL_USERNAME", config),
                    sasl_mechanism=self.get_config(
                        "KAFKA_SASL_MECHANISM", config, default="PLAIN"
                    ),
                    security_protocol=self.get_config(
                        "KAFKA_SECURITY_PROTOCOL", config, default="SASL_PALIN"
                    ),
                )
                self._config["kafka_group_id_prefix"] = self.get_config(
                    "GROUP_ID_PREFIX", config
                )
                try:
                    self._config["schema_registry"] = SchemaRegistryClient(
                        {
                            "url": self.get_config("SCHEMA_REGISTRY_URL", config),
                            "basic.auth.user.info": self.get_config(
                                "SCHEMA_REGISTRY_BASIC_AUTH", config
                            ),
                        }
                    )
                except ValueError:
                    self._config["schema_registry"] = None
                self._config["consumer_middlewares"] = self.get_config(
                    "CONSUMER_MIDDLEWARES", config
                )
                if django_support:
                    self._config["consumers_dirs"] = settings.INSTALLED_APPS
                else:
                    self._config["consumers_dirs"] = self.get_config(
                        "CONSUMERS_DIRS", config
                    )

            else:
                raise NotImplementedError(f"Broker {broker} not supported yet")

        else:
            self._config = None

    def get_config(self, key, config: dict = None, default=None):
        if config and key in config:
            return config[key]

        if not django_support:
            return self.get_default_or_raise_bad_config(key, default)

        if self.django_support and not self.is_django_setup:
            self.is_django_setup = True
            django.setup()

        if not hasattr(settings, "PAPFA"):
            return self.get_default_or_raise_bad_config(key, default)

        papfa = settings.PAPFA

        if not isinstance(papfa, dict):
            return self.get_default_or_raise_bad_config(key, default)

        if key not in papfa:
            return self.get_default_or_raise_bad_config(key, default)

        return papfa[key]

    def get_default_or_raise_bad_config(self, key, default, raise_error=False):
        if raise_error:
            raise Exception("PAPFA.%s not set in settings.py" % key)
        else:
            return default

    def __getitem__(self, item):
        if self._config is None:
            raise RuntimeError("PAPFA Settings is not configured yet!")

        return self._config.get(item)

    def get_configs(self):
        return deepcopy(self._config)
