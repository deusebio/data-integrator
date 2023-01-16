#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to database charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging
from typing import Dict, Optional

from ops.charm import ActionEvent, CharmBase, RelationBrokenEvent
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, StatusBase, Relation

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseCreatedEvent,
    DatabaseRequires,
    KafkaRequires,
    TopicCreatedEvent,
)

logger = logging.getLogger(__name__)

MYSQL = "mysql"
POSTGRESQL = "postgresql"
MONGODB = "mongodb"
KAFKA = "kafka"


class IntegratorCharm(CharmBase):
    """Integrator charm that connects to database charms."""

    def _setup_database_requirer(self, relation_name: str) -> DatabaseRequires:
        database_requirer = DatabaseRequires(
            self,
            relation_name=relation_name,
            database_name=self.database_name or "",
            extra_user_roles=self.extra_user_roles or "",
        )
        self.framework.observe(database_requirer.on.database_created, self._on_database_created)
        self.framework.observe(self.on[relation_name].relation_broken, self._on_relation_broken)
        return database_requirer

    def __init__(self, *args):
        super().__init__(*args)

        self.framework.observe(self.on.get_credentials_action, self._on_get_credentials_action)
        self.framework.observe(self.on.config_changed, self._on_config_changed)

        self.databases: Dict[str, DatabaseRequires] = {
            name: self._setup_database_requirer(name)
            for name in [MYSQL, MONGODB, POSTGRESQL]
        }

        # Kafka
        self.kafka = KafkaRequires(
            self,
            relation_name=KAFKA,
            topic=self.topic_name or "",
            extra_user_roles=self.extra_user_roles or "",
        )
        self.framework.observe(self.kafka.on.topic_created, self._on_topic_created)
        self.framework.observe(self.on[KAFKA].relation_broken, self._on_relation_broken)

    @property
    def topic_name(self):
        return self.model.config.get("topic-name", None)

    @property
    def database_name(self):
        return self.model.config.get("database-name", None)

    @property
    def extra_user_roles(self):
        return self.model.config.get("extra-user-roles", None)

    @property
    def kafka_relation(self) -> Optional[Relation]:
        return self.kafka.relations[0] if len(self.kafka.relations) > 0 else None

    @property
    def database_relations(self) -> Dict[str, Relation]:
        return {
            name: requirer.relations[0]
            for name, requirer in self.databases.items() if len(requirer.relations) > 0
        }

    @property
    def databases_active(self) -> Dict[str, str]:
        """Return the configured topic name."""
        return {
            name: relation.data[self.app]["database"]
            for name, relation in self.database_relations.items()
        }

    @property
    def topic_active(self) -> Optional[str]:
        """Return the configured topic name."""
        return relation.data[self.app]["topic"] if (relation := self.kafka_relation) else None

    @property
    def extra_user_roles_active(self) -> Optional[str]:
        """Return the configured user-extra-roles parameter."""
        return relation.data[self.app]["extra-user-roles"] if (relation := self.kafka_relation) else None

    @property
    def is_database_related(self):
        """Return if a relation with database is present."""
        possible_relations = [
            self._check_for_credentials(database_requirer.relations)
            for _, database_requirer in self.databases.items()
        ]
        return any(possible_relations)

    @staticmethod
    def _check_for_credentials(relations) -> bool:
        """Check if credentials are present in the relation databag."""
        for relation in relations:
            if (
                    "username" in relation.data[relation.app]
                    and "password" in relation.data[relation.app]
            ):
                return True
        return False

    @property
    def is_kafka_related(self):
        """Return if a relation with kafka is present."""
        return self._check_for_credentials(self.kafka.relations)

    def get_status(self) -> StatusBase:
        if not self.topic_name and not self.database_name:
            return BlockedStatus("Please specify either topic or database")

        if not self.is_database_related and not self.is_kafka_related:
            return BlockedStatus("Please relate the app with something")

        if self.is_kafka_related and self.topic_active != self.topic_name:
            logger.error("Trying to change Kafka configuration for existing relation")
            return BlockedStatus("To change topic, please remove relation and add it again")

        if self.is_database_related and any(
                [database != self.database_name for database in self.databases_active.values()]
        ):
            logger.error("Trying to change Kafka configuration for existing relation")
            return BlockedStatus("To change topic, please remove relation and add it again")

        return ActiveStatus()

    def _on_config_changed(self, _):
        """Handle on config changed event."""
        # Only execute in the unit leader
        if not self.unit.is_leader():
            return

        self.unit.status = self.get_status()

    def _on_get_credentials_action(self, event: ActionEvent) -> None:
        """Returns the credentials an action response."""
        if not self.database_name and not self.topic_name:
            event.fail("The database name or topic name is not specified in the config.")
            event.set_results({"ok": False})
            return

        if not self.is_database_related and not self.is_kafka_related:
            event.fail("The action can be run only after relation is created.")
            event.set_results({"ok": False})
            return

        result = {"ok": True}

        for name in self.databases_active.keys():
            result[name] = list(self.databases[name].fetch_relation_data().values())[0]

        if self.is_kafka_related:
            result[KAFKA] = list(self.kafka.fetch_relation_data().values())[0]

        event.set_results(result)

    def _on_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        logger.debug(f"database credentials are received: {event.username}")
        self.unit.status = self.get_status()
        if not self.unit.is_leader:
            return

    def _on_relation_broken(self, event: RelationBrokenEvent):
        self.unit.status = self.get_status()

    def _on_topic_created(self, event: TopicCreatedEvent) -> None:
        """Event triggered when a topic was created for this application."""
        logger.debug(f"Kafka credentials are received: {event.username}")
        self.unit.status = self.get_status()
        if not self.unit.is_leader:
            return


if __name__ == "__main__":
    main(IntegratorCharm)
