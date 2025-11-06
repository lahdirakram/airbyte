package io.airbyte.integrations.source.postgres.cdc

import io.airbyte.cdk.command.OpaqueStateValue
import io.airbyte.cdk.read.Stream
import io.airbyte.cdk.read.cdc.CdcPartitionReaderDebeziumOperations
import io.airbyte.cdk.read.cdc.CdcPartitionsCreatorDebeziumOperations
import io.airbyte.cdk.read.cdc.DebeziumOffset
import io.airbyte.cdk.read.cdc.DebeziumPropertiesBuilder
import io.airbyte.cdk.read.cdc.DebeziumRecordKey
import io.airbyte.cdk.read.cdc.DebeziumRecordValue
import io.airbyte.cdk.read.cdc.DebeziumSchemaHistory
import io.airbyte.cdk.read.cdc.DebeziumWarmStartState
import io.airbyte.cdk.read.cdc.DeserializedRecord
import io.airbyte.integrations.source.postgres.cdc.PostgresSourceCdcPosition
import io.airbyte.integrations.source.postgres.config.PostgresSourceConfiguration
import jakarta.inject.Singleton
import org.apache.kafka.connect.source.SourceRecord

@Singleton
class PostgresSourceDebeziumOperations(val config: PostgresSourceConfiguration) :
    CdcPartitionsCreatorDebeziumOperations<PostgresSourceCdcPosition>,
    CdcPartitionReaderDebeziumOperations<PostgresSourceCdcPosition> {

    val commonProperties = mapOf(
        "snapshot.mode" to "initial",
        "publication.autocreate.mode" to "disabled",
        "connector.class" to "io.debezium.connector.postgresql.PostgresConnector",
        "converters" to "datetime",
        "datetime.type" to PostgresDebeziumConverter::class.java.getName(),

    )

    override fun position(offset: DebeziumOffset): PostgresSourceCdcPosition {
        TODO("Not yet implemented")
    }

    override fun generateColdStartOffset(): DebeziumOffset {
        TODO("Not yet implemented")
    }

    override fun generateColdStartProperties(streams: List<Stream>): Map<String, String> {
        DebeziumPropertiesBuilder()
            .with(commonProperties)
            .withStreams(streams)
            .with("plugin.name", config.)
            //.with("plugin.name", "pgoutput")
            .with("slot.name", )
            .with("publication.name", )
            .with("", )
            .with("", )
            .with("", )
            .with("", )
            .with("", )
            .buildMap()



        props.setProperty(
            "plugin.name",
            PostgresUtils.getPluginValue(sourceConfig.get("replication_method")),
        )
        if (sourceConfig.has("snapshot_mode")) {
            // The parameter `snapshot_mode` is passed in test to simulate reading the WAL Logs directly and
            // skip initial snapshot
            props.setProperty("snapshot.mode", sourceConfig.get("snapshot_mode").asText())
        } else {
            props.setProperty("snapshot.mode", "initial")
        }

        props.setProperty(
            "slot.name",
            sourceConfig.get("replication_method").get("replication_slot").asText(),
        )
        props.setProperty(
            "publication.name",
            sourceConfig.get("replication_method").get("publication").asText(),
        )

        props.setProperty("publication.autocreate.mode", "disabled")

        return props
    }

    override fun deserializeState(opaqueStateValue: OpaqueStateValue): DebeziumWarmStartState {
        TODO("Not yet implemented")
    }

    override fun generateWarmStartProperties(streams: List<Stream>): Map<String, String> {
        TODO("Not yet implemented")
    }

    override fun deserializeRecord(
        key: DebeziumRecordKey,
        value: DebeziumRecordValue,
        stream: Stream
    ): DeserializedRecord? {
        TODO("Not yet implemented")
    }

    override fun findStreamNamespace(key: DebeziumRecordKey, value: DebeziumRecordValue): String? {
        TODO("Not yet implemented")
    }

    override fun findStreamName(key: DebeziumRecordKey, value: DebeziumRecordValue): String? {
        TODO("Not yet implemented")
    }

    override fun serializeState(
        offset: DebeziumOffset,
        schemaHistory: DebeziumSchemaHistory?
    ): OpaqueStateValue {
        TODO("Not yet implemented")
    }

    override fun position(recordValue: DebeziumRecordValue): PostgresSourceCdcPosition? {
        TODO("Not yet implemented")
    }

    override fun position(sourceRecord: SourceRecord): PostgresSourceCdcPosition? {
        TODO("Not yet implemented")
    }
}
