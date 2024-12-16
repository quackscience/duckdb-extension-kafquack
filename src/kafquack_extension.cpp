#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "cppkafka/consumer.h"
#include "cppkafka/configuration.h"
#include <thread>
#include <mutex>

namespace duckdb {

struct KafkaConsumerData : public TableFunctionData {
    string brokers;
    string topic;
    string group_id;
    unique_ptr<cppkafka::Consumer> consumer;
    bool running = true;
    
    // Buffer for messages
    std::mutex buffer_mutex;
    std::vector<cppkafka::Message> message_buffer;
    static constexpr size_t BUFFER_SIZE = 1000;

    KafkaConsumerData(string brokers, string topic, string group_id) 
        : brokers(brokers), topic(topic), group_id(group_id) {
        
        // Configure Kafka consumer
        cppkafka::Configuration config = {
            { "metadata.broker.list", brokers },
            { "group.id", group_id },
            { "enable.auto.commit", false }
        };

        // Create consumer
        consumer = make_uniq<cppkafka::Consumer>(config);
        consumer->subscribe({topic});
    }

    ~KafkaConsumerData() {
        running = false;
        if (consumer) {
            consumer->unsubscribe();
        }
    }
};

struct KafkaConsumerGlobalState : public GlobalTableFunctionState {
    KafkaConsumerGlobalState() {}

    idx_t MaxThreads() const override {
        return 1;
    }
};

static unique_ptr<FunctionData> KafkaConsumerBind(ClientContext &context, 
                                                 TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, 
                                                 vector<string> &names) {
    
    auto brokers = input.inputs[0].GetValue<string>();
    auto topic = input.inputs[1].GetValue<string>();
    auto group_id = input.inputs[2].GetValue<string>();

    // Define the schema for our Kafka messages
    names = {"topic", "partition", "offset", "timestamp", "key", "value", "error"};
    return_types = {
        LogicalType::VARCHAR,    // topic
        LogicalType::INTEGER,    // partition
        LogicalType::BIGINT,     // offset
        LogicalType::TIMESTAMP,  // timestamp
        LogicalType::VARCHAR,    // key
        LogicalType::VARCHAR,    // value
        LogicalType::VARCHAR     // error
    };

    return make_uniq<KafkaConsumerData>(brokers, topic, group_id);
}

static unique_ptr<GlobalTableFunctionState> KafkaConsumerInit(ClientContext &context,
                                                            TableFunctionInitInput &input) {
    return make_uniq<KafkaConsumerGlobalState>();
}

static void KafkaConsumerFunction(ClientContext &context, TableFunctionInput &data_p, 
                                DataChunk &output) {
    auto &data = data_p.bind_data->Cast<KafkaConsumerData>();
    
    // Vector to store the messages for this chunk
    vector<cppkafka::Message> messages;
    
    // Try to get messages up to the vector size
    idx_t chunk_size = 0;
    while (chunk_size < STANDARD_VECTOR_SIZE && data.running) {
        auto msg = data.consumer->poll(std::chrono::milliseconds(100));
        if (!msg) {
            continue;
        }
        
        messages.push_back(std::move(msg));
        chunk_size++;
    }

    if (chunk_size == 0) {
        // No messages available
        output.SetCardinality(0);
        return;
    }

    // Set up the output vectors
    output.SetCardinality(chunk_size);
    
    for (idx_t i = 0; i < chunk_size; i++) {
        const auto &msg = messages[i];
        
        // topic
        output.data[0].SetValue(i, Value(msg.get_topic()));
        
        // partition
        output.data[1].SetValue(i, Value::INTEGER(msg.get_partition()));
        
        // offset
        output.data[2].SetValue(i, Value::BIGINT(msg.get_offset()));
        
        // timestamp
        auto ts = msg.get_timestamp();
        if (ts) {
            // Get milliseconds since epoch and convert to timestamp
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(ts.get().get_timestamp());
            output.data[3].SetValue(i, Value::TIMESTAMP(Timestamp::FromEpochMs(ms.count())));
        } else {
            FlatVector::SetNull(output.data[3], i, true);
        }
        
        // key
        if (msg.get_key()) {
            string key_str(reinterpret_cast<const char*>(msg.get_key().get_data()), 
                         msg.get_key().get_size());
            output.data[4].SetValue(i, Value(key_str));
        } else {
            FlatVector::SetNull(output.data[4], i, true);
        }
        
        // value and error
        if (!msg.get_error()) {
            string value_str(reinterpret_cast<const char*>(msg.get_payload().get_data()), 
                           msg.get_payload().get_size());
            output.data[5].SetValue(i, Value(value_str));
            FlatVector::SetNull(output.data[6], i, true);
        } else {
            FlatVector::SetNull(output.data[5], i, true);
            output.data[6].SetValue(i, Value(msg.get_error().to_string()));
        }

        // Commit the message
        data.consumer->commit(msg);
    }
}

class KafquackExtension : public Extension {  // Changed from KafkaExtension to KafquackExtension
public:
    void Load(DuckDB &db) override {
        // Register the Kafka consumer function
        vector<LogicalType> args = {
            LogicalType::VARCHAR,    // brokers
            LogicalType::VARCHAR,    // topic
            LogicalType::VARCHAR     // group_id
        };
        
        TableFunction kafka_consumer("kafka_consumer", args, KafkaConsumerFunction, 
                                   KafkaConsumerBind, KafkaConsumerInit);
        
        // Set parallel properties
        kafka_consumer.projection_pushdown = false;
        kafka_consumer.filter_pushdown = false;
        
        ExtensionUtil::RegisterFunction(*db.instance, kafka_consumer);
    }

    std::string Name() override {
        return "kafquack";  // Changed from "kafka" to "kafquack"
    }

    // Add Version method implementation
    std::string Version() const override {
#ifdef KAFQUACK_VERSION
        return KAFQUACK_VERSION;
#else
        return "0.0.1";
#endif
    }
};

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void kafquack_init(duckdb::DatabaseInstance &db) {  // Changed from kafka_init
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::KafquackExtension>();
}

DUCKDB_EXTENSION_API const char *kafquack_version() {  // Changed from kafka_version
    return duckdb::DuckDB::LibraryVersion();
}
}
