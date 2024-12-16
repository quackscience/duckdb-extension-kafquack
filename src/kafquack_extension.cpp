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
    string security_protocol;
    string sasl_mechanism;
    string username;
    string password;
    unique_ptr<cppkafka::Consumer> consumer;
    bool running = true;
    
    std::mutex buffer_mutex;
    std::vector<cppkafka::Message> message_buffer;
    static constexpr size_t BUFFER_SIZE = 1000;

    unique_ptr<FunctionData> Copy() const override {
        auto result = make_uniq<KafkaConsumerData>();
        result->brokers = brokers;
        result->topic = topic;
        result->group_id = group_id;
        result->security_protocol = security_protocol;
        result->sasl_mechanism = sasl_mechanism;
        result->username = username;
        result->password = password;
        return result;
    }

    bool Equals(const FunctionData &other_p) const override {
        auto &other = other_p.Cast<KafkaConsumerData>();
        return brokers == other.brokers && 
               topic == other.topic &&
               group_id == other.group_id &&
               security_protocol == other.security_protocol &&
               sasl_mechanism == other.sasl_mechanism &&
               username == other.username &&
               password == other.password;
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
    auto result = make_uniq<KafkaConsumerData>();
    result->brokers = input.inputs[0].GetValue<string>();
    result->topic = input.inputs[1].GetValue<string>();
    result->group_id = input.inputs[2].GetValue<string>();

    for (auto &kv : input.named_parameters) {
        if (kv.first == "security_protocol") {
            result->security_protocol = StringValue::Get(kv.second);
            if (result->security_protocol != "SASL_SSL" && result->security_protocol != "SASL_PLAINTEXT") {
                throw InvalidInputException("security_protocol must be either SASL_SSL or SASL_PLAINTEXT");
            }
        } else if (kv.first == "sasl_mechanism") {
            result->sasl_mechanism = StringValue::Get(kv.second);
            if (result->sasl_mechanism != "SCRAM-SHA-256" && result->sasl_mechanism != "PLAIN") {
                throw InvalidInputException("sasl_mechanism must be either SCRAM-SHA-256 or PLAIN");
            }
        } else if (kv.first == "username") {
            result->username = StringValue::Get(kv.second);
        } else if (kv.first == "password") {
            result->password = StringValue::Get(kv.second);
        } else {
            throw InvalidInputException("Unknown named parameter: %s", kv.first);
        }
    }

    if (!result->security_protocol.empty() && (result->username.empty() || result->password.empty())) {
        throw InvalidInputException("username and password are required when security_protocol is set");
    }

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

    try {
        cppkafka::Configuration config;
        config.set("metadata.broker.list", result->brokers);
        config.set("group.id", result->group_id);
        config.set("enable.auto.commit", false);

        if (!result->security_protocol.empty()) {
            config.set("security.protocol", result->security_protocol);
            config.set("sasl.mechanisms", result->sasl_mechanism.empty() ? "PLAIN" : result->sasl_mechanism);
            config.set("sasl.username", result->username);
            config.set("sasl.password", result->password);
        }

        result->consumer = make_uniq<cppkafka::Consumer>(config);
        result->consumer->subscribe({result->topic});
    } catch (const cppkafka::Exception &e) {
        throw InvalidInputException("Failed to create Kafka consumer: %s", e.what());
    }

    return result;
}

static unique_ptr<GlobalTableFunctionState> KafkaConsumerInit(ClientContext &context,
                                                            TableFunctionInitInput &input) {
    return make_uniq<KafkaConsumerGlobalState>();
}

static void KafkaConsumerFunction(ClientContext &context, TableFunctionInput &data_p, 
                                DataChunk &output) {
    auto &data = data_p.bind_data->Cast<KafkaConsumerData>();
    
    vector<cppkafka::Message> messages;
    
    idx_t chunk_size = 0;
    while (chunk_size < STANDARD_VECTOR_SIZE && data.running) {
        try {
            auto msg = data.consumer->poll(std::chrono::milliseconds(100));
            if (!msg) {
                continue;
            }
            messages.push_back(std::move(msg));
            chunk_size++;
        } catch (const cppkafka::Exception &e) {
            throw InvalidInputException("Error polling Kafka: %s", e.what());
        }
    }

    if (chunk_size == 0) {
        output.SetCardinality(0);
        return;
    }

    output.SetCardinality(chunk_size);
    
    for (idx_t i = 0; i < chunk_size; i++) {
        const auto &msg = messages[i];
        
        output.data[0].SetValue(i, Value(msg.get_topic()));
        output.data[1].SetValue(i, Value::INTEGER(msg.get_partition()));
        output.data[2].SetValue(i, Value::BIGINT(msg.get_offset()));
        
        auto ts = msg.get_timestamp();
        if (ts) {
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(ts.get().get_timestamp());
            output.data[3].SetValue(i, Value::TIMESTAMP(Timestamp::FromEpochMs(ms.count())));
        } else {
            FlatVector::SetNull(output.data[3], i, true);
        }
        
        if (msg.get_key()) {
            string key_str(reinterpret_cast<const char*>(msg.get_key().get_data()), 
                         msg.get_key().get_size());
            output.data[4].SetValue(i, Value(key_str));
        } else {
            FlatVector::SetNull(output.data[4], i, true);
        }
        
        if (!msg.get_error()) {
            string value_str(reinterpret_cast<const char*>(msg.get_payload().get_data()), 
                           msg.get_payload().get_size());
            output.data[5].SetValue(i, Value(value_str));
            FlatVector::SetNull(output.data[6], i, true);
        } else {
            FlatVector::SetNull(output.data[5], i, true);
            output.data[6].SetValue(i, Value(msg.get_error().to_string()));
        }

        try {
            data.consumer->commit(msg);
        } catch (const cppkafka::Exception &e) {
            throw InvalidInputException("Error committing message: %s", e.what());
        }
    }
}

class KafquackExtension : public Extension {
public:
    void Load(DuckDB &db) override {
        vector<LogicalType> args = {
            LogicalType::VARCHAR,    // brokers
            LogicalType::VARCHAR,    // topic
            LogicalType::VARCHAR     // group_id
        };
        
        named_parameter_type_map_t named_params = {
            {"security_protocol", LogicalType::VARCHAR},
            {"sasl_mechanism", LogicalType::VARCHAR},
            {"username", LogicalType::VARCHAR},
            {"password", LogicalType::VARCHAR}
        };
        
        TableFunction kafka_consumer("kafka_consumer", args, KafkaConsumerFunction, 
                                   KafkaConsumerBind, KafkaConsumerInit);
        kafka_consumer.named_parameters = named_params;
        kafka_consumer.projection_pushdown = false;
        kafka_consumer.filter_pushdown = false;
        
        ExtensionUtil::RegisterFunction(*db.instance, kafka_consumer);
    }

    std::string Name() override {
        return "kafquack";
    }

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
DUCKDB_EXTENSION_API void kafquack_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::KafquackExtension>();
}

DUCKDB_EXTENSION_API const char *kafquack_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}
