#ifndef CPPKAFKA_KAFKA_HANDLE_BASE_H
#define CPPKAFKA_KAFKA_HANDLE_BASE_H

#include <string>
#include <memory>
#include <chrono>
#include <unordered_map>
#include <mutex>
#include <tuple>
#include <librdkafka/rdkafka.h>
#include "metadata.h"
#include "topic_partition.h"
#include "topic_partition_list.h"
#include "topic_configuration.h"
#include "configuration.h"

namespace cppkafka {

class Topic;

class KafkaHandleBase {
public:
    using OffsetTuple = std::tuple<int64_t, int64_t>;

    virtual ~KafkaHandleBase() = default;
    KafkaHandleBase(const KafkaHandleBase&) = delete;
    KafkaHandleBase(KafkaHandleBase&&) = delete;
    KafkaHandleBase& operator=(const KafkaHandleBase&) = delete;
    KafkaHandleBase& operator=(KafkaHandleBase&&) = delete;

    void pause_partitions(const TopicPartitionList& topic_partitions);
    void resume_partitions(const TopicPartitionList& topic_partitions);

    void set_timeout(const std::chrono::milliseconds& timeout);

    OffsetTuple query_offsets(const std::string& topic, int partition) const;

    rd_kafka_t* get_handle() const;
    Topic get_topic(const std::string& name);
    Topic get_topic(const std::string& name, TopicConfiguration config);
    Metadata get_metadata() const;
    Metadata get_metadata(const Topic& topic) const;
    std::string get_name() const;
    std::chrono::milliseconds get_timeout() const;
    const Configuration& get_configuration() const;
protected:
    KafkaHandleBase(Configuration config);

    void set_handle(rd_kafka_t* handle);
    void check_error(rd_kafka_resp_err_t error) const;
    rd_kafka_conf_t* get_configuration_handle();
private:
    static const std::chrono::milliseconds DEFAULT_TIMEOUT;

    using HandlePtr = std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)>;
    using TopicConfigurationMap = std::unordered_map<std::string, TopicConfiguration>;

    Topic get_topic(const std::string& name, rd_kafka_topic_conf_t* conf);
    Metadata get_metadata(rd_kafka_topic_t* topic_ptr) const;
    void save_topic_config(const std::string& topic_name, TopicConfiguration config);

    HandlePtr handle_;
    std::chrono::milliseconds timeout_ms_;
    Configuration config_;
    TopicConfigurationMap topic_configurations_;
    std::mutex topic_configurations_mutex_;
};

} // cppkafka

#endif // CPPKAFKA_KAFKA_HANDLE_BASE_H