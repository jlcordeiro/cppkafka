#include "configuration.h"
#include <vector>
#include <librdkafka/rdkafka.h>
#include "exceptions.h"
#include "message.h"
#include "producer.h"
#include "consumer.h"

using std::string;
using std::move;
using std::vector;

using boost::optional;

using std::chrono::milliseconds;

namespace cppkafka {

// Callback proxies

void delivery_report_callback_proxy(rd_kafka_t*, const rd_kafka_message_t* msg, void *opaque) {
    Producer* handle = static_cast<Producer*>(opaque);
    Message message = Message::make_non_owning((rd_kafka_message_t*)msg);
    const auto& callback = handle->get_configuration().get_delivery_report_callback();
    if (callback) {
        callback(*handle, message);
    }
}

void offset_commit_callback_proxy(rd_kafka_t*, rd_kafka_resp_err_t err,
                                  rd_kafka_topic_partition_list_t *offsets, void *opaque) {
    Consumer* handle = static_cast<Consumer*>(opaque);
    TopicPartitionList list = offsets ? convert(offsets) : TopicPartitionList{};
    const auto& callback = handle->get_configuration().get_offset_commit_callback();
    if (callback) {
        callback(*handle, err, list);
    }
}

void error_callback_proxy(rd_kafka_t*, int err, const char *reason, void *opaque) {
    KafkaHandleBase* handle = static_cast<KafkaHandleBase*>(opaque);
    const auto& callback = handle->get_configuration().get_error_callback();
    if (callback) {
        callback(*handle, err, reason);
    }
}

void throttle_callback_proxy(rd_kafka_t*, const char* broker_name,
                              int32_t broker_id, int throttle_time_ms, void *opaque) {
    KafkaHandleBase* handle = static_cast<KafkaHandleBase*>(opaque);
    const auto& callback = handle->get_configuration().get_throttle_callback();
    if (callback) {
        callback(*handle, broker_name, broker_id, milliseconds(throttle_time_ms));
    }
}

void log_callback_proxy(const rd_kafka_t* h, int level,
                        const char* facility, const char* message) {
    KafkaHandleBase* handle = static_cast<KafkaHandleBase*>(rd_kafka_opaque(h));
    const auto& callback = handle->get_configuration().get_log_callback();
    if (callback) {
        callback(*handle, level, facility, message);
    }
}

int stats_callback_proxy(rd_kafka_t*, char *json, size_t json_len, void *opaque) {
    KafkaHandleBase* handle = static_cast<KafkaHandleBase*>(opaque);
    const auto& callback = handle->get_configuration().get_stats_callback();
    if (callback) {
        callback(*handle, string(json, json + json_len));
    }
    return 0;
}

int socket_callback_proxy(int domain, int type, int protocol, void* opaque) {
    KafkaHandleBase* handle = static_cast<KafkaHandleBase*>(opaque);
    const auto& callback = handle->get_configuration().get_socket_callback();
    if (callback) {
        return callback(domain, type, protocol);
    }
    else {
        return -1;
    }
}

// Configuration

Configuration::Configuration() 
: handle_(make_handle(rd_kafka_conf_new())) {

}

Configuration::Configuration(rd_kafka_conf_t* ptr) 
: handle_(make_handle(ptr)) {

}

void Configuration::set(const string& name, const string& value) {
    char error_buffer[512];
    rd_kafka_conf_res_t result;
    result = rd_kafka_conf_set(handle_.get(), name.data(), value.data(), error_buffer,
                               sizeof(error_buffer));
    if (result != RD_KAFKA_CONF_OK) {
        throw ConfigException(name, error_buffer);
    }
}

void Configuration::set_delivery_report_callback(DeliveryReportCallback callback) {
    delivery_report_callback_ = move(callback);
    rd_kafka_conf_set_dr_msg_cb(handle_.get(), &delivery_report_callback_proxy);
}

void Configuration::set_offset_commit_callback(OffsetCommitCallback callback) {
    offset_commit_callback_ = move(callback);
    rd_kafka_conf_set_offset_commit_cb(handle_.get(), &offset_commit_callback_proxy);
}

void Configuration::set_error_callback(ErrorCallback callback) {
    error_callback_ = move(callback);
    rd_kafka_conf_set_error_cb(handle_.get(), &error_callback_proxy);
}

void Configuration::set_throttle_callback(ThrottleCallback callback) {
    throttle_callback_ = move(callback);
    rd_kafka_conf_set_throttle_cb(handle_.get(), &throttle_callback_proxy);
}

void Configuration::set_log_callback(LogCallback callback) {
    log_callback_ = move(callback);
    rd_kafka_conf_set_log_cb(handle_.get(), &log_callback_proxy);
}

void Configuration::set_stats_callback(StatsCallback callback) {
    stats_callback_ = move(callback);
    rd_kafka_conf_set_stats_cb(handle_.get(), &stats_callback_proxy);
}

void Configuration::set_socket_callback(SocketCallback callback) {
    socket_callback_ = move(callback);
    rd_kafka_conf_set_socket_cb(handle_.get(), &socket_callback_proxy);
}

void Configuration::set_default_topic_configuration(optional<TopicConfiguration> config) {
    default_topic_config_ = move(config);
}

rd_kafka_conf_t* Configuration::get_handle() const {
    return handle_.get();
}

string Configuration::get(const string& name) const {
    size_t size = 0;
    auto result = rd_kafka_conf_get(handle_.get(), name.data(), nullptr, &size);
    if (result != RD_KAFKA_CONF_OK) {
        throw ConfigOptionNotFound(name);
    }
    vector<char> buffer(size);
    rd_kafka_conf_get(handle_.get(), name.data(), buffer.data(), &size);
    return string(buffer.data());
}

const Configuration::DeliveryReportCallback& Configuration::get_delivery_report_callback() const {
    return delivery_report_callback_;
}

const Configuration::OffsetCommitCallback& Configuration::get_offset_commit_callback() const {
    return offset_commit_callback_;
}

const Configuration::ErrorCallback& Configuration::get_error_callback() const {
    return error_callback_;
}

const Configuration::ThrottleCallback& Configuration::get_throttle_callback() const {
    return throttle_callback_;
}

const Configuration::LogCallback& Configuration::get_log_callback() const {
    return log_callback_;
}

const Configuration::StatsCallback& Configuration::get_stats_callback() const {
    return stats_callback_;
}

const Configuration::SocketCallback& Configuration::get_socket_callback() const {
    return socket_callback_;
}

const optional<TopicConfiguration>& Configuration::get_default_topic_configuration() const {
    return default_topic_config_;
}

optional<TopicConfiguration>& Configuration::get_default_topic_configuration() {
    return default_topic_config_;
}

Configuration::HandlePtr Configuration::make_handle(rd_kafka_conf_t* ptr) {
    return HandlePtr(ptr, &rd_kafka_conf_destroy, &rd_kafka_conf_dup);
}     

} // cppkafka