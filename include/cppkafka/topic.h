#ifndef CPPKAFKA_TOPIC_H
#define CPPKAFKA_TOPIC_H

#include <string>
#include <memory>
#include <boost/optional.hpp>
#include <librdkafka/rdkafka.h>

namespace cppkafka {

class Topic {
public:
    static Topic make_non_owning(rd_kafka_topic_t* handle);

    Topic(rd_kafka_topic_t* handle);

    std::string get_name() const;
    rd_kafka_topic_t* get_handle() const;
private:
    using HandlePtr = std::unique_ptr<rd_kafka_topic_t, decltype(&rd_kafka_topic_destroy)>;

    struct NonOwningTag { };

    Topic(rd_kafka_topic_t* handle, NonOwningTag);

    HandlePtr handle_;
};

} // cppkafka

#endif // CPPKAFKA_TOPIC_H