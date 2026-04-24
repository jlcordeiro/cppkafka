#include <stdexcept>
#include <iostream>
#include <csignal>
#include "cppkafka/consumer.h"
#include "cppkafka/configuration.h"
#include "cppkafka/utils/consumer_dispatcher.h"

using std::string;
using std::exception;
using std::cout;
using std::endl;
using std::function;

using cppkafka::Consumer;
using cppkafka::ConsumerDispatcher;
using cppkafka::Configuration;
using cppkafka::Message;
using cppkafka::TopicPartition;
using cppkafka::TopicPartitionList;
using cppkafka::Error;

function<void()> on_signal;

void signal_handler(int) {
    on_signal();
}

// This example uses ConsumerDispatcher, a simple synchronous wrapper over a Consumer
// to allow processing messages using pattern matching rather than writing a loop
// and check if there's a message, if there's an error, etc. 
int main(int argc, char* argv[]) {
    string brokers;
    string topic_name;
    string group_id;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            cout << "Options:\n"
            << "  -b, --brokers <brokers>     Kafka broker list (required)\n"
            << "  -t, --topic <topic>         Topic (required)\n"
            << "  -g, --group-id <group>      Consumer group id (required)\n";
            return 0;
        }
        else if ((arg == "-b" || arg == "--brokers") && i + 1 < argc) {
            brokers = argv[++i];
        }
        else if ((arg == "-t" || arg == "--topic") && i + 1 < argc) {
            topic_name = argv[++i];
        }
        else if ((arg == "-g" || arg == "--group-id") && i + 1 < argc) {
            group_id = argv[++i];
        }
        else {
            cout << "Unknown or invalid argument: " << arg << endl;
            return 1;
        }
    }

    if (brokers.empty() || topic_name.empty() || group_id.empty()) {
        cout << "Error: missing required arguments\n";
        return 1;
    }

    // Construct the configuration
    Configuration config = {
        { "metadata.broker.list", brokers },
        { "group.id", group_id },
        // Disable auto commit
        { "enable.auto.commit", false }
    };

    // Create the consumer
    Consumer consumer(config);

    // Print the assigned partitions on assignment
    consumer.set_assignment_callback([](const TopicPartitionList& partitions) {
        cout << "Got assigned: " << partitions << endl;
    });

    // Print the revoked partitions on revocation
    consumer.set_revocation_callback([](const TopicPartitionList& partitions) {
        cout << "Got revoked: " << partitions << endl;
    });

    // Subscribe to the topic
    consumer.subscribe({ topic_name });

    cout << "Consuming messages from topic " << topic_name << endl;

    // Create a consumer dispatcher
    ConsumerDispatcher dispatcher(consumer);

    // Stop processing on SIGINT
    on_signal = [&]() {
        dispatcher.stop();
    };
    signal(SIGINT, signal_handler);

    // Now run the dispatcher, providing a callback to handle messages, one to handle
    // errors and another one to handle EOF on a partition
    dispatcher.run(
        // Callback executed whenever a new message is consumed
        [&](Message msg) {
            // Print the key (if any)
            if (msg.get_key()) {
                cout << msg.get_key() << " -> ";
            }
            // Print the payload
            cout << msg.get_payload() << endl;
            // Now commit the message
            consumer.commit(msg);
        },
        // Whenever there's an error (other than the EOF soft error)
        [](Error error) {
            cout << "[+] Received error notification: " << error << endl;
        },
        // Whenever EOF is reached on a partition, print this
        [](ConsumerDispatcher::EndOfFile, const TopicPartition& topic_partition) {
            cout << "Reached EOF on partition " << topic_partition << endl;
        }
    );
}
