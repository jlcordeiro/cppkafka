#include <stdexcept>
#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include "cppkafka/consumer.h"
#include "cppkafka/configuration.h"

using std::string;
using std::exception;
using std::cout;
using std::endl;

using cppkafka::Consumer;
using cppkafka::Configuration;
using cppkafka::Message;
using cppkafka::TopicPartitionList;

namespace po = boost::program_options;

bool running = true;

int main(int argc, char* argv[]) {
    string brokers;
    string topic_name;
    string group_id;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            cout << "Options:\n"
            << "  -b, --brokers <brokers>     Kafka broker list (required)\n"
            << "  -t, --topic <topic>         Topic to use (required)\n"
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

    if (brokers.empty()) {
        cout << "Error: brokers is required\n";
        return 1;
    }
    if (topic_name.empty()) {
        cout << "Error: topic is required\n";
        return 1;
    }
    if (group_id.empty()) {
        cout << "Error: group-id is required\n";
        return 1;
    }

    // Stop processing on SIGINT
    signal(SIGINT, [](int) { running = false; });

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

    // Now read lines and write them into kafka
    while (running) {
        // Try to consume a message
        Message msg = consumer.poll();
        if (msg) {
            // If we managed to get a message
            if (msg.get_error()) {
                // Ignore EOF notifications from rdkafka
                if (!msg.is_eof()) {
                    cout << "[+] Received error notification: " << msg.get_error() << endl;
                }
            }
            else {
                // Print the key (if any)
                if (msg.get_key()) {
                    cout << msg.get_key() << " -> ";
                }
                // Print the payload
                cout << msg.get_payload() << endl;
                // Now commit the message
                consumer.commit(msg);
            }
        }
    }
}
