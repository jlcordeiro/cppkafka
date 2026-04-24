#include <stdexcept>
#include <iostream>
#include "cppkafka/utils/buffered_producer.h"
#include "cppkafka/configuration.h"

using std::string;
using std::exception;
using std::getline;
using std::cin;
using std::cout;
using std::endl;

using cppkafka::BufferedProducer;
using cppkafka::Configuration;
using cppkafka::Topic;
using cppkafka::MessageBuilder;
using cppkafka::Message;

int main(int argc, char* argv[]) {
    string brokers;
    string topic_name;
    int partition_value = -1;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            cout << "Options:\n"
            << "  -b, --brokers <brokers>      Kafka broker list (required)\n"
            << "  -t, --topic <topic>          Topic to write to (required)\n"
            << "  -p, --partition <int>        Partition (optional)\n";
            return 0;
        }
        else if ((arg == "-b" || arg == "--brokers") && i + 1 < argc) {
            brokers = argv[++i];
        }
        else if ((arg == "-t" || arg == "--topic") && i + 1 < argc) {
            topic_name = argv[++i];
        }
        else if ((arg == "-p" || arg == "--partition") && i + 1 < argc) {
            partition_value = std::stoi(argv[++i]);
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

    // Create a message builder for this topic
    MessageBuilder builder(topic_name);

    // Get the partition we want to write to. If no partition is provided, this will be
    // an unassigned one
    if (partition_value != -1) {
        builder.partition(partition_value);
    }

    // Construct the configuration
    Configuration config = {
        { "metadata.broker.list", brokers }
    };

    // Create the producer
    BufferedProducer<string> producer(config);

    // Set a produce success callback
    producer.set_produce_success_callback([](const Message& msg) {
        cout << "Successfully produced message with payload " << msg.get_payload() << endl;
    });
    // Set a produce failure callback
    producer.set_produce_failure_callback([](const Message& msg) {
        cout << "Failed to produce message with payload " << msg.get_payload() << endl;
        // Return false so we stop trying to produce this message
        return false;
    });

    cout << "Producing messages into topic " << topic_name << endl;

    // Now read lines and write them into kafka
    string line;
    while (getline(cin, line)) {
        // Set the payload on this builder
        builder.payload(line);

        // Add the message we've built to the buffered producer
        producer.add_message(builder);

        // Now flush so we:
        // * emit the buffered message
        // * poll the producer so we dispatch on delivery report callbacks and
        // therefore get the produce failure/success callbacks
        producer.flush();
    }
}
