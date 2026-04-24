#include <stdexcept>
#include <iostream>
#include "cppkafka/producer.h"
#include "cppkafka/configuration.h"
#include "cppkafka/metadata.h"
#include "cppkafka/topic.h"

using std::string;
using std::exception;
using std::cout;
using std::endl;

using cppkafka::Producer;
using cppkafka::Exception;
using cppkafka::Configuration;
using cppkafka::Topic;
using cppkafka::Metadata;
using cppkafka::TopicMetadata;
using cppkafka::BrokerMetadata;

static void print_usage() {
    cout << "Usage:\n"
    << "  -b, --brokers <broker list>\n";
}

int main(int argc, char* argv[]) {
    string brokers;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            print_usage();
            return 0;
        }
        else if ((arg == "-b" || arg == "--brokers") && i + 1 < argc) {
            brokers = argv[++i];
        }
        else {
            cout << "Unknown or invalid argument: " << arg << endl;
            print_usage();
            return 1;
        }
    }

    if (brokers.empty()) {
        cout << "Error: brokers not specified\n";
        print_usage();
        return 1;
    }

    // Construct the configuration
    Configuration config = {
        { "metadata.broker.list", brokers },
    };

    try {
        // Construct a producer
        Producer producer(config);

        // Fetch the metadata
        Metadata metadata = producer.get_metadata();

        // Iterate over brokers
        cout << "Found the following brokers: " << endl;
        for (const BrokerMetadata& broker : metadata.get_brokers()) {
            cout << "* " << broker.get_host() << endl;
        }
        cout << endl;

        // Iterate over topics
        cout << "Found the following topics: " << endl;
        for (const TopicMetadata& topic : metadata.get_topics()) {
            cout << "* " << topic.get_name() << ": " << topic.get_partitions().size()
                 << " partitions" << endl;
        }
    }
    catch (const Exception& ex) {
        cout << "Error fetching metadata: " << ex.what() << endl;
    }
}
