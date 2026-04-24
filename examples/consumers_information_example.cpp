#include <stdexcept>
#include <iostream>
#include "cppkafka/producer.h"
#include "cppkafka/configuration.h"
#include "cppkafka/group_information.h"
#include "cppkafka/topic.h"

using std::string;
using std::exception;
using std::vector;
using std::cout;
using std::endl;

using cppkafka::Producer;
using cppkafka::Exception;
using cppkafka::Configuration;
using cppkafka::Topic;
using cppkafka::GroupInformation;
using cppkafka::GroupMemberInformation;
using cppkafka::MemberAssignmentInformation;

int main(int argc, char* argv[]) {
    string brokers;
    string group_id;
    bool show_assignment = false;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            cout << "Options:\n"
            << "  -b, --brokers <brokers>        Kafka broker list (required)\n"
            << "  -g, --group-id <group>         Consumer group id (optional)\n"
            << "  -a, --assignment               Show topic/partition assignment\n";
            return 0;
        }
        else if ((arg == "-b" || arg == "--brokers") && i + 1 < argc) {
            brokers = argv[++i];
        }
        else if ((arg == "-g" || arg == "--group-id") && i + 1 < argc) {
            group_id = argv[++i];
        }
        else if (arg == "-a" || arg == "--assignment") {
            show_assignment = true;
        }
        else {
            cout << "Unknown or invalid argument: " << arg << endl;
            return 1;
        }
    }

    if (brokers.empty()) {
        cout << "Error: brokers are required\n";
        return 1;
    }

    // Construct the configuration
    Configuration config = {
        { "metadata.broker.list", brokers },
        // Disable auto commit
        { "enable.auto.commit", false }
    };

    try {
        // Construct a producer
        Producer producer(config);

        // Fetch the group information
        vector<GroupInformation> groups = [&]() {
            if (!group_id.empty()) {
                return vector<GroupInformation>{producer.get_consumer_group(group_id)};
            }
            else {
                return producer.get_consumer_groups();
            }
        }();

        if (groups.empty()) {
            cout << "Found no consumers" << endl;
            return 0;
        }
        cout << "Found the following consumers: " << endl;
        for (const GroupInformation& group : groups) {
            cout << "* \"" << group.get_name() << "\" having the following (" <<
                    group.get_members().size() << ") members: " << endl;
            for (const GroupMemberInformation& info : group.get_members()) {
                cout << "    - " << info.get_member_id() << " @ " << info.get_client_host();
                if (show_assignment) {
                    MemberAssignmentInformation assignment(info.get_member_assignment());
                    cout << " has assigned: " << assignment.get_topic_partitions();
                }
                cout << endl;
            }
            cout << endl;
        }
    }
    catch (const Exception& ex) {
        cout << "Error fetching group information: " << ex.what() << endl;
    }
}
