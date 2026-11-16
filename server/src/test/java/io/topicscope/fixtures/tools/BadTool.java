package io.topicscope.fixtures.tools;

import org.apache.kafka.clients.admin.Admin;

/** A tool class that talks to a broker itself. Never run; see {@code BadExamples}. */
final class BadTool {

  Object look(Admin admin) {
    return admin.listTopics();
  }
}
