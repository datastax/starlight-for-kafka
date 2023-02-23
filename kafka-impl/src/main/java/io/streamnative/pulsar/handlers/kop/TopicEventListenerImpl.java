package io.streamnative.pulsar.handlers.kop;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.TopicEventsListener;
import org.apache.pulsar.common.naming.TopicName;

/**
 * This is a listener to receive notifications about UNLOAD and DELETE events.
 * The TopicEventsListener API is available only since Pulsar 3.0.0
 * and Luna Streaming 2.10.3.3. This is the reason why this class is not an innerclass
 * of {@link NamespaceBundleOwnershipListenerImpl}, because we don't want to load it and
 * cause errors on older versions of Pulsar.
 *
 * Please note that we are not interested in LOAD events because they are handled
 * in {@link NamespaceBundleOwnershipListenerImpl} in a different way.
 */
@Slf4j
class TopicEventListenerImpl implements TopicEventsListener {

    final NamespaceBundleOwnershipListenerImpl parent;

    public TopicEventListenerImpl(NamespaceBundleOwnershipListenerImpl parent) {
        this.parent = parent;
    }

    @Override
    public void handleEvent(String topicName, TopicEvent event, EventStage stage, Throwable t) {
        if (parent.isClosed()) {
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("handleEvent {} {} on {}", event, stage, topicName);
        }
        if (stage == EventStage.SUCCESS || stage == EventStage.FAILURE) {
            TopicName topicName1 = TopicName.get(topicName);
            switch (event) {
                case UNLOAD:
                    parent.notifyUnloadTopic(topicName1.getNamespaceObject(), topicName1);
                    break;
                case DELETE:
                    parent.notifyDeleteTopic(topicName1.getNamespaceObject(), topicName1);
                    break;
                default:
                    if (log.isDebugEnabled()) {
                        log.debug("Ignore event {} {} on {}", event, stage, topicName);
                    }
                    break;
            }
        }
    }
}
