package org.zenoss.zep.rest;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.annotations.GZIP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.zenoss.protobufs.ProtobufConstants;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventDetailSet;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscription;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventSignalSpool;
import org.zenoss.zep.dao.EventSignalSpoolDao;
import org.zenoss.zep.dao.EventTriggerSubscriptionDao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Path("1.0/incman")
public class IncManResource {
    private static final Logger logger = LoggerFactory.getLogger(IncManResource.class);

    public class TriggerSubscriptionNotFound extends ZepException {}

    private EventTriggerSubscriptionDao eventTriggerSubscriptionDao;
    private EventSignalSpoolDao eventSignalSpoolDao;

    @Autowired
    private void setEventTriggerSubscriptionDao(EventTriggerSubscriptionDao eventTriggerSubscriptionDao) {
        this.eventTriggerSubscriptionDao = eventTriggerSubscriptionDao;
    }

    @Autowired
    private void setEventSignalSpoolDao(EventSignalSpoolDao eventSignalSpoolDao) {
        this.eventSignalSpoolDao = eventSignalSpoolDao;
    }

    private String getTriggerSubscriptionUuid(String triggerUuid, String notificationUuid) throws ZepException {
        List<EventTriggerSubscription> triggerSubscriptions =
            this.eventTriggerSubscriptionDao.findAll();

        logger.debug("Searching for " + triggerUuid + "/" + notificationUuid);
        for (EventTriggerSubscription triggerSubscription :
                    triggerSubscriptions) {
            logger.debug("\tComparing to " + triggerSubscription.getTriggerUuid() + "/" + triggerSubscription.getSubscriberUuid());

            if ((triggerSubscription.getTriggerUuid().equals(triggerUuid)) &&
                    (triggerSubscription.getSubscriberUuid().equals(notificationUuid))) {
                logger.debug("Found!");
                return triggerSubscription.getUuid();
            }
        }
        logger.debug("Not found!");
        throw new TriggerSubscriptionNotFound();
    }

    @POST
    @Path("trigger_spool")
    @Produces({ MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    @GZIP
    public Response insertTriggerSpool(EventDetailSet update) throws ZepException {
        EventSignalSpool spool;
        Map<String, String> updatesByKey = new HashMap<String, String>();

        for (EventDetail detail : update.getDetailsList()) {
            updatesByKey.put(detail.getName(), detail.getValue(0));
        }

        String triggerSubscriptionUuid = this.getTriggerSubscriptionUuid(
            updatesByKey.get("trigger_uuid"), updatesByKey.get("notification_uuid"));

        spool = new EventSignalSpool();
        spool.setSubscriptionUuid(triggerSubscriptionUuid);
        spool.setEventSummaryUuid(updatesByKey.get("event_summary_uuid"));
        spool.setSentSignal(true);
        spool.setCreated(System.currentTimeMillis());
        spool.setFlushTime(Long.MAX_VALUE);

        this.eventSignalSpoolDao.create(spool);

        return Response.noContent().build();
    }

}
