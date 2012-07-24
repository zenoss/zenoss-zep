/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao;

import org.zenoss.protobufs.zep.Zep.EventTriggerSubscription;
import org.zenoss.zep.ZepException;

import java.util.List;

/**
 * DAO for EventTriggerSubscription.
 */
public interface EventTriggerSubscriptionDao {
    /**
     * Creates the EventTriggerSubscription in the database.
     * 
     * @param evtTriggerSubscription
     *            EventTriggerSubscription to create.
     * @return The UUID of the created EventTriggerSubscription.
     * @throws ZepException
     *             If the EventTriggerSubscription could not be created.
     */
    public String create(EventTriggerSubscription evtTriggerSubscription)
            throws ZepException;

    /**
     * Deletes the EventTriggerSubscription with the specified UUID from the
     * database.
     * 
     * @param uuid
     *            UUID of the EventTriggerSubscription to delete.
     * @return The number of affected rows.
     * @throws ZepException
     *             If the EventTriggerSubscription could not be deleted.
     */
    public int delete(String uuid) throws ZepException;

    /**
     * Returns all configured subscriptions.
     * 
     * @return All configured subscriptions, or an empty list if no
     *         subscriptions exist.
     * @throws ZepException
     *             If the list of subscriptions couldn't be queried from the
     *             database.
     */
    public List<EventTriggerSubscription> findAll() throws ZepException;

    /**
     * Returns the subscription with the matching UUID, or null if not found.
     * 
     * @param uuid
     *            UUID of EventTriggerSubscription to find.
     * @return The matching EventTriggerSubscription, or null if not found.
     * @throws ZepException
     *             If the database cannot be queried.
     */
    public EventTriggerSubscription findByUuid(String uuid) throws ZepException;

    /**
     * Returns the {@link EventTriggerSubscription} objects for the specified
     * subscriber UUID, or an empty list of none are found.
     * 
     * @param subscriberUuid
     *            The subscriber UUID.
     * @return The subscriptions for the specified subscriber.
     * @throws ZepException
     *             If a database error occurs.
     */
    public List<EventTriggerSubscription> findBySubscriberUuid(
            String subscriberUuid) throws ZepException;

    /**
     * Bulk updates subscriptions for the specified subscriber UUID. Any
     * existing subscriptions for the subscriber not found in the list are
     * removed from the database and any new/updated subscriptions are created
     * or updated in the database.
     * 
     * @param subscriberUuid
     *            The subscriber UUID these subscriptions are for. This should
     *            match the {@link EventTriggerSubscription#getSubscriberUuid()}
     *            of each specified subscription.
     * @param subscriptions
     *            The subscriptions for this subscriber.
     * @return The number of affected rows.
     * @throws ZepException
     *             If the database operation failed.
     */
    public int updateSubscriptions(String subscriberUuid,
            List<EventTriggerSubscription> subscriptions) throws ZepException;

}
