package org.zenoss.zep.index.impl.solr;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.scheduling.TaskScheduler;
import org.zenoss.zep.Messages;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventArchiveDao;
import org.zenoss.zep.dao.EventSummaryBaseDao;
import org.zenoss.zep.index.IndexedDetailsConfiguration;

public class SolrEventIndexBackendFactory implements FactoryBean<SolrEventIndexBackend>, DisposableBean {

    private static Logger logger = LoggerFactory.getLogger(SolrEventIndexBackendFactory.class);

    private SolrEventIndexBackend backend;

    private boolean enableSolr = false;
    private String solrURL = "";
    private String name = "";
    private EventArchiveDao dao;
    private IndexedDetailsConfiguration config;
    private int shards = 24;
    private int replicationFactor = 1;
    private int maxShardsPerNode = 0;
    private int concurrentUploadQueueSize = 10000;
    private int concurrentThreads = 4;
    private Messages messages;
    private TaskScheduler scheduler;
    private UUIDGenerator uuidGenerator;

    public void setShards(int shards) {
        this.shards = shards;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public void setMaxShardsPerNode(int maxShardsPerNode) {
        this.maxShardsPerNode = maxShardsPerNode;
    }

    public void setConcurrentUploadQueueSize(int concurrentUploadQueueSize) {
        this.concurrentUploadQueueSize = concurrentUploadQueueSize;
    }

    public void setConcurrentThreads(int concurrentThreads) {
        this.concurrentThreads = concurrentThreads;
    }

    public void setMessages(Messages messages) {
        this.messages = messages;
    }

    public void setScheduler(TaskScheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void setUuidGenerator(UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    public void setSolrURL(String solrURL) {
        this.solrURL = solrURL;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDao(EventArchiveDao dao) {
        this.dao = dao;
    }

    public void setConfig(IndexedDetailsConfiguration config) {
        this.config = config;
    }

    public void setEnableSolr(boolean enableSolr) {
        this.enableSolr = enableSolr;
    }

    @Override
    public SolrEventIndexBackend getObject() throws Exception {
        if (backend != null) {
            throw new RuntimeException("SolrEventIndexBackend already created");
        }
        //explicitly check to see if solr indexing should be used
        if (!enableSolr) {
            logger.info("Solr Indexing disabled");
            return null;
        }

        if (Strings.isNullOrEmpty(solrURL)) {
            throw new RuntimeException("Solr URL must be specified");
        }
        if (Strings.isNullOrEmpty(name)) {
            throw new RuntimeException("Name for solr backend must be specified");
        }

        if (dao == null) {
            throw new RuntimeException("EventSummaryBaseDao must be set");
        }

        if (config == null) {
            throw new RuntimeException("IndexedDetailsConfiguration must be set");
        }

        if (messages == null) {
            throw new RuntimeException("Messages must be set");
        }

        if (scheduler == null) {
            throw new RuntimeException("Scheduler must be set");
        }

        if (uuidGenerator == null) {
            throw new RuntimeException("UUIDGenerator must be set");
        }

        logger.info("Attempting to initialize Solr Indexing with URL {}", solrURL);

        if (maxShardsPerNode <= 0)
            maxShardsPerNode = shards;

        backend = new SolrEventIndexBackend(name, solrURL, config, dao, shards, replicationFactor, maxShardsPerNode,
                concurrentUploadQueueSize, concurrentThreads, messages, scheduler, uuidGenerator);
        backend.start();

        return backend;
    }

    @Override
    public Class<SolrEventIndexBackend> getObjectType() {
        return SolrEventIndexBackend.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }


    @Override
    public void destroy() throws Exception {
        if (backend != null) {
            backend.close();
        }
    }
}
