package org.zenoss.zep;


import org.zenoss.protobufs.zep.Zep.ZepConfig;

/**
 * Interface used to return the most up-to-date version of the
 * ZepConfig.
 */
public interface ZepConfigService {
    /**
     * Returns the current up-to-date ZepConfig.
     * 
     * @return The current up-to-date ZepConfig.
     * @throws ZepException Thrown when config cannot be loaded
     *          from the database.
     */
    ZepConfig getConfig() throws ZepException;
}