/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.memory;

import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for choosing the default allocation manager.
 */
public class DefaultAllocationManagerOption {
  private static final Logger logger = LoggerFactory.getLogger(DefaultAllocationManagerOption.class);

  /**
   * The environmental variable to set the default allocation manager type.
   */
  public static final String ALLOCATION_MANAGER_TYPE_ENV_NAME = "ARROW_ALLOCATION_MANAGER_TYPE";

  /**
   * The system property to set the default allocation manager type.
   */
  public static final String ALLOCATION_MANAGER_TYPE_PROPERTY_NAME = "arrow.allocation.manager.type";

  /**
   * The default allocation manager factory.
   */
  public static final AllocationManager.Factory DEFAULT_ALLOCATION_MANAGER_FACTORY =
      getDefaultAllocationManagerFactory();

  /**
   * The allocation manager type.
   */
  public enum AllocationManagerType {
    /**
     * Netty based allocation manager.
     */
    Netty,

    /**
     * Unsafe based allocation manager.
     */
    Unsafe,

    /**
     * Unknown type.
     */
    Unknown,

    /**
     * Only used for testing! will raise an error when used.
     */
    Test
  }

  private static String getDefaultAllocationManagerName() {
    String envValue = System.getenv(ALLOCATION_MANAGER_TYPE_ENV_NAME);
    String propValue = System.getProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME);
    // system property takes precedence
    return propValue == null ? envValue : propValue;
  }

  protected static AllocationManagerType getDefaultAllocationManagerType() {
    AllocationManagerType ret = AllocationManagerType.Unknown;
    String name = getDefaultAllocationManagerName();

    if (name == null) {
      return ret;
    }

    try {
      ret = AllocationManagerType.valueOf(name);
      return ret;
    } catch (IllegalArgumentException | NullPointerException e) {
      // ignore the exception
      return null;
    }
  }

  private static AllocationManager.Factory getDefaultAllocationManagerFactory() {
    AllocationManagerType type = getDefaultAllocationManagerType();

    if (type == null) {
      return getFactory(getDefaultAllocationManagerName());
    }
    switch (type) {
      case Netty:
        return getNettyFactory();
      case Unsafe:
        return getUnsafeFactory();
      case Unknown:
        logger.info("allocation manager type not specified, using netty as the default type");
        return getNettyFactory();
      case Test:
        logger.warn("An empty allocator is being used for the default allocator type. Is this correct?");
        return (accountingAllocator, size) -> {
          return null; //empty allocation manager for tests only
        };
      default:
        return getFactory(getDefaultAllocationManagerName());
    }
  }

  private static AllocationManager.Factory getFactory(String clazzName) {
    try {
      Field field = Class.forName(clazzName).getDeclaredField("FACTORY");
      field.setAccessible(true);
      return (AllocationManager.Factory) field.get(null);
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate Allocation Manager for " + clazzName, e);
    }
  }

  private static AllocationManager.Factory getUnsafeFactory() {
    return getFactory("org.apache.arrow.memory.UnsafeAllocationManager");
  }

  private static AllocationManager.Factory getNettyFactory() {
    return getFactory("org.apache.arrow.memory.NettyAllocationManager");
  }
}
