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
public class DefaultBufferAllocator {
  private static final Logger logger = LoggerFactory.getLogger(DefaultBufferAllocator.class);

  /**
   * The environmental variable to set the default allocation manager type.
   */
  public static final String ALLOCATOR_TYPE_ENV_NAME = "ARROW_ALLOCATOR_TYPE";

  /**
   * The system property to set the default allocation manager type.
   */
  public static final String ALLOCATOR_TYPE_PROPERTY_NAME = "arrow.allocator.type";


  public static BufferAllocator create(long memory) {
    return getDefaultAllocatorFactory().create(BaseAllocator.configBuilder().maxAllocation(memory).build());
  }

  public static BufferAllocator create() {
    return getDefaultAllocatorFactory().create(BaseAllocator.defaultConfig());
  }

  public static BufferAllocator create(BaseAllocator.Config config) {
    return getDefaultAllocatorFactory().create(config);
  }

  public static BufferAllocator create(AllocatorType type) {
    return getAllocatorFactory(type).create(BaseAllocator.defaultConfig());
  }

  public static BufferAllocator create(AllocatorType type, BaseAllocator.Config config) {
    return getAllocatorFactory(type).create(config);
  }

  /**
   * The allocation manager type.
   */
  public enum AllocatorType {
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
  }

  private static String getDefaultAllocationManagerName() {
    String envValue = System.getenv(ALLOCATOR_TYPE_ENV_NAME);
    String propValue = System.getProperty(ALLOCATOR_TYPE_PROPERTY_NAME);
    // system property takes precedence
    return propValue == null ? envValue : propValue;
  }

  private static AllocatorType getDefaultAllocatorType() {
    AllocatorType ret = AllocatorType.Unknown;
    String name = getDefaultAllocationManagerName();

    if (name == null) {
      return ret;
    }

    try {
      ret = AllocatorType.valueOf(name);
      return ret;
    } catch (IllegalArgumentException | NullPointerException e) {
      // ignore the exception
      return null;
    }
  }

  private static BaseAllocator.Factory getDefaultAllocatorFactory() {
    AllocatorType type = getDefaultAllocatorType();
    return getAllocatorFactory(type);
  }

  private static BaseAllocator.Factory getAllocatorFactory(AllocatorType type) {
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
      default:
        throw new IllegalStateException("Unknown allocation manager type: " + type);
    }
  }

  private static BaseAllocator.Factory getFactory(String clazzName) {
    try {
      Field field = Class.forName(clazzName).getDeclaredField("FACTORY");
      field.setAccessible(true);
      return (BaseAllocator.Factory) field.get(null);
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate Allocation Manager for " + clazzName, e);
    }
  }

  private static BaseAllocator.Factory getUnsafeFactory() {
    return getFactory("org.apache.arrow.memory.UnsafeAllocatorFactory");
  }

  private static BaseAllocator.Factory getNettyFactory() {
    return getFactory("org.apache.arrow.memory.NettyAllocatorFactory");
  }
}
