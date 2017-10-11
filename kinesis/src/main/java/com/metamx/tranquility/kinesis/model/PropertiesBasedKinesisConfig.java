/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.metamx.tranquility.kinesis.model;

import com.google.common.collect.ImmutableSet;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import org.skife.config.Config;
import org.skife.config.Default;

/**
 * Configuration object which extends Tranquility configuration with Kinesis specific parameters.
 */
public abstract class PropertiesBasedKinesisConfig extends PropertiesBasedConfig
{
  public PropertiesBasedKinesisConfig()
  {
    super(
        ImmutableSet.of(
            "kinesis.aws.region"
        )
    );
  }

  @Config("kinesis.aws.region")
  @Default("tranquility-kinesis")
  public abstract String getKinesisAwsRegion();
}
