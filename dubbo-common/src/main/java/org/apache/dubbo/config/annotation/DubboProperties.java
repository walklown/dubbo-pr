/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.config.annotation;

import org.apache.dubbo.config.ConfigScope;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface DubboProperties {

    /**
     * The default value for {@link #repeatable()}
     *
     * @since 1.0.6
     */
    boolean DEFAULT_MULTIPLE = false;

    ConfigScope configScope();

    boolean required() default false;

    /**
     * It indicates whether the config binding to multiple environment.
     *
     * @return the default value is <code>false</code>
     * @see #DEFAULT_MULTIPLE
     */
    boolean repeatable() default DEFAULT_MULTIPLE;
}
