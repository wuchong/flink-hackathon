/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * The interface for SwitchableSource. It acts like a factory class that helps switch
 * the {@link Source}.
 *
 * @param <StartStateT> The type of start state switched by the source.
 * @param <EndStateT>   The type of end state switched by the source.
 */
@PublicEvolving
public interface SwitchableSource<StartStateT, EndStateT> extends Serializable {

	/**
	 * Set the start state of the source with the StartStateT type.
	 *
	 * @param startState the start state of the source.
	 */
	void setStartState(StartStateT startState);

	/**
	 * Get the end state of the source with the EndStateT type.
	 *
	 * @param endState the end state of the source.
	 */
	EndStateT getEndState(EndStateT endState);
}
