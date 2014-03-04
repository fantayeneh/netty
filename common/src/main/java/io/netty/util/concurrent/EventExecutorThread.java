/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util.concurrent;

/**
 * Special {@link Thread} which allows to access the tied {@link EventExecutor}
 */
public class EventExecutorThread extends Thread {
    private final EventExecutor executor;

    public EventExecutorThread(Runnable target, String name) {
        super(target, name);
        executor = executor(target);
    }

    public EventExecutorThread(ThreadGroup group, Runnable target, String name, long stackSize) {
        super(group, target, name, stackSize);
        executor = executor(target);
    }

    public EventExecutorThread(ThreadGroup group, Runnable target, String name) {
        super(group, target, name);
        executor = executor(target);
    }

    /**
     * Try to access the {@link EventExecutor} which is used
     */
    private static EventExecutor executor(Runnable runnable) {
        if (runnable instanceof EventExecutorRunnable) {
            return ((EventExecutorRunnable) runnable).executor();
        }
        return null;
    }

    /**
     * Return the {@link EventExecutor} in which this {@link EventExecutorThread} is used.
     */
    public EventExecutor executor() {
        return executor;
    }
}
