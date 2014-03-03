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

import java.util.concurrent.ThreadFactory;

/**
 * Special Thread which allows to set a {@link Runnable} to be executed after the target {@link Runnable} was run.
 * You should use this in your {@link ThreadFactory} that is used for the {@link EventExecutor} and sub-types if
 * possible.
 */
public final class EventThread extends Thread {
    private Runnable task;

    public EventThread(Runnable target, String name) {
        super(target, name);
    }

    public EventThread(ThreadGroup group, Runnable target, String name, long stackSize) {
        super(group, target, name, stackSize);
    }

    public EventThread(ThreadGroup group, Runnable target, String name) {
        super(group, target, name);
    }

    @Override
    public void run() {
        try {
            super.run();
        } finally {
            if (task != null) {
                task.run();
            }
        }
    }

    /**
     * Must be called from this {@link EventThread} itself
     */
    public void executeAfterRun(Runnable task) {
        assert Thread.currentThread() == this;
        this.task = task;
    }
}
