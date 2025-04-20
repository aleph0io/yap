/*-
 * =================================LICENSE_START==================================
 * yap-core
 * ====================================SECTION=====================================
 * Copyright (C) 2025 aleph0
 * ====================================SECTION=====================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ==================================LICENSE_END===================================
 */
package io.aleph0.yap.core.task.action;

import java.util.concurrent.ExecutionException;

public sealed interface TaskAction permits StartWorkerTaskAction, StopWorkerTaskAction,
    SucceedTaskAction, CancelTaskAction, FailTaskAction {
  static StartWorkerTaskAction newStartWorkerTaskAction() {
    return new StartWorkerTaskAction();
  }

  static StopWorkerTaskAction newStopWorkerTaskAction() {
    return new StopWorkerTaskAction();
  }

  static SucceedTaskAction newSucceedTask() {
    return new SucceedTaskAction();
  }

  static CancelTaskAction newCancelTask() {
    return new CancelTaskAction();
  }

  static FailTaskAction newFailTask(ExecutionException e) {
    return new FailTaskAction(e);
  }
}
