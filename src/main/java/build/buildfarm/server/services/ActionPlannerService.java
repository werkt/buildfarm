// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.server.services;

import build.bazel.remote.execution.v2.ActionPlannerGrpc;
import build.bazel.remote.execution.v2.ActionPlanResponse;
import build.bazel.remote.execution.v2.InformRequest;
import build.buildfarm.instance.Instance;
import io.grpc.stub.StreamObserver;
import lombok.extern.java.Log;

@Log
public class ActionPlannerService extends ActionPlannerGrpc.ActionPlannerImplBase {
  private final Instance instance;

  public ActionPlannerService(Instance instance) {
    this.instance = instance;
  }

  @Override
  public void inform(InformRequest request, StreamObserver<ActionPlanResponse> observer) {
    throw new UnsupportedOperationException();
  }
}
