# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.22 as builder

# Copy local code to the container image.
COPY . /postprocessor/
WORKDIR /postprocessor

# go build with no path looks in current directory (/postprocessor) and finds package main
RUN CGO_ENABLED=0 GOOS=linux go build -v -o post_processor

# Install tools used in build
RUN (export GOTOOLCHAIN='auto' && \
    go install honnef.co/go/tools/cmd/staticcheck@latest && \
    go install github.com/jstemmer/go-junit-report@latest && \
    go install golang.org/x/lint/golint@latest && \
    go install golang.org/x/tools/cmd/goimports@latest)

CMD [ "/postprocessor/post_processor"]
