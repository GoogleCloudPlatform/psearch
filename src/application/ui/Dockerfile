# 
# Copyright 2025 Google LLC
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Use Node 20 LTS Alpine
FROM node:22-alpine as build

WORKDIR /app

# Copy the source code
COPY . .

# Aggressively clean before install
RUN npm cache clean --force && rm -rf node_modules

# Install specific AJV versions known to be compatible with react-scripts 5.x ecosystem
RUN npm install ajv@^6.12.2 ajv-keywords@^3.5.2 --legacy-peer-deps

# Install remaining dependencies
RUN npm install --legacy-peer-deps

# Set environment variables
ENV DISABLE_ESLINT_PLUGIN=true
ENV SKIP_PREFLIGHT_CHECK=true
ENV NODE_ENV=production
ENV PATH /app/node_modules/.bin:$PATH

# Add build arguments
ARG REACT_APP_INGESTION_SOURCE_API_URL
ENV REACT_APP_INGESTION_SOURCE_API_URL=$REACT_APP_INGESTION_SOURCE_API_URL

ARG REACT_APP_API_URL
ENV REACT_APP_API_URL=$REACT_APP_API_URL

ARG REACT_APP_GEN_AI_URL
ENV REACT_APP_GEN_AI_URL=$REACT_APP_GEN_AI_URL

# Build the application
RUN npm run build

# --- Production Stage ---
FROM nginx:alpine
COPY --from=build /app/build /usr/share/nginx/html
RUN echo 'server { \
  listen 8080; \
  location / { \
    root /usr/share/nginx/html; \
    index index.html index.htm; \
    try_files $uri $uri/ /index.html; \
  } \
}' > /etc/nginx/conf.d/default.conf
EXPOSE 8080
CMD ["nginx", "-g", "daemon off;"]
