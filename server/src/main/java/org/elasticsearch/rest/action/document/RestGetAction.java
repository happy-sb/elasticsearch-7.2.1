/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.document;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *  处理逻辑见
 * @see  TransportGetAction
 */
public class RestGetAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
        LogManager.getLogger(RestGetAction.class));
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in " +
        "document get requests is deprecated, use the /{index}/_doc/{id} endpoint instead.";

    public RestGetAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/{index}/_doc/{id}", this);
        controller.registerHandler(HEAD, "/{index}/_doc/{id}", this);

        // Deprecated typed endpoints.
        controller.registerHandler(GET, "/{index}/{type}/{id}", this);
        controller.registerHandler(HEAD, "/{index}/{type}/{id}", this);
    }

    @Override
    public String getName() {
        return "document_get_action";
    }

    /**
     * 处理查询请求
     *
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     */
    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        GetRequest getRequest;
        if (request.hasParam("type")) {
            deprecationLogger.deprecatedAndMaybeLog("get_with_types", TYPES_DEPRECATION_MESSAGE);
            getRequest = new GetRequest(request.param("index"), request.param("type"), request.param("id"));
        } else {
            getRequest = new GetRequest(request.param("index"), request.param("id"));
        }

        // 在读取前是否要刷新, 默认false
        getRequest.refresh(request.paramAsBoolean("refresh", getRequest.refresh()));
        // 是否制定了路由, 请求发送到集群的哪个shard, 每个shard包含一个Primary和若干个Replica
        getRequest.routing(request.param("routing"));
        // 优先选择哪个节点, "_local":当前节点;  "_primary":主节点。 请求指向shard里的Primary还是哪个Replica
        getRequest.preference(request.param("preference"));
        // 是否实时, 默认true
        getRequest.realtime(request.paramAsBoolean("realtime", getRequest.realtime()));
        if (request.param("fields") != null) {
            throw new IllegalArgumentException("the parameter [fields] is no longer supported, " +
                "please use [stored_fields] to retrieve stored fields or [_source] to load the field from _source");
        }
        // 指定查询那些Field
        final String fieldsParam = request.param("stored_fields");
        if (fieldsParam != null) {
            final String[] fields = Strings.splitStringByCommaToArray(fieldsParam);
            if (fields != null) {
                getRequest.storedFields(fields);
            }
        }

        // 数据版本
        getRequest.version(RestActions.parseVersion(request));
        getRequest.versionType(VersionType.fromString(request.param("version_type"), getRequest.versionType()));

        // 构建要查询的Field的上下文信息,就是制定查询那些Field,排除那些Field
        getRequest.fetchSourceContext(FetchSourceContext.parseFromRestRequest(request));

        return channel -> client.get(getRequest, new RestToXContentListener<GetResponse>(channel) {
            @Override
            protected RestStatus getStatus(final GetResponse response) {
                return response.isExists() ? OK : NOT_FOUND;
            }
        });
    }

}
