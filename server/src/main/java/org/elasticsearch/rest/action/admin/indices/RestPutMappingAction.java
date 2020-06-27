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

package org.elasticsearch.rest.action.admin.indices;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.client.Requests.putMappingRequest;
import static org.elasticsearch.index.mapper.MapperService.isMappingSourceTyped;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * 构建索引映射Action
 * 处理逻辑见 {@link TransportPutMappingAction}
 */
public class RestPutMappingAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
        LogManager.getLogger(RestPutMappingAction.class));
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in put " +
        "mapping requests is deprecated. The parameter will be removed in the next major version.";

    public RestPutMappingAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(PUT, "/{index}/_mapping/", this);
        controller.registerHandler(PUT, "/{index}/{type}/_mapping", this);
        controller.registerHandler(PUT, "/{index}/_mapping/{type}", this);
        controller.registerHandler(PUT, "/_mapping/{type}", this);

        controller.registerHandler(POST, "/{index}/_mapping/", this);
        controller.registerHandler(POST, "/{index}/{type}/_mapping", this);
        controller.registerHandler(POST, "/{index}/_mapping/{type}", this);
        controller.registerHandler(POST, "/_mapping/{type}", this);

        //register the same paths, but with plural form _mappings
        controller.registerHandler(PUT, "/{index}/_mappings/", this);
        controller.registerHandler(PUT, "/{index}/{type}/_mappings", this);
        controller.registerHandler(PUT, "/{index}/_mappings/{type}", this);
        controller.registerHandler(PUT, "/_mappings/{type}", this);

        controller.registerHandler(POST, "/{index}/_mappings/", this);
        controller.registerHandler(POST, "/{index}/{type}/_mappings", this);
        controller.registerHandler(POST, "/{index}/_mappings/{type}", this);
        controller.registerHandler(POST, "/_mappings/{type}", this);
    }

    @Override
    public String getName() {
        return "put_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER,
            DEFAULT_INCLUDE_TYPE_NAME_POLICY);
        if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
            deprecationLogger.deprecatedAndMaybeLog("put_mapping_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        // 索引名称
        PutMappingRequest putMappingRequest = putMappingRequest(Strings.splitStringByCommaToArray(request.param("index")));
        // type
        final String type = request.param("type");
        putMappingRequest.type(includeTypeName ? type : MapperService.SINGLE_MAPPING_NAME);
        // 内容
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false,
            request.getXContentType()).v2();
        if (includeTypeName == false &&
                (type != null || isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, sourceAsMap))) {
            throw new IllegalArgumentException("Types cannot be provided in put mapping requests, unless " +
                "the include_type_name parameter is set to true.");
        }

        putMappingRequest.source(sourceAsMap);
        // 超时设置
        putMappingRequest.timeout(request.paramAsTime("timeout", putMappingRequest.timeout()));
        // master节点超时设置
        putMappingRequest.masterNodeTimeout(request.paramAsTime("master_timeout", putMappingRequest.masterNodeTimeout()));
        putMappingRequest.indicesOptions(IndicesOptions.fromRequest(request, putMappingRequest.indicesOptions()));
        return channel -> client.admin().indices().putMapping(putMappingRequest, new RestToXContentListener<>(channel));
    }
}
