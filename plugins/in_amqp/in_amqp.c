/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2015-2024 The Fluent Bit Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <time.h>

#include <rabbitmq-c/ssl_socket.h>
#include <rabbitmq-c/tcp_socket.h>

#include <msgpack.h>
#include <fluent-bit/flb_input.h>
#include <fluent-bit/flb_input_plugin.h>
#include <fluent-bit/flb_config.h>
#include <fluent-bit/flb_config_map.h>
#include <fluent-bit/flb_error.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_log_event.h>
#include <fluent-bit/flb_utils.h>

#include "in_amqp.h"

static void log_amqp_reply_error(struct flb_input_instance *in, amqp_rpc_reply_t x, char const *context) {
    switch (x.reply_type) {
        case AMQP_RESPONSE_NORMAL: return;

        case AMQP_RESPONSE_NONE:
            flb_plg_error(in, "%s: missing RPC reply type", context);
            break;

        case AMQP_RESPONSE_LIBRARY_EXCEPTION:
            flb_plg_error(in, "%s: %s", context, amqp_error_string2(x.library_error));
            break;

        case AMQP_RESPONSE_SERVER_EXCEPTION:
            switch (x.reply.id) {
                case AMQP_CONNECTION_CLOSE_METHOD: {
                    amqp_connection_close_t *m = (amqp_connection_close_t *)x.reply.decoded;

                    flb_plg_error(in, "%s: server connection error %uh, message: %.*s\n", context, m->reply_code, (int)m->reply_text.len, (char *)m->reply_text.bytes);
                    break;

                }
                case AMQP_CHANNEL_CLOSE_METHOD: {
                    amqp_channel_close_t *m = (amqp_channel_close_t *)x.reply.decoded;

                    flb_plg_error(in, "%s: server channel error %uh, message: %.*s\n", context, m->reply_code, (int)m->reply_text.len, (char *)m->reply_text.bytes);
                    break;

                }
                default:
                    flb_plg_error(in, "%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
                    break;
            }
            break;
    }
}


/* cb_collect callback */
static int in_amqp_collect(struct flb_input_instance *ins,
                            struct flb_config *config,
                            void *in_context)
{
    struct flb_amqp* ctx = in_context;

    return 0;
}

static int config_destroy(struct flb_amqp *ctx)
{
    amqp_rpc_reply_t reply;
    int ret;

    if (ctx->encoder != NULL) {
        flb_log_event_encoder_destroy(ctx->encoder);
    }

    reply = amqp_channel_close(ctx->conn, ctx->channel, AMQP_REPLY_SUCCESS);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        log_amqp_reply_error(ctx->ins, reply, "Cannot close the channel");
    }

    reply = amqp_connection_close(ctx->conn, AMQP_REPLY_SUCCESS);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        log_amqp_reply_error(ctx->ins, reply, "Cannot close the connection");
    }

    if (ctx->conn != NULL) {
        ret = amqp_destroy_connection(ctx->conn);
        if (ret != AMQP_STATUS_OK) {
            flb_plg_warn(ctx->ins, "Failed to destroy AMQP connection: %s", amqp_error_string2(ret));
        }
    }

    flb_free(ctx);

    return 0;
}

/* Set plugin configuration */
static int configure(struct flb_amqp *ctx,
                     struct flb_input_instance *in,
                     struct timespec *tm)
{
    int ret = -1;

    ret = flb_input_config_map_set(in, (void *) ctx);
    if (ret == -1) {
        return -1;
    }

    if (ctx->uri) {
        ret = amqp_parse_url(ctx->uri, &ctx->conn_info);
        if (ret != AMQP_STATUS_OK) {
            flb_plg_error(in, "Error while parsing AMQP URI: %s", amqp_error_string2(ret));

            return -1;
        }
    }

    return 0;
}

/* Initialize plugin */
static int in_amqp_init(struct flb_input_instance *in,
                         struct flb_config *config, void *data)
{
    int ret = -1;
    amqp_rpc_reply_t reply;
    amqp_queue_declare_ok_t *queue_declare_ok;
    struct flb_amqp *ctx = NULL;
    struct timespec tm;

    /* Allocate space for the configuration */
    ctx = flb_malloc(sizeof(struct flb_amqp));
    if (ctx == NULL) {
        return -1;
    }

    /* Fill default AMQP connection info */
    amqp_default_connection_info(&ctx->conn_info);

    ctx->ins = in;
    ctx->conn = NULL;
    ctx->socket = NULL;
    ctx->channel = 0;

    /* Initialize head config */
    ret = configure(ctx, in, &tm);
    if (ret < 0) {
        config_destroy(ctx);
        return -1;
    }

    ctx->conn = amqp_new_connection();
    ctx->socket = ctx->conn_info.ssl ? amqp_ssl_socket_new(ctx->conn) : amqp_tcp_socket_new(ctx->conn);
    if (ctx->socket == NULL) {
        flb_plg_error(in, "Cannot create AMQP socket");

        config_destroy(ctx);
        return -1;
    }

    ret = amqp_socket_open(ctx->socket, ctx->conn_info.host, ctx->conn_info.port);
    if (ret != AMQP_STATUS_OK) {
        flb_plg_error(in, "Cannot open AMQP socket: %s", amqp_error_string2(ret));

        config_destroy(ctx);
        return -1;
    }

    reply = amqp_login(ctx->conn, ctx->conn_info.vhost, 0, AMQP_DEFAULT_FRAME_SIZE, 0, AMQP_SASL_METHOD_PLAIN, ctx->conn_info.user, ctx->conn_info.password);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        log_amqp_reply_error(in, reply, "Cannot login to the broker");

        config_destroy(ctx);
        return -1;
    }

    ctx->channel = 1;
    if (amqp_channel_open(ctx->conn, ctx->channel) == NULL) {
        log_amqp_reply_error(in, amqp_get_rpc_reply(ctx->conn), "Cannot open AMQP channel");

        config_destroy(ctx);
        return -1;
    }

    queue_declare_ok = amqp_queue_declare(ctx->conn, ctx->channel, amqp_empty_bytes, 0, 0, 0/*1*/, 1, amqp_empty_table);
    if (queue_declare_ok == NULL) {
        log_amqp_reply_error(in, amqp_get_rpc_reply(ctx->conn), "Cannot declare AMQP queue");

        config_destroy(ctx);
        return -1;
    }

    if (amqp_basic_consume(ctx->conn, ctx->channel, queue_declare_ok->queue, amqp_empty_bytes, 0, 1, 1, amqp_empty_table) == NULL) {
        log_amqp_reply_error(in, amqp_get_rpc_reply(ctx->conn), "Cannot consume");

        config_destroy(ctx);
        return -1;
    }

    ctx->encoder = flb_log_event_encoder_create(FLB_LOG_EVENT_FORMAT_DEFAULT);

    if (ctx->encoder == NULL) {
        flb_plg_error(in, "could not initialize event encoder");
        config_destroy(ctx);

        return -1;
    }

    flb_input_set_context(in, ctx);

    ret = flb_input_set_collector_socket(in, in_amqp_collect, amqp_socket_get_sockfd(ctx->socket), config);
    if (ret < 0) {
        flb_plg_error(ctx->ins, "could not set collector for AMQP input plugin");
        config_destroy(ctx);
        return -1;
    }

    return 0;
}

static void in_amqp_pause(void *data, struct flb_config *config)
{
  //  struct flb_amqp *ctx = data;

//    flb_input_collector_pause(ctx->coll_fd, ctx->ins);
}

static void in_amqp_resume(void *data, struct flb_config *config)
{
//    struct flb_amqp *ctx = data;

//    flb_input_collector_resume(ctx->coll_fd, ctx->ins);
}

static int in_amqp_exit(void *data, struct flb_config *config)
{
    (void) *config;
    struct flb_amqp *ctx = data;

    config_destroy(ctx);

    return 0;
}

/* Configuration properties map */
static struct flb_config_map config_map[] = {
    {
     FLB_CONFIG_MAP_STR, "uri", NULL,
     0, FLB_TRUE, offsetof(struct flb_amqp, uri),
     "Specify an AMQP URI to connect the broker"
    },
    {0},
};


struct flb_input_plugin in_amqp_plugin = {
    .name         = "amqp",
    .description  = "AMQP input plugin",
    .cb_init      = in_amqp_init, // FIXME
    .cb_pre_run   = NULL,
    .cb_collect   = in_amqp_collect, // FIXME
    .cb_flush_buf = NULL,
    .config_map   = config_map, // FIXME
    .cb_pause     = in_amqp_pause, // FIXME
    .cb_resume    = in_amqp_resume, // FIXME
    .cb_exit      = in_amqp_exit // FIXME
};
