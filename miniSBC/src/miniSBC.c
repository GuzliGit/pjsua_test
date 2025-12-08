#include "miniSBC.h"
#include "registartor.h"
#include <signal.h>
#include <pjsip.h>
#include <pjsip_ua.h>
#include <pjmedia.h>

static pjsip_endpoint* sbc_endpt;
static pjmedia_endpt* med_endpt;
static pjmedia_event_mgr* event_mgr;

static pj_caching_pool ch_pool;
static pj_pool_t* main_pool;
static pj_pool_t* stream_pool;

static pjsip_transport* in_transport;
static pjmedia_transport* in_sbc_side_tp;
static pjmedia_transport_info in_sbc_side_tpinfo;
static pjmedia_sock_info in_sbc_side_sockinfo;

static pjsip_transport* out_transport;
static pjmedia_transport* out_sbc_side_tp;
static pjmedia_transport_info out_sbc_side_tpinfo;
static pjmedia_sock_info out_sbc_side_sockinfo;

static pjsip_inv_session* uas_inv = NULL;
static pjmedia_sdp_session* local_sdp_uas;
static pjmedia_sdp_session* remote_sdp_uas;

static pjsip_inv_session* uac_inv = NULL;
static pjmedia_sdp_session* local_sdp_uac;
static pjmedia_sdp_session* remote_sdp_uac;

static pj_sockaddr* in_addr;
static pj_sockaddr* out_addr;

static pjsip_module mod_minisbc;
static volatile char is_end = 0;


static pj_status_t create_endpoint();
static pj_status_t create_pools();
static pj_status_t create_med_endpoit();
static pj_status_t create_transport();
static pj_status_t init_modules();
static void create_sbc_module();
static pj_status_t create_registrator();

static pj_status_t start_uas_dlg(pjsip_rx_data*);
static pj_status_t start_uac_dlg(pjsip_rx_data*, pjsip_tx_data*);
static void set_attach_param(pjmedia_transport_attach_param*, pjmedia_sdp_session*, pjmedia_transport*);
static pj_status_t initialize_media_bridge();

static void call_on_state_changed(pjsip_inv_session*, pjsip_event*);
static pj_bool_t on_rx_request(pjsip_rx_data*);
static pj_bool_t on_rx_response(pjsip_rx_data*);
static void rtp_callback(void*, void*, pj_ssize_t);
static void rtcp_callback(void*, void*, pj_ssize_t);

void cleanup()
{
    if (uas_inv)
    {
        pjsip_tx_data* tdata;
        pjsip_inv_end_session(uas_inv, 500, NULL, &tdata);
        pjsip_inv_send_msg(uas_inv, tdata);
    }
    if (uac_inv)
    {
        pjsip_tx_data* tdata;
        pjsip_inv_end_session(uac_inv, 500, NULL, &tdata);
        pjsip_inv_send_msg(uac_inv, tdata);
    }
    if (in_transport)
    {
        pjsip_transport_shutdown(in_transport);
    }
    if (in_sbc_side_tp)
    {
        pjmedia_transport_close(in_sbc_side_tp);
    }
    if (out_transport)
    {
        pjsip_transport_shutdown(out_transport);
    }
    if (out_sbc_side_tp)
    {
        pjmedia_transport_close(out_sbc_side_tp);
    }

    registrator_destroy();
    
    if (event_mgr)
    {
        pjmedia_event_mgr_destroy(event_mgr);
    }
    if (med_endpt)
    {
        pjmedia_endpt_destroy(med_endpt);
    }
    if (sbc_endpt)
    {
        pjsip_endpt_destroy(sbc_endpt);
    }
    if (main_pool)
    {
        pj_pool_release(main_pool);
    }
    if (stream_pool)
    {
        pj_pool_release(stream_pool);
    }
    if (&ch_pool)
    {
        pj_caching_pool_destroy(&ch_pool);
    }
    pj_shutdown();
}

void sig_handler()
{
    is_end = 1;
}

int start_sbc(pj_sockaddr* inner_addr, pj_sockaddr* outer_addr)
{
    pj_status_t status = -1;
    in_addr = inner_addr;
    out_addr = outer_addr;

    status = pj_init();
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "pj_init error"));
        goto _exit;
    }
    pj_log_set_level(5);

    status = create_endpoint();
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "create_endpoint error"));
        goto _exit;
    }
    status = create_pools();
    if (status != PJ_SUCCESS)
    {
        goto _exit;
    }
    status = create_med_endpoit();
    if (status != PJ_SUCCESS)
    {
        goto _exit;
    }
    status = create_transport(in_addr, out_addr);
    if (status != PJ_SUCCESS)
    {
        goto _exit;
    }
    status = init_modules();
    if (status != PJ_SUCCESS)
    {
        goto _exit;
    }
    status = create_registrator();
    if (status != PJ_SUCCESS)
    {
        goto _exit;
    }

    signal(SIGTERM, sig_handler);
    signal(SIGINT, sig_handler);
    
    pj_time_val delay = {0, 100};
    while (!is_end)
    {
        pjsip_endpt_handle_events(sbc_endpt, &delay);
    }

_exit:
    cleanup();
    return status;
}

static pj_status_t create_endpoint()
{
    pj_status_t status = -1;
    pj_caching_pool_init(&ch_pool, &pj_pool_factory_default_policy, 0);
    if (!(&ch_pool))
    {
        return status;
    }
    
    status = pjsip_endpt_create(&ch_pool.factory, ENDPOINT_NAME, &sbc_endpt);
    return status;
}

static pj_status_t create_pools()
{
    main_pool = pj_pool_create(&ch_pool.factory, "main_pool", MAIN_POOL_SIZE, MAIN_POOL_SIZE, NULL);
    if (!main_pool)
    {
        return -1;
    }

    stream_pool = pj_pool_create(&ch_pool.factory, "stream_pool", STREAM_POOL_SIZE, STREAM_POOL_SIZE, NULL);
    if (!stream_pool)
    {
        return -1;
    }

    return PJ_SUCCESS;
}

static pj_status_t create_med_endpoit()
{
    pj_status_t status;
    pj_ioqueue_t* ioqueue = pjsip_endpt_get_ioqueue(sbc_endpt);
    if (ioqueue)
    {
        status = pjmedia_endpt_create(&ch_pool.factory, ioqueue, 0, &med_endpt);
    }
    else
    {
        status = pjmedia_endpt_create(&ch_pool.factory, NULL, 1, &med_endpt);
    }

    if (status != PJ_SUCCESS)
    {
        return status;
    }

    status = pjmedia_event_mgr_create(main_pool, 0, &event_mgr);
    if (status != PJ_SUCCESS)
    {
        return status;
    }    
    pjmedia_event_mgr_set_instance(event_mgr);

    status = pjmedia_codec_g711_init(med_endpt);
    return status;
}

static pj_status_t create_transport()
{
    pj_status_t status;
    status = pjsip_udp_transport_start(sbc_endpt, &in_addr->ipv4, NULL, 1, &in_transport);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "create inner transport error"));
        return status;
    }

    status = pjsip_udp_transport_start(sbc_endpt, &out_addr->ipv4, NULL, 1, &out_transport);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "create outer transport error"));
        return status;
    }

    pj_str_t inner_addr = pj_str(pj_inet_ntoa(in_addr->ipv4.sin_addr));
    status = pjmedia_transport_udp_create2(med_endpt, "Inner media transport", &inner_addr, RTP_PORT_IN, 0, &in_sbc_side_tp);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "create inner media transport error"));
        return status;
    }

    pjmedia_transport_info_init(&in_sbc_side_tpinfo);
    status = pjmedia_transport_get_info(in_sbc_side_tp, &in_sbc_side_tpinfo);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "can't get info about inner media transport"));
        return status;
    }
    pj_memcpy(&in_sbc_side_sockinfo, &in_sbc_side_tpinfo.sock_info, sizeof(pjmedia_sock_info));

    pj_str_t outer_addr = pj_str(pj_inet_ntoa(out_addr->ipv4.sin_addr));
    status = pjmedia_transport_udp_create2(med_endpt, "Outer media transport", &outer_addr, RTP_PORT_OUT, 0, &out_sbc_side_tp);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "create outer media transport error"));
        return status;
    }

    pjmedia_transport_info_init(&out_sbc_side_tpinfo);
    status = pjmedia_transport_get_info(out_sbc_side_tp, &out_sbc_side_tpinfo);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "can't get info about outer media transport"));
    }
    pj_memcpy(&out_sbc_side_sockinfo, &out_sbc_side_tpinfo.sock_info, sizeof(pjmedia_sock_info));

    return status;
}

static pj_status_t init_modules()
{
    pj_status_t status;
    status = pjsip_tsx_layer_init_module(sbc_endpt);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "pjsip_tsx_layer_init_module error"));
        return status;
    }

    status = pjsip_ua_init_module(sbc_endpt, NULL);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "pjsip_ua_init_module error"));
        return status;
    }

    pjsip_inv_callback inv_cb;
    pj_bzero(&inv_cb, sizeof(inv_cb));
    inv_cb.on_state_changed = &call_on_state_changed;

    status = pjsip_inv_usage_init(sbc_endpt, &inv_cb);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "pjsip_inv_usage_init error"));
        return status;
    }

    status = pjsip_100rel_init_module(sbc_endpt);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "pjsip_100rel_init_module error"));
        return status;
    }

    create_sbc_module();
    status = pjsip_endpt_register_module(sbc_endpt, &mod_minisbc);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "pjsip_endpt_register_module [miniSBC] error"));
    }

    return status;
}

static void create_sbc_module()
{
    mod_minisbc.prev = NULL;
    mod_minisbc.next = NULL;
    mod_minisbc.name = pj_str("mod-miniSBC");
    mod_minisbc.id = -1;
    mod_minisbc.priority = PJSIP_MOD_PRIORITY_TRANSPORT_LAYER + 1;
    mod_minisbc.start = NULL;
    mod_minisbc.load = NULL;
    mod_minisbc.stop = NULL;
    mod_minisbc.unload = NULL;
    mod_minisbc.on_rx_request = &on_rx_request;
    mod_minisbc.on_rx_response = &on_rx_response;
    mod_minisbc.on_tsx_state = NULL;
    mod_minisbc.on_tx_request = NULL;
    mod_minisbc.on_tx_response = NULL;
}

static pj_status_t create_registrator()
{
    pj_status_t status;
    status = registrator_init(&ch_pool.factory);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "Registrator init error"));
        return status;
    }
    
    pj_str_t name = pj_str("tester");
    pj_str_t addr_str = pj_str("127.0.0.1");
    pj_in_addr addr;
    pj_inet_aton(&addr_str, &addr);
    pj_uint16_t port = 5063;
    pj_str_t password = pj_str(DEFAULT_PASS);

    status = add_contact(&name, &addr, port, &password);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "Can't add equal contacts to registrator"));
    }

    name = pj_str("user1");
    addr_str = pj_str("127.0.0.10");
    pj_inet_aton(&addr_str, &addr);
    port = 5080;
    password = pj_str(DEFAULT_PASS);

    status = add_contact(&name, &addr, port, &password);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "Can't add equal contacts to registrator"));
    }

    return PJ_SUCCESS;
}

static pj_status_t start_uas_dlg(pjsip_rx_data* rdata)
{
    pjsip_dialog* uas_dlg;
    pj_status_t status = pjsip_dlg_create_uas_and_inc_lock(pjsip_ua_instance(), rdata, NULL, &uas_dlg);
    if (status != PJ_SUCCESS)
    {
        pj_str_t st_text = pj_str("Can't create UAS dialog");
        pjsip_endpt_respond_stateless(sbc_endpt, rdata, 500, &st_text, NULL, NULL);
        return status;

    }
 
    pjmedia_sock_info* cur_sock_info = (rdata->tp_info.transport == in_transport) ? &in_sbc_side_sockinfo : &out_sbc_side_sockinfo;
    status = pjmedia_endpt_create_sdp(med_endpt, stream_pool, 1, cur_sock_info, &local_sdp_uas);
    if (status != PJ_SUCCESS)
    {
        pjsip_dlg_dec_lock(uas_dlg);
        return status;
    }

    status = pjsip_inv_create_uas(uas_dlg, rdata, local_sdp_uas, 0, &uas_inv);
    pjsip_dlg_dec_lock(uas_dlg);

    return status;
}

static pj_status_t start_uac_dlg(pjsip_rx_data* rdata, pjsip_tx_data* tdata)
{
    pjsip_dialog* uac_dlg;
    char local_uri[128], remote_uri[128];
    pj_in_addr dest_addr;
    pj_uint16_t dest_port;
    pjsip_sip_uri* req_uri = (pjsip_sip_uri*) rdata->msg_info.msg->line.req.uri;
    if (!req_uri)
    {
        pj_str_t st_text = pj_str("Can't find username");
        pjsip_inv_end_session(uas_inv, 500, &st_text, &tdata);
        pjsip_inv_send_msg(uas_inv, tdata);
        return -1;
    }
    else if (get_info_by_name(req_uri->user, &dest_addr, &dest_port) != PJ_SUCCESS)
    {
        pj_str_t st_text = pj_str("Can't find info about contact with such name");
        pjsip_inv_end_session(uas_inv, 404, &st_text, &tdata);
        pjsip_inv_send_msg(uas_inv, tdata);
        return -1;
    }

    pjsip_transport* cur_transport;
    pjmedia_sock_info* cur_sock_info;
    pj_sockaddr* cur_sockaddr;
    if (rdata->tp_info.transport == in_transport)
    {
        cur_transport = out_transport;
        cur_sock_info = &out_sbc_side_sockinfo;
        cur_sockaddr = out_addr;
    }
    else
    {
        cur_transport = in_transport;
        cur_sock_info = &in_sbc_side_sockinfo;
        cur_sockaddr = in_addr;
    }

    pj_status_t status = pjmedia_endpt_create_sdp(med_endpt, stream_pool, 1, cur_sock_info, &local_sdp_uac);
    if (status != PJ_SUCCESS)
    {
        return -2;
    }

    snprintf(local_uri, 128, "sip:sbc@%s", pj_inet_ntoa(cur_sockaddr->ipv4.sin_addr));
    pj_ansi_snprintf(remote_uri, 128, "sip:%.*s@%s:%d", (int) req_uri->user.slen, req_uri->user.ptr, pj_inet_ntoa(dest_addr), dest_port);
    pj_str_t loc = pj_str(local_uri);
    pj_str_t rem = pj_str(remote_uri);

    status = pjsip_dlg_create_uac(pjsip_ua_instance(), &loc, NULL, &rem, NULL, &uac_dlg);
    if (status != PJ_SUCCESS)
    {
        return -2;
    }

    status = pjsip_inv_create_uac(uac_dlg, local_sdp_uac, 0, &uac_inv);
    if (status != PJ_SUCCESS)
    {
        return -2;
    }

    pjsip_tpselector sel;
    sel.type = PJSIP_TPSELECTOR_TRANSPORT;
    sel.u.transport = cur_transport;
    status = pjsip_dlg_set_transport(uac_dlg, &sel);
    if (status != PJ_SUCCESS)
    {
        return -3;
    }

    status = pjsip_inv_invite(uac_inv, &tdata);
    if (status != PJ_SUCCESS)
    {
        return -3;
    }
    
    status = pjsip_inv_send_msg(uac_inv, tdata);
    if (status != PJ_SUCCESS)
    {
        return -3;
    }

    return PJ_SUCCESS;
}

static void set_attach_param(pjmedia_transport_attach_param* param, pjmedia_sdp_session* sdp_sess, pjmedia_transport* tp)
{
    pj_bzero(param, sizeof(pjmedia_transport_attach_param));
    pj_inet_aton(&sdp_sess->origin.addr, &param->rem_addr.ipv4.sin_addr);
    param->rem_addr.ipv4.sin_port = pj_htons(sdp_sess->media[0]->desc.port);
    param->addr_len = sizeof(param->rem_addr.ipv4);
    param->rem_addr.addr.sa_family = PJ_AF_INET;
    param->user_data = tp;
    param->rtp_cb = rtp_callback;
    param->rtcp_cb = rtcp_callback;
}

static pj_status_t initialize_media_bridge()
{
    char loc_addr[32];
    pj_memcpy(loc_addr, local_sdp_uas->origin.addr.ptr, local_sdp_uas->origin.addr.slen);
    loc_addr[local_sdp_uas->origin.addr.slen] = '\0';
    char* in_sbc_addr = pj_inet_ntoa(in_sbc_side_sockinfo.rtp_addr_name.ipv4.sin_addr); 

    pjmedia_sdp_session* cur_rem_in_sess, *cur_loc_in_sess;
    pjmedia_sdp_session* cur_rem_out_sess, *cur_loc_out_sess;
    if (strncmp(in_sbc_addr, loc_addr, 32) == 0)
    {
        cur_rem_in_sess = remote_sdp_uas;
        cur_loc_in_sess = local_sdp_uas;
        cur_rem_out_sess = remote_sdp_uac;
        cur_loc_out_sess = local_sdp_uac;
    }
    else
    {
        cur_rem_in_sess = remote_sdp_uac;
        cur_loc_in_sess = local_sdp_uac;
        cur_rem_out_sess = remote_sdp_uas;
        cur_loc_out_sess = local_sdp_uas;
    }

    pjmedia_transport_attach_param in_param;
    set_attach_param(&in_param, cur_rem_in_sess, out_sbc_side_tp);

    pj_status_t status = pjmedia_transport_attach2(in_sbc_side_tp, &in_param);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "== pjmedia_transport_attach2 error =="));
        return status;
    }

    pjmedia_transport_attach_param out_param;
    set_attach_param(&out_param, cur_rem_out_sess, in_sbc_side_tp);

    status = pjmedia_transport_attach2(out_sbc_side_tp, &out_param);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "== pjmedia_transport_attach2 error =="));
        return status;
    }

    status = pjmedia_transport_media_start(in_sbc_side_tp, stream_pool, cur_loc_in_sess, cur_rem_in_sess, 0);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "== pjmedia_transport_media_start error =="));
        return status;
    }
    status = pjmedia_transport_media_start(out_sbc_side_tp, stream_pool, cur_loc_out_sess, cur_rem_out_sess, 0);
    if (status != PJ_SUCCESS)
    {
        PJ_LOG(2, (__FILE__, "== pjmedia_transport_media_start error =="));
        return status;
    }

    return PJ_SUCCESS;
}
//
///
////Callbacks
static void call_on_state_changed(pjsip_inv_session* inv, pjsip_event* e)
{
    PJ_UNUSED_ARG(e);
    if (inv->state == PJSIP_INV_STATE_DISCONNECTED)
    {
        pjsip_tx_data* tdata;
        PJ_LOG(2, (__FILE__, "Call disconnected | reason=%d [%.*s]", inv->cause, (int)inv->cause_text.slen, inv->cause_text.ptr));
        if (uas_inv && uac_inv)
        {
            if (uac_inv->state == PJSIP_INV_STATE_DISCONNECTED && uas_inv->state == PJSIP_INV_STATE_DISCONNECTED)
            {
                pj_pool_reset(stream_pool);
            }
            else if (uac_inv->state != PJSIP_INV_STATE_DISCONNECTED && uas_inv->state == PJSIP_INV_STATE_DISCONNECTED)
            {
                pjsip_inv_terminate(uac_inv, 500, 0);
                pj_pool_reset(stream_pool);
            }
            else if (uac_inv->state == PJSIP_INV_STATE_DISCONNECTED && uas_inv->state != PJSIP_INV_STATE_DISCONNECTED)
            {
                pjsip_inv_terminate(uas_inv, 500, 0);
                pj_pool_reset(stream_pool);
            }

            uas_inv = NULL;
            uac_inv = NULL;
        }
    }
    else if (uas_inv && uac_inv && uas_inv->state == PJSIP_INV_STATE_CONFIRMED && uac_inv->state == PJSIP_INV_STATE_CONFIRMED)
    {
        PJ_LOG(2, (__FILE__, "===== Connection established | media bridge initializing ====="));
        if (initialize_media_bridge() != PJ_SUCCESS)
        {
            PJ_LOG(2, (__FILE__, "==== Media bridge initialization error ===="));
        }
        else
        {
            PJ_LOG(2, (__FILE__, "==== Media bridge initialization complete ===="));
        }
    }
    else
    {
        PJ_LOG(2, (__FILE__, "Call state changed to %d", inv->state));
    }
}

static pj_bool_t on_rx_request(pjsip_rx_data* rdata)
{
    PJ_LOG(2,(__FILE__, "== REQUEST %d bytes %s from %s %s:%d ==",
                         rdata->msg_info.len,
                         pjsip_rx_data_get_info(rdata),
                         rdata->tp_info.transport->type_name,
                         rdata->pkt_info.src_name,
                         rdata->pkt_info.src_port));

    pjsip_method_e method = rdata->msg_info.msg->line.req.method.id;
    if (method == PJSIP_REGISTER_METHOD)
    {
        pjsip_authorization_hdr* hdr = pjsip_msg_find_hdr(rdata->msg_info.msg, PJSIP_H_AUTHORIZATION, NULL);
        if (!hdr)
        {
            try_register(sbc_endpt, rdata);
            return PJ_FALSE;
        }
        else
        {
            try_auth(sbc_endpt, rdata);
            return PJ_FALSE;
        }
    }
    else if (method == PJSIP_BYE_METHOD && (uas_inv || uac_inv))
    {
        goto clean_uac_and_uas;
    }
    else if (method != PJSIP_INVITE_METHOD)
    {
        return PJ_FALSE;
    }

    if (uas_inv)
    {
        pj_str_t st_text = pj_str("Another call is in progress");
        pjsip_endpt_respond_stateless(sbc_endpt, rdata, 486, &st_text, NULL, NULL);
        return PJ_FALSE;
    }
    PJ_LOG(2, (__FILE__, "[TRANSPORT] request from %s:%d is being processed", pj_inet_ntoa(rdata->pkt_info.src_addr.ipv4.sin_addr), rdata->pkt_info.src_port));

    unsigned int options = 0;
    pj_status_t status = pjsip_inv_verify_request(rdata, &options, NULL, NULL, sbc_endpt, NULL);
    if (status != PJ_SUCCESS)
    {
        pj_str_t st_text = pj_str("Can't verify this invite");
        pjsip_endpt_respond_stateless(sbc_endpt, rdata, 500, &st_text, NULL, NULL);
        return PJ_FALSE;
    }

    if (start_uas_dlg(rdata) != PJ_SUCCESS)
    {
        return PJ_FALSE;
    }

    remote_sdp_uas = pjsip_get_sdp_info(stream_pool, rdata->msg_info.msg->body, NULL, NULL)->sdp;
    if (!remote_sdp_uas)
    {
        goto clean_uas;
    }

    pjsip_tx_data* tdata;
    status = pjsip_inv_initial_answer(uas_inv, rdata, 100, NULL, NULL, &tdata);
    if (status != PJ_SUCCESS)
    {
        goto clean_uas;
    }

    status = pjsip_inv_send_msg(uas_inv, tdata);
    if (status != PJ_SUCCESS)
    {
        goto clean_uas;
    }

    switch (start_uac_dlg(rdata, tdata))
    {
    case -1:
        return PJ_FALSE;
    
    case -2:
        goto clean_uas;
        break;

    case -3:
        goto clean_uac_and_uas;
        break;

    default:
        break;
    }

    return PJ_FALSE;

clean_uas:
    pjsip_inv_end_session(uas_inv, 500, NULL, &tdata);
    pjsip_inv_send_msg(uas_inv, tdata);
    return PJ_FALSE;

clean_uac_and_uas:
    pjsip_inv_end_session(uas_inv, 500, NULL, &tdata);
    pjsip_inv_send_msg(uas_inv, tdata);
    pjsip_inv_end_session(uac_inv, 500, NULL, &tdata);
    pjsip_inv_send_msg(uac_inv, tdata);
    return PJ_FALSE;
}

static pj_bool_t on_rx_response(pjsip_rx_data* rdata)
{
    PJ_LOG(2,(__FILE__, "== RESPONSE %d bytes %s from %s %s:%d ==\n %.*s\n == end of msg ==",
                         rdata->msg_info.len,
                         pjsip_rx_data_get_info(rdata),
                         rdata->tp_info.transport->type_name,
                         rdata->pkt_info.src_name,
                         rdata->pkt_info.src_port,
                         (int)rdata->msg_info.len,
                         rdata->msg_info.msg_buf));

    pj_status_t status;
    if (uas_inv && uac_inv)
    {
        pjsip_tx_data* tdata;
        pjsip_inv_session* answer_sess = (pjsip_rdata_get_dlg(rdata) == uas_inv->dlg) ? uac_inv : uas_inv;
        pjmedia_sdp_session* cur_sdp_sess = pjsip_get_sdp_info(stream_pool, rdata->msg_info.msg->body, NULL, NULL)->sdp;
        if (cur_sdp_sess)
        {
            if (answer_sess == uas_inv)
            {
                remote_sdp_uac = cur_sdp_sess;
                pjsip_inv_set_sdp_answer(answer_sess, local_sdp_uas);
            }
            else
            {
                remote_sdp_uas = cur_sdp_sess;
                pjsip_inv_set_sdp_answer(answer_sess, local_sdp_uac);
            }
        }
        
        status = pjsip_inv_answer(answer_sess, rdata->msg_info.msg->line.status.code, NULL, NULL, &tdata);
        if (status != PJ_SUCCESS)
        {
            return PJ_FALSE;
        }
        status = pjsip_inv_send_msg(answer_sess, tdata);
        if (status != PJ_SUCCESS)
        {
            return PJ_FALSE;
        }

        return PJ_FALSE;
    }
    
    return PJ_FALSE;
}

static void rtp_callback(void* user_data, void* pkt, pj_ssize_t size)
{
    pjmedia_transport* cur_tp = (pjmedia_transport*) user_data;
    
    pjmedia_transport_send_rtp(cur_tp, pkt, size);
    PJ_LOG(2, (__FILE__, "== RTP was forwarded to %s ==", cur_tp->name));
}

static void rtcp_callback(void* user_data, void* pkt, pj_ssize_t size)
{
    pjmedia_transport* cur_tp = (pjmedia_transport*) user_data;

    pjmedia_transport_send_rtcp(cur_tp, pkt, size);
    PJ_LOG(2, (__FILE__, "== RTCP was forwarded to %s ==", cur_tp->name));
}