------------------------------------------------------
-- Export file for user UPAMGR@PUMES2_NEW           --
-- Created by LSY on 2017-02-22,  14:23:49 14:23:49 --
------------------------------------------------------

set define off
spool install_oredis.log

prompt
prompt Creating type NUMBERTABLE
prompt =========================
prompt
@@numbertable.tps
prompt
prompt Creating type OREDIS_RESP_ITEM
prompt ==============================
prompt
@@oredis_resp_item.tps
prompt
prompt Creating type OREDIS_RESP_ITEM_TABLE
prompt ====================================
prompt
@@oredis_resp_item_table.tps
prompt
prompt Creating type OREDIS_RESP
prompt =========================
prompt
@@oredis_resp.tps
prompt
prompt Creating type OREDIS_RESP_TABLE
prompt ===============================
prompt
@@oredis_resp_table.tps
prompt
prompt Creating type OREDIS_TCP_CONNECTION
prompt ===================================
prompt
@@oredis_tcp_connection.tps
prompt
prompt Creating type VARCHAR2TABLE
prompt ===========================
prompt
@@varchar2table.tps
prompt
prompt Creating package PKG_OREDIS
prompt ===========================
prompt
@@pkg_oredis.pck
prompt
prompt Creating type OREDIS
prompt ====================
prompt
@@oredis.typ
prompt
prompt Creating type OREDIS_NODE
prompt =========================
prompt
@@oredis_node.typ
prompt
prompt Creating type OREDIS_NODE_TABLE
prompt ===============================
prompt
@@oredis_node_table.tps
prompt
prompt Creating type OREDIS_MASTER_NODE
prompt ================================
prompt
@@oredis_master_node.typ
prompt
prompt Creating type OREDIS_MASTER_NODE_TABLE
prompt ======================================
prompt
@@oredis_master_node_table.tps
prompt
prompt Creating type OREDIS_CLUSTER_MAP
prompt ================================
prompt
@@oredis_cluster_map.typ
prompt
prompt Creating type OREDIS_CLUSTER
prompt ============================
prompt
@@oredis_cluster.typ
prompt
prompt Creating package PKG_OREDIS_TEST
prompt ================================
prompt
@@pkg_oredis_test.pck

spool off
