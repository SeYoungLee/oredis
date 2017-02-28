set define off
spool install_oredis_10g.log

prompt
prompt Creating type NUMBERTABLE
prompt =========================
prompt
CREATE OR REPLACE TYPE NumberTable IS TABLE OF NUMBER
/

prompt
prompt Creating type OREDIS_RESP_ITEM
prompt ==============================
prompt
CREATE OR REPLACE TYPE OREDIS_RESP_ITEM IS OBJECT (
    type NUMBER,      -- PKG_OREDIS.STRING, PKG_OREDIS.INTEGER...  STRING : 1, ARRAY : 2, INTEGER : 3, NIL : 4,  STATUS : 5, ERROR : 6, RAW : 7
    str VARCHAR2(32767),
    int NUMBER
  )
/

prompt
prompt Creating type OREDIS_RESP_ITEM_TABLE
prompt ====================================
prompt
CREATE OR REPLACE TYPE OREDIS_RESP_ITEM_TABLE AS TABLE OF OREDIS_RESP_ITEM
/

prompt
prompt Creating type OREDIS_RESP
prompt =========================
prompt
CREATE OR REPLACE TYPE OREDIS_RESP IS OBJECT (
    type NUMBER,    -- PKG_OREDIS.STRING, PKG_OREDIS.INTEGER... STRING : 1, ARRAY : 2, INTEGER : 3, NIL : 4,  STATUS : 5, ERROR : 6, RAW : 7
    item_cnt NUMBER,
    item OREDIS_RESP_ITEM_TABLE,
    str VARCHAR2(32767),
    int NUMBER
  )
/

prompt
prompt Creating type OREDIS_RESP_TABLE
prompt ===============================
prompt
CREATE OR REPLACE TYPE OREDIS_RESP_TABLE AS TABLE OF OREDIS_RESP
/

prompt
prompt Creating type OREDIS_TCP_CONNECTION
prompt ===================================
prompt
CREATE OR REPLACE TYPE OREDIS_TCP_CONNECTION AS OBJECT
(
    remote_host   VARCHAR2(255),   -- Remote host name
    remote_port   NUMBER,     -- Remote port number
    local_host    VARCHAR2(255),   -- Local host name
    local_port    NUMBER,     -- Local port number
    charset       VARCHAR2(30),    -- Character set for on-the-wire comm.
    newline       VARCHAR2(2),     -- Newline character sequence
    tx_timeout    NUMBER,     -- Transfer time-out value (in seconds)
    private_sd    NUMBER      -- For internal use only

)
/

prompt
prompt Creating type VARCHAR2TABLE
prompt ===========================
prompt
CREATE OR REPLACE TYPE Varchar2Table AS TABLE OF  VARCHAR2(32767)
/

prompt
prompt Creating package PKG_OREDIS
prompt ===========================
prompt
CREATE OR REPLACE PACKAGE PKG_OREDIS IS

  -- Author  : LSY
  -- Created : 2017-01-03  13:48:56 13:48:56
  -- Purpose : 
    
  
  REDIS_CLUSTER_SLOT_CNT CONSTANT NUMBER := 16384;
  
  REPLY_STRING CONSTANT NUMBER := 1; 
  REPLY_ARRAY CONSTANT NUMBER := 2;     
  REPLY_INTEGER CONSTANT NUMBER := 3;
  REPLY_NIL CONSTANT NUMBER := 4;     
  REPLY_STATUS CONSTANT NUMBER := 5;
  REPLY_ERROR CONSTANT NUMBER := 6;
  REPLY_RAW CONSTANT NUMBER := 7;
    
  
  --E_TIMEOUT EXCEPTION;  
  --E_RESPONSE EXCEPTION;
  
  E_ASSERT EXCEPTION;
  PRAGMA EXCEPTION_INIT(E_ASSERT, -20201);  --Can't assign a variable as error number -_-;
  
  E_NOT_SUPPORTED_COMMAND EXCEPTION;
  PRAGMA EXCEPTION_INIT(E_NOT_SUPPORTED_COMMAND, -20202);
  
  E_AUTH_FAIL EXCEPTION;
  PRAGMA EXCEPTION_INIT(E_AUTH_FAIL, -20203);  
  
  E_INVALID_CONFIG EXCEPTION;
  PRAGMA EXCEPTION_INIT(E_INVALID_CONFIG, -20204);
  
  E_CONNECTION EXCEPTION;
  PRAGMA EXCEPTION_INIT(E_CONNECTION, -20205);
  
  E_IN_ASYNC_MODE EXCEPTION;
  PRAGMA EXCEPTION_INIT(E_IN_ASYNC_MODE, -20206);
  
  E_NODE_NOT_FOUND EXCEPTION;
  PRAGMA EXCEPTION_INIT(E_NODE_NOT_FOUND, -20207);
  
  E_PROTOCOL EXCEPTION;
  PRAGMA EXCEPTION_INIT(E_PROTOCOL, -20208);
  
  E_WRONG_ASYNC_RESP_COUNT EXCEPTION;
  PRAGMA EXCEPTION_INIT(E_PROTOCOL, -20209);
  
  E_GENERAL_EXCEPTION EXCEPTION;
  PRAGMA EXCEPTION_INIT(E_GENERAL_EXCEPTION, -20210);
    
        
  NEWLINE CONSTANT VARCHAR(2):= UTL_TCP.CRLF;
  OK CONSTANT VARCHAR2(10) := 'OK';  
  NIL CONSTANT VARCHAR2(10) := '(nil)';  
  
  MASTER_NODE_TYPE CONSTANT VARCHAR2(10) := 'M'; 
  SLAVE_NODE_TYPE  CONSTANT VARCHAR2(10) := 'S'; 
    
  FUNCTION OPEN_TCP(p_host VARCHAR2, 
                    p_port PLS_INTEGER DEFAULT 6379, 
                    p_tx_timeout PLS_INTEGER DEFAULT NULL,
                    p_in_buffer_size PLS_INTEGER DEFAULT NULL,
                    p_out_buffer_size PLS_INTEGER DEFAULT NULL,
                    p_charset VARCHAR2 DEFAULT NULL) RETURN OREDIS_TCP_CONNECTION;
                    
  PROCEDURE CLOSE_TCP(p_tcpCon IN OUT NOCOPY UTL_TCP.CONNECTION);
  
  FUNCTION SEND_COMMAND(p_tcpCon UTL_TCP.CONNECTION, p_packed_cmd IN VARCHAR2) RETURN PLS_INTEGER;
  FUNCTION RECV_RESP(p_con UTL_TCP.CONNECTION) RETURN VARCHAR2;
                    
  FUNCTION EXEC(p_con UTL_TCP.CONNECTION, 
                p_cmd VARCHAR2) RETURN OREDIS_RESP;
  FUNCTION EXEC_PLAIN(p_con UTL_TCP.CONNECTION, 
                      p_cmd VARCHAR2) RETURN VARCHAR2;
  FUNCTION EXEC_RAW(p_con UTL_TCP.CONNECTION, 
                      p_cmd VARCHAR2) RETURN VARCHAR2;
                      
  PROCEDURE EXEC_ASYNC(p_con UTL_TCP.CONNECTION, p_cmd VARCHAR2);                    
                      
  
  FUNCTION PACK_COMMAND(p_cmd VARCHAR2) RETURN VARCHAR2;
  FUNCTION CREATE_RESPONSE(p_resp_str IN OUT NOCOPY VARCHAR2) RETURN OREDIS_RESP;
  FUNCTION CREATE_RESPONSE_ITEM(p_resp_item VARCHAR2) RETURN OREDIS_RESP_ITEM;
                    
  FUNCTION CONVERT_TCP_TYPE(p_oredis_tcp IN OUT NOCOPY OREDIS_TCP_CONNECTION) RETURN UTL_TCP.CONNECTION;
  FUNCTION CONVERT_TCP_TYPE(p_utl_tcp IN OUT NOCOPY UTL_TCP.CONNECTION) RETURN OREDIS_TCP_CONNECTION;                    
                    
  FUNCTION CONTAINS(p_varchar2tbl Varchar2Table, value VARCHAR2) RETURN BOOLEAN;
  FUNCTION SPLIT(p_str VARCHAR2, p_delim VARCHAR2) RETURN Varchar2Table;
  FUNCTION SPLIT_WITH_QUOTE(p_str VARCHAR2, p_delim VARCHAR2) RETURN Varchar2Table;
  
  FUNCTION CONVERT_RESP_TO_TEXT(response OREDIS_RESP) RETURN VARCHAR2;
  FUNCTION CONVERT_RESP_TO_TEXT(responses OREDIS_RESP_TABLE) RETURN VARCHAR2;
  
  FUNCTION HAS_RESPONSE(p_tcpCon IN OUT NOCOPY UTL_TCP.CONNECTION) RETURN BOOLEAN;
    
  
  PROCEDURE ASSERT_OK(resp OREDIS_RESP, msg VARCHAR2 DEFAULT NULL);
  PROCEDURE ASSERT_EQUAL(val1 VARCHAR2 , val2 VARCHAR2, msg VARCHAR2 DEFAULT NULL);
  PROCEDURE ASSERT_EQUAL(val1 NUMBER , val2 NUMBER, msg VARCHAR2 DEFAULT NULL);
  PROCEDURE ASSERT_EQUAL(val1 VARCHAR2 , val2 NUMBER, msg VARCHAR2 DEFAULT NULL);
  PROCEDURE ASSERT_EQUAL(val1 NUMBER , val2 VARCHAR2, msg VARCHAR2 DEFAULT NULL);
  
  
  NOT_SUPPORTED_COMMAND Varchar2Table := Varchar2Table(
'BGREWRITEAOF', 'BGSAVE',    'CLIENT',      'CONFIG', 
'DEBUG',        'DUMP',      'EVAL',        'EVALSHA',  
'LASTSAVE',     'MIGRATE',   'MONITOR',     'MOVE',         'OBJECT',  
'PSUBSCRIBE',   'PUBSUB',    'PUBLISH',     'PUNSUBSCRIBE', 'QUIT', 
'SAVE',         'SUBSCRIBE', 'SCRIPT',      'SHUTDOWN',     'SLAVEOF', 'SYNC',
'UNSUBSCRIBE',  'WAIT'
);


  NOT_SUPPORTED_COMMAND_CLUSTER Varchar2Table := Varchar2Table(
'BITOP', 'DBSIZE',    'DISCARD', 'EXEC',   'FLUSHALL', 'FLUSHDB',  'INFO',  'MULTI', 
'PING',  'RANDOMKEY', 'ROLE',    'SWAPDB', 'SELECT',   'SLOWLOG',  'WATCH', 'UNWATCH', 
'SCAN',  'SSCAN',     'HSCAN',   'ZSCAN'
);
  
    
  CRC16TAB NumberTable := NumberTable(
0,4129,8258,12387,16516,20645,24774,28903,
33032,37161,41290,45419,49548,53677,57806,61935,
4657,528,12915,8786,21173,17044,29431,25302,
37689,33560,45947,41818,54205,50076,62463,58334,
9314,13379,1056,5121,25830,29895,17572,21637,
42346,46411,34088,38153,58862,62927,50604,54669,
13907,9842,5649,1584,30423,26358,22165,18100,
46939,42874,38681,34616,63455,59390,55197,51132,
18628,22757,26758,30887,2112,6241,10242,14371,
51660,55789,59790,63919,35144,39273,43274,47403,
23285,19156,31415,27286,6769,2640,14899,10770,
56317,52188,64447,60318,39801,35672,47931,43802,
27814,31879,19684,23749,11298,15363,3168,7233,
60846,64911,52716,56781,44330,48395,36200,40265,
32407,28342,24277,20212,15891,11826,7761,3696,
65439,61374,57309,53244,48923,44858,40793,36728,
37256,33193,45514,41451,53516,49453,61774,57711,
4224,161,12482,8419,20484,16421,28742,24679,
33721,37784,41979,46042,49981,54044,58239,62302,
689,4752,8947,13010,16949,21012,25207,29270,
46570,42443,38312,34185,62830,58703,54572,50445,
13538,9411,5280,1153,29798,25671,21540,17413,
42971,47098,34713,38840,59231,63358,50973,55100,
9939,14066,1681,5808,26199,30326,17941,22068,
55628,51565,63758,59695,39368,35305,47498,43435,
22596,18533,30726,26663,6336,2273,14466,10403,
52093,56156,60223,64286,35833,39896,43963,48026,
19061,23124,27191,31254,2801,6864,10931,14994,
64814,60687,56684,52557,48554,44427,40424,36297,
31782,27655,23652,19525,15522,11395,7392,3265,
61215,65342,53085,57212,44955,49082,36825,40952,
28183,32310,20053,24180,11923,16050,3793,7920
);


END PKG_OREDIS;
/

prompt
prompt Creating package PKG_OREDIS_TEST
prompt ================================
prompt
create or replace package PKG_OREDIS_TEST is

  -- Author  : LSY
  -- Created : 2017-01-03  19:44:21 19:44:21
  -- Purpose : 
  
 --PRAGMA SERIALLY_REUSABLE;
  
  --g_cluster_config VARCHAR2(100) := '10.3.10.64:6379, 10.3.11.34:6379';
  
  FUNCTION TEST_CLUSTER_BASIC RETURN VARCHAR2;
  FUNCTION TEST_CLUSTER_MULTIKEY RETURN VARCHAR2;
  FUNCTION TEST_CLUSTER_ASYNC RETURN VARCHAR2;  
  
  FUNCTION TEST_CLUSTER_SLAVE_READ RETURN VARCHAR2;  
  
  -- Public type declarations
  FUNCTION EXEC_CMD_TEST RETURN VARCHAR2 ;
  FUNCTION EXEC_PERF_TEST RETURN VARCHAR2 ;
  
  
  FUNCTION EXEC_API_TEST RETURN VARCHAR2 ;
  
  --------------------------------------------
  --FUNCTION TEST_CLUSTER_PIPELINE_PERF RETURN VARCHAR2 ;
  --FUNCTION EXEC_PIPELINE_CMD_TEST RETURN VARCHAR2 ;
  
  --FUNCTION EXEC_PIPELINE_CMD_PERF_TEST RETURN VARCHAR2 ;


end PKG_OREDIS_TEST;
/

prompt
prompt Creating type OREDIS
prompt ====================
prompt
CREATE OR REPLACE TYPE OREDIS AS OBJECT
(
  -- Author  : LSY
  -- Created : 2017-01-03  9:53:27 9:53:27
  -- Purpose : 
  
  CONNECTION   OREDIS_TCP_CONNECTION, 
  ASYNC_CMD_COUNT NUMBER,
  
  CONSTRUCTOR FUNCTION OREDIS(SELF IN OUT NOCOPY OREDIS, 
                              p_config VARCHAR2) RETURN SELF AS RESULT,
  
  CONSTRUCTOR FUNCTION OREDIS(SELF IN OUT NOCOPY OREDIS, 
                              p_host VARCHAR2, 
                              p_port PLS_INTEGER, 
                              p_tx_timeout PLS_INTEGER DEFAULT NULL,    -- UTL_TCP.OPEN_CONNECTION => tx_timeout
                              p_in_buffer_size PLS_INTEGER DEFAULT NULL,
                              p_out_buffer_size PLS_INTEGER DEFAULT NULL,
                              --p_charset VARCHAR2 DEFAULT NULL,
                              p_password VARCHAR2 DEFAULT NULL,
                              p_db PLS_INTEGER DEFAULT 0  ) RETURN SELF AS RESULT,
                              
  MEMBER PROCEDURE INITIALIZE(SELF IN OUT NOCOPY OREDIS, 
                              p_host VARCHAR2, 
                              p_port PLS_INTEGER, 
                              p_tx_timeout PLS_INTEGER DEFAULT NULL,
                              p_in_buffer_size PLS_INTEGER DEFAULT NULL,
                              p_out_buffer_size PLS_INTEGER DEFAULT NULL,
                              --p_charset VARCHAR2 DEFAULT NULL,
                              p_password VARCHAR2 DEFAULT NULL,
                              p_db PLS_INTEGER DEFAULT 0  ),
                              
  MEMBER PROCEDURE CLOSE,
                                
  MEMBER FUNCTION EXEC(self IN OUT NOCOPY OREDIS, p_cmd VARCHAR2) RETURN OREDIS_RESP,  
  MEMBER FUNCTION EXEC_ASYNC(self IN OUT NOCOPY OREDIS, p_cmd VARCHAR2) RETURN NUMBER,
  --MEMBER FUNCTION EXEC_RAW(self IN OUT NOCOPY OREDIS, p_cmd VARCHAR2) RETURN VARCHAR2
  MEMBER FUNCTION GET_ASYNC_RESPONSE(self IN OUT NOCOPY OREDIS) RETURN OREDIS_RESP_TABLE,
    
  MEMBER FUNCTION GET_ADDRESS(self IN OUT NOCOPY OREDIS) RETURN VARCHAR2,  
  
  /* API */
  MEMBER FUNCTION SET_(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION PUT(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP,  -- same with SET_()
  MEMBER FUNCTION SETEX(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_value VARCHAR2, p_expiry NUMBER) RETURN OREDIS_RESP,
  MEMBER FUNCTION GET(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION DEL(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION EXIST(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP,
  
  MEMBER FUNCTION HSET(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_field VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION HGET(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_field VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION HDEL(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_field VARCHAR2) RETURN OREDIS_RESP,                                          
  MEMBER FUNCTION HEXISTS(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_field VARCHAR2) RETURN OREDIS_RESP,
  
  MEMBER FUNCTION LPUSH(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION LPOP(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION RPUSH(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP,  
  MEMBER FUNCTION RPOP(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION LRANGE(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_start NUMBER, p_end NUMBER) RETURN OREDIS_RESP,
  
  MEMBER FUNCTION SADD(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION SREM(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION SMEMBERS(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION SCARD(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP,
  
  MEMBER FUNCTION ZADD(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_score NUMBER, p_member VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION ZREM(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION ZCARD(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION ZRANGE(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_start NUMBER, p_end NUMBER) RETURN OREDIS_RESP,
  MEMBER FUNCTION ZRANGEBYSCORE(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_start VARCHAR2, p_end VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION ZRANK(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION ZSCORE(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION KEYS(self IN OUT NOCOPY OREDIS, p_pattern VARCHAR2) RETURN OREDIS_RESP
  
  
      
) NOT FINAL;
/

prompt
prompt Creating type OREDIS_NODE
prompt =========================
prompt
create or replace type OREDIS_NODE AS OBJECT
(
  NODEID    VARCHAR2(100),
  TYPE      VARCHAR(10),   -- PKG_OREDIS.MASTER_NODE_TYPE    PKG_OREDIS.SLAVE_NODE_TYPE
  SLOT_FROM NUMBER,
  SLOT_TO   NUMBER,
  AGENT     OREDIS,
    
  CONSTRUCTOR FUNCTION OREDIS_NODE(SELF IN OUT NOCOPY OREDIS_NODE,  
                                   p_nodeid VARCHAR2, 
                                   p_oredis  OREDIS,
                                   p_type VARCHAR2,                                     
                                   p_slot_from NUMBER,
                                   p_slot_to   NUMBER) RETURN SELF AS RESULT,
                                    
  MEMBER PROCEDURE CLOSE(SELF IN OUT NOCOPY OREDIS_NODE),
    
  MEMBER FUNCTION EXEC(self IN OUT NOCOPY OREDIS_NODE, p_cmd VARCHAR2) RETURN OREDIS_RESP,  
  MEMBER FUNCTION EXEC_ASYNC(self IN OUT NOCOPY OREDIS_NODE, p_cmd VARCHAR2) RETURN NUMBER,
  MEMBER FUNCTION GET_ASYNC_RESPONSE(self IN OUT NOCOPY OREDIS_NODE) RETURN OREDIS_RESP_TABLE,
    
  MEMBER FUNCTION GET_ADDRESS(self IN OUT NOCOPY OREDIS_NODE) RETURN VARCHAR2
)
/

prompt
prompt Creating type OREDIS_NODE_TABLE
prompt ===============================
prompt
CREATE OR REPLACE TYPE OREDIS_NODE_TABLE IS TABLE OF OREDIS_NODE
/

prompt
prompt Creating type OREDIS_MASTER_NODE
prompt ================================
prompt
CREATE OR REPLACE TYPE OREDIS_MASTER_NODE IS OBJECT (
    NODEID    VARCHAR2(100),
    NODE      OREDIS_NODE,
    SLAVES    OREDIS_NODE_TABLE,
    SLOT_FROM NUMBER,
    SLOT_TO   NUMBER,
    
    CONSTRUCTOR FUNCTION OREDIS_MASTER_NODE(SELF IN OUT NOCOPY OREDIS_MASTER_NODE,
                                            p_nodeid     VARCHAR2,
                                            p_master_node    OREDIS_NODE,                                           
                                            p_slot_from NUMBER,
                                            p_slot_to   NUMBER
    
    ) RETURN SELF AS RESULT,
    
    MEMBER PROCEDURE CLOSE(SELF IN OUT NOCOPY OREDIS_MASTER_NODE),
    
    MEMBER PROCEDURE ADD_SLAVE(SELF IN OUT NOCOPY OREDIS_MASTER_NODE, 
                              p_slave_node OREDIS_NODE),
                              
    MEMBER FUNCTION GET_SLAVE_COUNT(SELF IN OUT NOCOPY OREDIS_MASTER_NODE) RETURN NUMBER
  )
/

prompt
prompt Creating type OREDIS_MASTER_NODE_TABLE
prompt ======================================
prompt
CREATE OR REPLACE TYPE OREDIS_MASTER_NODE_TABLE IS TABLE OF OREDIS_MASTER_NODE
/

prompt
prompt Creating type OREDIS_CLUSTER_MAP
prompt ================================
prompt
CREATE OR REPLACE TYPE OREDIS_CLUSTER_MAP AS OBJECT (
    MASTER_NODES  OREDIS_MASTER_NODE_TABLE,
    
    CONSTRUCTOR FUNCTION OREDIS_CLUSTER_MAP(SELF IN OUT NOCOPY OREDIS_CLUSTER_MAP, p_config VARCHAR2) RETURN SELF AS RESULT,
    
    MEMBER FUNCTION GET_CLUSTER_INFO(p_endpoint_arr Varchar2Table, p_password VARCHAR2 DEFAULT NULL) RETURN VARCHAR2,
    --MEMBER PROCEDURE SETUP_CLUSTER_MAP(SELF IN OUT NOCOPY OREDIS_CLUSTER_MAP, p_nodes_info VARCHAR2, p_options Varchar2Table DEFAULT NULL),
    
    MEMBER FUNCTION GET_NODE_ANY(SELF IN OUT NOCOPY OREDIS_CLUSTER_MAP, 
                                 p_prefer_node_type VARCHAR2 DEFAULT 'M', 
                                 p_master_node OREDIS_MASTER_NODE DEFAULT NULL) RETURN OREDIS_NODE,
                                 
    MEMBER FUNCTION GET_NODE_BY_SLOT(SELF IN OUT NOCOPY OREDIS_CLUSTER_MAP, 
                                     p_slot NUMBER, 
                                     p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_NODE,  
                                    
    MEMBER FUNCTION GET_NODE_BY_ID(SELF IN OUT NOCOPY OREDIS_CLUSTER_MAP, 
                                   p_nodeid VARCHAR2) RETURN OREDIS_NODE,
                                   
    MEMBER FUNCTION GET_NODE_BY_ADDRESS(SELF IN OUT NOCOPY OREDIS_CLUSTER_MAP, 
                                        p_address VARCHAR2) RETURN OREDIS_NODE
                                                                        
      
  )
/

prompt
prompt Creating type OREDIS_CLUSTER
prompt ============================
prompt
CREATE OR REPLACE TYPE OREDIS_CLUSTER AS OBJECT (
    CONFIG        VARCHAR2(500),
    NODE_MAP  OREDIS_CLUSTER_MAP,
    
    ASYNC_CMD_COUNT NUMBER,
        
    CONSTRUCTOR FUNCTION OREDIS_CLUSTER(SELF IN OUT NOCOPY OREDIS_CLUSTER, p_config VARCHAR2) RETURN SELF AS RESULT,
    MEMBER PROCEDURE CLOSE(SELF IN OUT NOCOPY OREDIS_CLUSTER),
    
    MEMBER FUNCTION EXEC(SELF IN OUT NOCOPY OREDIS_CLUSTER, 
                         p_cmd VARCHAR2,                                    
                         p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP,   --Can't assign a value declared in package(PKG_OREDIS.MASTER_NODE_TYPE)
                                                                                        --as default value -_-;
                         
    MEMBER FUNCTION EXEC_ASYNC(self IN OUT NOCOPY OREDIS_CLUSTER, 
                               p_cmd VARCHAR2, 
                               p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN NUMBER,
                               
    MEMBER FUNCTION GET_NODE_BY_CMD(self IN OUT NOCOPY OREDIS_CLUSTER, 
                                    p_cmd VARCHAR2, 
                                    p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_NODE,                           
                               
    MEMBER FUNCTION GET_NODE_BY_KEY(self IN OUT NOCOPY OREDIS_CLUSTER, 
                                    p_key VARCHAR2, 
                                    p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_NODE,
    
    
    MEMBER FUNCTION GET_ASYNC_RESPONSE(self IN OUT NOCOPY OREDIS_CLUSTER) RETURN OREDIS_RESP_TABLE,
    
    MEMBER FUNCTION GET_HASH_KEY(p_params IN VARCHAR2) RETURN VARCHAR2,
    MEMBER FUNCTION GET_HASH_SLOT(KEY VARCHAR2) RETURN NUMBER,
    MEMBER FUNCTION BITOR(exp1 IN NUMBER, exp2 IN NUMBER) RETURN NUMBER,
    MEMBER FUNCTION BITXOR(exp1 IN NUMBER, exp2 IN NUMBER) RETURN NUMBER,
    
    
    /* API */
  MEMBER FUNCTION SET_(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION PUT(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION SETEX(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_value VARCHAR2, p_expiry NUMBER) RETURN OREDIS_RESP,
  MEMBER FUNCTION GET(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP,
  MEMBER FUNCTION DEL(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION EXIST(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP,
  
  MEMBER FUNCTION HSET(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_field VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION HGET(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_field VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP,
  MEMBER FUNCTION HDEL(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_field VARCHAR2) RETURN OREDIS_RESP,                                          
  MEMBER FUNCTION HEXISTS(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_field VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP,
  
  MEMBER FUNCTION LPUSH(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION LPOP(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION RPUSH(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP,  
  MEMBER FUNCTION RPOP(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION LRANGE(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_start NUMBER, p_end NUMBER, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP,
  
  MEMBER FUNCTION SADD(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION SREM(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION SMEMBERS(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP,
  MEMBER FUNCTION SCARD(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP,
  
  MEMBER FUNCTION ZADD(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_score NUMBER, p_member VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION ZREM(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP,
  MEMBER FUNCTION ZCARD(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP,
  MEMBER FUNCTION ZRANGE(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_start NUMBER, p_end NUMBER, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP,
  MEMBER FUNCTION ZRANGEBYSCORE(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_start VARCHAR2, p_end VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP,
  MEMBER FUNCTION ZRANK(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_member VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP,
  MEMBER FUNCTION ZSCORE(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_member VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP,
  MEMBER FUNCTION KEYS(self IN OUT NOCOPY OREDIS_CLUSTER, p_pattern VARCHAR2) RETURN OREDIS_RESP
    
    
  )
/

prompt
prompt Creating package body PKG_OREDIS
prompt ================================
prompt
CREATE OR REPLACE PACKAGE BODY PKG_OREDIS IS

FUNCTION OPEN_TCP(p_host VARCHAR2, 
                  p_port PLS_INTEGER DEFAULT 6379, 
                  p_tx_timeout PLS_INTEGER DEFAULT NULL,
                  p_in_buffer_size PLS_INTEGER DEFAULT NULL,
                  p_out_buffer_size PLS_INTEGER DEFAULT NULL,
                  p_charset VARCHAR2 DEFAULT NULL) RETURN OREDIS_TCP_CONNECTION IS
                        
  v_tcpConnection   UTL_TCP.CONNECTION;                    
  v_host varchar2(30);
  v_port PLS_INTEGER;
  v_host_port Varchar2Table;
  v_port_delim     varchar2(1) := ':';
  v_port_delim_pos NUMBER := 0;
BEGIN
  
  v_port_delim_pos := INSTRB(p_host, v_port_delim);
  
  IF v_port_delim_pos > 0 THEN
    v_host_port := split(p_host, v_port_delim);
    v_host := v_host_port(1);
    v_port := TO_NUMBER(v_host_port(2));
  ELSE
    v_host := p_host;
    v_port := p_port;
  END IF;
  
  v_tcpConnection := UTL_TCP.OPEN_CONNECTION(remote_host     => v_host,
                                             remote_port     => v_port,
                                             charset         => p_charset,      --'US7ASCII'  'KO16MSWIN949'
                                             in_buffer_size  => p_in_buffer_size,
                                             out_buffer_size => p_out_buffer_size,
                                             tx_timeout      => p_tx_timeout);   
  
  RETURN CONVERT_TCP_TYPE(v_tcpConnection);

EXCEPTION
    WHEN OTHERS THEN
      RAISE;   

END OPEN_TCP;

PROCEDURE CLOSE_TCP(p_tcpCon IN OUT NOCOPY UTL_TCP.CONNECTION) IS 
BEGIN
  
  UTL_TCP.CLOSE_CONNECTION(p_tcpCon);
  
END CLOSE_TCP;

/* p_packed_cmd  : REDIS Protocol formated cmd */
FUNCTION SEND_COMMAND(p_tcpCon UTL_TCP.CONNECTION, p_packed_cmd IN VARCHAR2) RETURN PLS_INTEGER IS  
  v_con   UTL_TCP.CONNECTION;
  v_len PLS_INTEGER;  
BEGIN
  
  v_con := p_tcpCon;
    
  v_len := UTL_TCP.WRITE_TEXT(v_con, p_packed_cmd);
  --UTL_TCP.FLUSH(v_con);
  
  RETURN v_len;
       
EXCEPTION
    WHEN OTHERS THEN
      RAISE;   
END SEND_COMMAND;


/* Extract single Resoponse from TCP Stream.*/
FUNCTION RECV_RESP(p_con UTL_TCP.CONNECTION) RETURN VARCHAR2 IS
  v_con   UTL_TCP.CONNECTION;
  v_resp_line VARCHAR2(32767);
  v_resp_lines VARCHAR2(32767);
  v_type char(1);
  v_resp_item_cnt PLS_INTEGER;
  v_str_resp_val VARCHAR2(32767);
  v_str_resp_len PLS_INTEGER;
BEGIN

  v_con := p_con;

  v_resp_line := UTL_TCP.GET_LINE(v_con);         -- if v_resp_line > 32767

  v_type := SUBSTRB(v_resp_line, 1, 1);

  --When BLPOP timeouted return *-1
  IF (v_resp_line = '$-1' || PKG_OREDIS.NEWLINE) OR (v_resp_line = '*-1' || PKG_OREDIS.NEWLINE) THEN  -- NIL  returned
    RETURN '$-1' || PKG_OREDIS.NEWLINE;
  END IF;

  IF v_type IN ('+', '-', ':') THEN
    v_resp_lines := v_resp_line;                                              -- if v_resp_line > 32767
  ELSIF v_type IN ('$') THEN
    v_resp_lines := v_resp_line;                                              -- if v_resp_line > 32767
    v_str_resp_len := TO_NUMBER(SUBSTRB(v_resp_lines, 2, LENGTHB(v_resp_lines) - 3));
        
    v_str_resp_val := UTL_TCP.GET_LINE(v_con, peek => FALSE);
    
    WHILE LENGTHB(v_str_resp_val) < v_str_resp_len LOOP
      v_str_resp_val := v_str_resp_val || UTL_TCP.GET_LINE(v_con);
    END LOOP;
    
    v_resp_lines := v_resp_lines || v_str_resp_val;

  ELSIF v_type IN ('*') THEN
    v_resp_item_cnt := TO_NUMBER(SUBSTRB(REPLACE(v_resp_line, NEWLINE, ''), 2));

    v_resp_lines := v_resp_line;                                     -- if v_resp_line > 32767

    WHILE v_resp_item_cnt > 0 LOOP
      v_resp_lines := v_resp_lines || RECV_RESP(v_con);              -- if v_resp_lines > 32767
      v_resp_item_cnt := v_resp_item_cnt - 1;
    END LOOP;

  END IF;

  RETURN v_resp_lines;

EXCEPTION
    WHEN OTHERS THEN
      RAISE;
END RECV_RESP;


FUNCTION EXEC(p_con UTL_TCP.CONNECTION, 
              p_cmd IN VARCHAR2) RETURN OREDIS_RESP IS 
  v_con   UTL_TCP.CONNECTION;
  v_packed_cmd VARCHAR2(32767);
  v_len PLS_INTEGER;  
  v_resp_lines VARCHAR2(32767);
BEGIN
    
  v_con := p_con;
  
  v_packed_cmd := PACK_COMMAND(p_cmd);
  
  v_len := SEND_COMMAND(v_con, v_packed_cmd);
  UTL_TCP.FLUSH(v_con);
    
  v_resp_lines :=  RECV_RESP(v_con); 
  
  RETURN CREATE_RESPONSE(v_resp_lines);
  
EXCEPTION
    WHEN OTHERS THEN
      RAISE;   
END EXEC;


FUNCTION EXEC_PLAIN(p_con UTL_TCP.CONNECTION, p_cmd IN VARCHAR2) RETURN VARCHAR2 IS 
  v_response OREDIS_RESP;
BEGIN
  
  v_response := EXEC(p_con, p_cmd);  
    
  RETURN v_response.item(1).STR;
  
EXCEPTION
    WHEN OTHERS THEN
      RAISE;   
END EXEC_PLAIN;


FUNCTION EXEC_RAW(p_con UTL_TCP.CONNECTION, p_cmd IN VARCHAR2) RETURN VARCHAR2 IS 
  v_con   UTL_TCP.CONNECTION;
  v_packed_cmd VARCHAR2(32767);
  v_len PLS_INTEGER;  
  v_resp_lines VARCHAR2(32767);
BEGIN
    
  v_con := p_con;
  
  v_packed_cmd := PACK_COMMAND(p_cmd);
  
  v_len := SEND_COMMAND(v_con, v_packed_cmd);
    
  v_resp_lines :=  'R' || NEWLINE || RECV_RESP(v_con); 
  
  RETURN CREATE_RESPONSE(v_resp_lines).item(1).STR;
  
EXCEPTION
    WHEN OTHERS THEN
      RAISE;   
END EXEC_RAW;


PROCEDURE EXEC_ASYNC(p_con UTL_TCP.CONNECTION, 
                    p_cmd            VARCHAR2) IS
  v_con   UTL_TCP.CONNECTION;
  v_packed_cmd VARCHAR2(32767);
  v_len PLS_INTEGER;  
BEGIN
    
  v_con := p_con;
  
  v_packed_cmd := PACK_COMMAND(p_cmd);
  
  v_len := SEND_COMMAND(v_con, v_packed_cmd);
  
EXCEPTION
    WHEN OTHERS THEN
      RAISE;   
END EXEC_ASYNC;


FUNCTION PACK_COMMAND(p_cmd IN VARCHAR2) RETURN VARCHAR2 IS
  v_token varchar2(32767);
  v_token_len number; 
  v_cmd_token Varchar2Table := Varchar2Table();  
  
  v_packed_cmd varchar2(32767);
  
BEGIN
  
  v_cmd_token := SPLIT_WITH_QUOTE(TRIM(p_cmd), ' ');
  
  v_packed_cmd := '*' || v_cmd_token.count || NEWLINE;    
  
  FOR i in 1..v_cmd_token.COUNT LOOP
    v_token := v_cmd_token(i);
    
    IF i = 1 and CONTAINS(NOT_SUPPORTED_COMMAND, UPPER(v_token)) THEN
      --RAISE E_NOT_SUPPORTED_COMMAND;
      RAISE_APPLICATION_ERROR(-20202, 'Not supported command  : ' || p_cmd, TRUE);
    END IF;
        
    v_token_len := lengthb(v_token);
        
    v_packed_cmd := v_packed_cmd || '$' || v_token_len || NEWLINE;
    v_packed_cmd := v_packed_cmd || v_token || NEWLINE;    
    
  END LOOP;
  
  RETURN v_packed_cmd;
  
EXCEPTION
    WHEN OTHERS THEN
      RAISE;   
END PACK_COMMAND;


FUNCTION CREATE_RESPONSE(p_resp_str IN OUT NOCOPY VARCHAR2) RETURN OREDIS_RESP IS
  v_type VARCHAR2(20);
  v_type_item VARCHAR2(20);
 
  v_redis_response OREDIS_RESP;
  
  v_new_line_idx PLS_INTEGER;  
  v_item_cnt PLS_INTEGER;
  
  v_prev_item_idx PLS_INTEGER;
  v_cur_item_idx PLS_INTEGER := 1;
  
  v_resp_item VARCHAR2(32767);  
  v_str_resp_len PLS_INTEGER;
BEGIN
  
  v_type := SUBSTRB(p_resp_str, 1, 1);  
  
  IF INSTRB(p_resp_str, '*', 1, 2) > 0 THEN      -- IF p_resp_str contains '*' 2 times or more, it's a Nested RESP Arrays.
    v_type := 'R';                              -- Nested RESP Arrays is hard to process with PL/SQL, because PL/SQL does not support Recursive Data Structure.   
    p_resp_str := v_type || NEWLINE || p_resp_str; -- For now, let's treat it as a RAW response
  END IF;                                   
  
  IF v_type in ('-') THEN    
    
    v_redis_response := NEW OREDIS_RESP(NULL, 1, OREDIS_RESP_ITEM_TABLE(), NULL, NULL);
    v_redis_response.item.EXTEND;
    v_redis_response.item(1) := CREATE_RESPONSE_ITEM(p_resp_str);
    v_redis_response.TYPE := v_redis_response.item(1).TYPE;
    v_redis_response.STR := v_redis_response.item(1).STR;
    
  ELSIF v_type in ('+', ':', '$', 'R') THEN

    v_redis_response := NEW OREDIS_RESP(NULL, 1, OREDIS_RESP_ITEM_TABLE(), NULL, NULL);
    v_redis_response.item.EXTEND;
    v_redis_response.item(1) := CREATE_RESPONSE_ITEM(p_resp_str);
    v_redis_response.TYPE := v_redis_response.item(1).TYPE;
    v_redis_response.STR := v_redis_response.item(1).STR;
    
    IF v_type = ':' THEN
      v_redis_response.INT := TO_NUMBER(v_redis_response.item(1).STR);
    END IF;
    
  ELSIF v_type IN ('*') THEN
    
    v_new_line_idx := INSTRB(p_resp_str, NEWLINE);
    v_item_cnt := TO_NUMBER(SUBSTRB(p_resp_str, 2, v_new_line_idx - 2));
            
    v_redis_response := NEW OREDIS_RESP(NULL, v_item_cnt, OREDIS_RESP_ITEM_TABLE(), NULL, NULL);  
    v_redis_response.TYPE := REPLY_ARRAY;  
    
    v_prev_item_idx := REGEXP_INSTR(p_resp_str, NEWLINE || '[+:$]') + 2;
    v_type_item := SUBSTRB(p_resp_str, v_prev_item_idx, 1);
    
    FOR i IN 1..v_item_cnt LOOP
      
      IF v_type_item = '+' OR v_type_item = ':' THEN
        v_cur_item_idx := REGEXP_INSTR(p_resp_str, NEWLINE || '[+:$]', v_prev_item_idx + 1) + 2; 
        
        IF v_cur_item_idx > 2 THEN
          v_resp_item := SUBSTRB(p_resp_str, v_prev_item_idx, v_cur_item_idx - v_prev_item_idx);   -- if v_response > 32767
          v_redis_response.item.EXTEND;
          v_redis_response.item(i) := CREATE_RESPONSE_ITEM(v_resp_item);
          
          v_prev_item_idx := v_cur_item_idx;
          v_type_item := SUBSTRB(p_resp_str, v_prev_item_idx, 1);
        ELSE
          v_resp_item := SUBSTRB(p_resp_str, v_prev_item_idx);
          v_redis_response.item.EXTEND;
          v_redis_response.item(i) := CREATE_RESPONSE_ITEM(v_resp_item);
        END IF;
      ELSIF v_type_item = '$' THEN
        v_new_line_idx := INSTRB(p_resp_str, NEWLINE, v_prev_item_idx);
        v_str_resp_len := TO_NUMBER(SUBSTRB(p_resp_str, v_prev_item_idx + 1, v_new_line_idx - v_prev_item_idx - 1));
        
        v_cur_item_idx := v_new_line_idx + v_str_resp_len + 4;
        
        v_resp_item := SUBSTRB(p_resp_str, v_prev_item_idx, v_cur_item_idx - v_prev_item_idx); 
        v_redis_response.item.EXTEND;
        v_redis_response.item(i) := CREATE_RESPONSE_ITEM(v_resp_item);
        
        v_prev_item_idx := v_cur_item_idx;
        v_type_item := SUBSTRB(p_resp_str, v_prev_item_idx, 1);
      ELSE
        RAISE_APPLICATION_ERROR(-20208, 'Invalid response string! [ ' || p_resp_str || ']', TRUE);  --E_PROTOCOL
      END IF;
      
    END LOOP;
  ELSE
    RAISE_APPLICATION_ERROR(-20208, 'Invalid response string! [ ' || p_resp_str || ']', TRUE);  --E_PROTOCOL
  END IF;
    
  RETURN v_redis_response;
  
END CREATE_RESPONSE;


FUNCTION CREATE_RESPONSE_ITEM(p_resp_item VARCHAR2) RETURN OREDIS_RESP_ITEM IS
  v_type_symbol CHAR(1);
  v_type NUMBER;
  v_response_item OREDIS_RESP_ITEM;
  v_idx NUMBER;  
  v_value VARCHAR2(32767);
BEGIN
  
  v_type_symbol := SUBSTRB(p_resp_item, 1, 1); 
  
  SELECT DECODE(v_type_symbol , '+', REPLY_STATUS, '-', REPLY_ERROR, ':', REPLY_INTEGER, '$', REPLY_STRING, 'R', REPLY_RAW)
    INTO v_type
    FROM DUAL; 
  
  IF v_type_symbol in ('+', '-') THEN    
    v_value := REPLACE(SUBSTRB(p_resp_item, 2), NEWLINE, '');    
    v_response_item := NEW OREDIS_RESP_ITEM(v_type, v_value, NULL);
  ELSIF v_type_symbol IN (':') THEN  
    v_value := REPLACE(SUBSTRB(p_resp_item, 2), NEWLINE, '');    
    v_response_item := NEW OREDIS_RESP_ITEM(v_type, v_value, TO_NUMBER(v_value));
  ELSIF v_type_symbol IN ('$') THEN
        
    v_idx := INSTRB(p_resp_item, NEWLINE);
    
    IF p_resp_item = '$-1' || NEWLINE OR p_resp_item = '*-1' || NEWLINE THEN
      v_value := '(nil)';
      v_type := REPLY_NIL;
    ELSE
      v_value := REPLACE(SUBSTRB(p_resp_item, v_idx + 2), NEWLINE, '');
    END IF;
    
    v_response_item := NEW OREDIS_RESP_ITEM(v_type, v_value, NULL);
  ELSIF v_type_symbol IN ('R') THEN
    v_idx := INSTRB(p_resp_item, NEWLINE);
    
    v_value := SUBSTRB(p_resp_item, v_idx + 2);
    
    v_response_item := NEW OREDIS_RESP_ITEM(v_type, v_value, NULL);    
  END IF;
    
  return v_response_item;
  
END CREATE_RESPONSE_ITEM;


/* Oracle Ojbect Type can't hava attributes of type which is defined inside a PL/SQL package.
So it is needed to convert UTL_TCP.CONNECTION to OREDIS_TCP_CONNECTION and vice versa.
*/
FUNCTION CONVERT_TCP_TYPE(p_oredis_tcp IN OUT NOCOPY OREDIS_TCP_CONNECTION) RETURN UTL_TCP.CONNECTION IS  
  v_tcpConnection   UTL_TCP.CONNECTION;    
BEGIN
  v_tcpConnection.remote_host := p_oredis_tcp.remote_host;
  v_tcpConnection.remote_port := p_oredis_tcp.remote_port;
  v_tcpConnection.local_host := p_oredis_tcp.local_host;
  v_tcpConnection.local_port := p_oredis_tcp.local_port;
  v_tcpConnection.charset := p_oredis_tcp.charset;
  v_tcpConnection.newline := p_oredis_tcp.newline;
  v_tcpConnection.tx_timeout := p_oredis_tcp.tx_timeout;
  v_tcpConnection.private_sd := p_oredis_tcp.private_sd; 
  
  RETURN v_tcpConnection;
END CONVERT_TCP_TYPE;

  
FUNCTION CONVERT_TCP_TYPE(p_utl_tcp IN OUT NOCOPY UTL_TCP.CONNECTION) RETURN OREDIS_TCP_CONNECTION IS     
  v_oredis_tcp_con OREDIS_TCP_CONNECTION := new OREDIS_TCP_CONNECTION(null,null,null,null,null,null,null,null );
BEGIN  
  v_oredis_tcp_con.remote_host := p_utl_tcp.remote_host;
  v_oredis_tcp_con.remote_port := p_utl_tcp.remote_port;
  v_oredis_tcp_con.local_host := p_utl_tcp.local_host;
  v_oredis_tcp_con.local_port := p_utl_tcp.local_port;
  v_oredis_tcp_con.charset := p_utl_tcp.charset;
  v_oredis_tcp_con.newline := p_utl_tcp.newline;
  v_oredis_tcp_con.tx_timeout := p_utl_tcp.tx_timeout;
  v_oredis_tcp_con.private_sd := p_utl_tcp.private_sd; 
  
  return v_oredis_tcp_con;
  
END CONVERT_TCP_TYPE;



FUNCTION CONTAINS(p_varchar2tbl Varchar2Table, VALUE VARCHAR2) RETURN BOOLEAN IS 
  --v_cnt number;
BEGIN 
  
  FOR i IN 1..p_varchar2tbl.count LOOP
    IF p_varchar2tbl(i) = VALUE THEN
      RETURN TRUE;
    END IF;
  END LOOP;  

  RETURN FALSE;  
    
/*  SELECT COUNT(*)
    INTO v_cnt
    FROM TABLE(p_varchar2tbl)
  WHERE COLUMN_VALUE = VALUE;

  IF v_cnt > 1 THEN
    RETURN TRUE;
  END IF;
    
  RETURN FALSE;*/
  
END;


FUNCTION SPLIT(p_str VARCHAR2, p_delim VARCHAR2) RETURN Varchar2Table IS
  v_str      LONG DEFAULT p_str || p_delim;
  v_n        NUMBER;
  v_data     Varchar2Table := Varchar2Table();
BEGIN
  LOOP
     v_n := INSTRB( v_str, p_delim );
     EXIT WHEN (NVL(v_n,0) = 0);
     v_data.extend;
     v_data( v_data.count ) := TRIM(SUBSTRB(v_str,1,v_n-1));
     v_str := SUBSTRB( v_str, v_n + LENGTHB(p_delim) );
  END LOOP;
  
  RETURN v_data;
END SPLIT;


FUNCTION SPLIT_WITH_QUOTE(p_str VARCHAR2, p_delim VARCHAR2) RETURN Varchar2Table IS
  v_str      LONG DEFAULT p_str || p_delim;
  v_idx_delim NUMBER;
  v_token    VARCHAR2(32767);  
  v_data     Varchar2Table := Varchar2Table();
  v_quoted_flag BOOLEAN := FALSE;
  v_quoted_token    VARCHAR2(32767);
  v_idx_temp NUMBER;
  v_idx_quote_mark NUMBER;
  v_quote_mark_cnt NUMBER;
BEGIN
  
  LOOP
     v_idx_delim := INSTRB( v_str, p_delim );
     EXIT WHEN (NVL(v_idx_delim,0) = 0);
     
     v_token := TRIM(SUBSTRB(v_str,1, v_idx_delim - 1));
     
     v_idx_quote_mark := 0;
     v_quote_mark_cnt := 1;
     
     v_idx_temp := INSTRB(v_token, '"', 1, v_quote_mark_cnt);
     
     WHILE v_idx_temp > 0 LOOP
       
        IF v_idx_temp = 1 THEN
          v_idx_quote_mark := v_idx_temp;
          EXIT;
        END IF;
        
        IF SUBSTR(v_token, v_idx_temp - 1, 1) <> '\' THEN
          v_idx_quote_mark := v_idx_temp;
          EXIT;
        END IF;
               
       v_quote_mark_cnt := v_quote_mark_cnt + 1; 
       v_idx_temp := INSTRB(v_token, '"', 1, v_quote_mark_cnt);
       
     END LOOP;
     
     IF v_idx_quote_mark = 1 OR (v_idx_quote_mark > 1 AND SUBSTR(v_token, v_idx_quote_mark - 1, 1) <> '\')  THEN
       IF v_quoted_flag = FALSE THEN
         
         IF LENGTHB(v_token) > 1 AND SUBSTRB(v_token, LENGTHB(v_token), 1) = '"' THEN
           v_data.extend;          
           v_data( v_data.count ) := SUBSTR(v_token, 2, LENGTHB(v_token) - 2);
           v_str := SUBSTRB( v_str, v_idx_delim + LENGTHB(p_delim) );           
         ELSE
           v_quoted_token := v_quoted_token || v_token || p_delim;
           v_str := SUBSTRB( v_str, v_idx_delim + LENGTHB(p_delim) );
         
           v_quoted_flag := TRUE;
         END IF;
       ELSE
         v_token := v_quoted_token || v_token;
         v_data.extend;          
         v_data( v_data.count ) := REPLACE(v_token, '"', '');
         v_str := SUBSTRB( v_str, v_idx_delim + LENGTHB(p_delim) );
         
         v_quoted_token := '';
         v_quoted_flag := FALSE;
       END IF;
     ELSE
       IF v_quoted_flag = TRUE THEN
         v_quoted_token := v_quoted_token || v_token || p_delim;
         v_str := SUBSTRB( v_str, v_idx_delim + LENGTHB(p_delim) );         
       ELSE
         v_data.extend;          
         v_data( v_data.count ) := REPLACE(v_token, '\"', '"');
         v_str := SUBSTRB( v_str, v_idx_delim + LENGTHB(p_delim) );
       END IF;
     END IF;
          
  END LOOP;
  
  RETURN v_data;
END SPLIT_WITH_QUOTE;


FUNCTION CONVERT_RESP_TO_TEXT(response OREDIS_RESP) RETURN VARCHAR2 IS
  v_resp_text VARCHAR2(32767);
BEGIN
  
  FOR i in 1..response.ITEM.COUNT LOOP
    v_resp_text := v_resp_text || '  ' || i || ')' || ' (' || response.ITEM(i).TYPE || ')' || response.ITEM(i).STR || NEWLINE;
  END LOOP;
  
  RETURN v_resp_text;
END CONVERT_RESP_TO_TEXT;


FUNCTION CONVERT_RESP_TO_TEXT(responses OREDIS_RESP_TABLE) RETURN VARCHAR2 IS
  v_resp_text VARCHAR2(32767);
BEGIN
  
  FOR i in 1..responses.COUNT LOOP
    v_resp_text := v_resp_text || i || ')' || NEWLINE || CONVERT_RESP_TO_TEXT(responses(i)) || NEWLINE;
  END LOOP;
  
  RETURN v_resp_text;
END CONVERT_RESP_TO_TEXT;


FUNCTION HAS_RESPONSE(p_tcpCon IN OUT NOCOPY UTL_TCP.CONNECTION) RETURN BOOLEAN IS
BEGIN
  IF UTL_TCP.AVAILABLE(p_tcpCon) > 0 THEN
    RETURN TRUE;
  END IF;
  
  RETURN FALSE;
END HAS_RESPONSE;


PROCEDURE ASSERT_OK(resp OREDIS_RESP, msg VARCHAR2 DEFAULT NULL) IS
BEGIN
  IF resp.STR <> 'OK' THEN
    RAISE_APPLICATION_ERROR(-20201, 'ASSERT OK FAIL   ' || resp.STR || ' - ' || msg, TRUE);
  END IF;
END ASSERT_OK;


PROCEDURE ASSERT_EQUAL(val1 VARCHAR2 , val2 VARCHAR2, msg VARCHAR2 DEFAULT NULL) IS
BEGIN 
  IF val1 <> val2 OR (val1 IS NULL AND val2 IS NOT NULL) OR (val1 IS NOT NULL AND val2 IS NULL)THEN
    RAISE_APPLICATION_ERROR(-20201, 'ASSERT FAIL   ' || nvl(val1, 'NULL') || ' != ' || nvl(val2, 'NULL') || ' ' || msg, TRUE);
  END IF;
END ASSERT_EQUAL;


PROCEDURE ASSERT_EQUAL(val1 NUMBER , val2 NUMBER, msg VARCHAR2 DEFAULT NULL) IS
BEGIN
  IF val1 <> val2 OR (val1 IS NULL AND val2 IS NOT NULL) OR (val1 IS NOT NULL AND val2 IS NULL)THEN
    RAISE_APPLICATION_ERROR(-20201, 'ASSERT FAIL   ' || val1 || ' != ' || val2 || ' ' || msg, TRUE);
  END IF;
END ASSERT_EQUAL;


PROCEDURE ASSERT_EQUAL(val1 VARCHAR2 , val2 NUMBER, msg VARCHAR2 DEFAULT NULL) IS
BEGIN
  IF TO_NUMBER(val1) <> val2 OR (val1 IS NULL AND val2 IS NOT NULL) OR (val1 IS NOT NULL AND val2 IS NULL)  THEN
    RAISE_APPLICATION_ERROR(-20201, 'ASSERT FAIL   ' || nvl(val1, 'NULL') || ' != ' || val2 || ' ' || msg, TRUE);
  END IF;
END ASSERT_EQUAL;


PROCEDURE ASSERT_EQUAL(val1 NUMBER , val2 VARCHAR2, msg VARCHAR2 DEFAULT NULL) IS
BEGIN
  IF val1 <> TO_NUMBER(val2) OR (val1 IS NULL AND val2 IS NOT NULL) OR (val1 IS NOT NULL AND val2 IS NULL) THEN
    RAISE_APPLICATION_ERROR(-20201, 'ASSERT FAIL   ' || val1 || ' != ' || nvl(val2, 'NULL') || ' ' || msg, TRUE);
  END IF;
END ASSERT_EQUAL;


PROCEDURE GRANT_NETWORK_PRIVILEGE IS  -- NEED SYS USER PRIVILEGE
BEGIN 
  
  --dbms_network_acl_admin.create_acl('network_services.xml','Network connection permission', 'username', TRUE, 'connect');
  
  --dbms_network_acl_admin.add_privilege('network_services.xml','username',true,'resolve');
  
  --dbms_network_acl_admin.assign_acl('network_services.xml','*'); -- allow connectting to all hosts and ports
   null;

END GRANT_NETWORK_PRIVILEGE;

END PKG_OREDIS;
/

prompt
prompt Creating package body PKG_OREDIS_TEST
prompt =====================================
prompt
CREATE OR REPLACE PACKAGE BODY PKG_OREDIS_TEST IS

FUNCTION TEST_CLUSTER_BASIC RETURN VARCHAR2 IS
  redis_cluster OREDIS_CLUSTER;
  v_response OREDIS_RESP;
  v_test_result VARCHAR2(32767) := 'All Test Passed!';
  v_responses OREDIS_RESP_TABLE;
  
  v_key1 VARCHAR2(32767);
  v_int_val1 NUMBER;
  
  v_str_val1 VARCHAR2(32767);
  v_str_val2 VARCHAR2(32767);  
  
BEGIN
        
  redis_cluster := NEW OREDIS_CLUSTER('10.3.10.52:6379, 10.3.11.35:6379,readSlave=true'); --,TIMEOUT=1,inBufferSize=1000,outBufferSize=2000,password=pass
  
  --v_response := redis_cluster.EXEC('MULTI '); -- not supported command test
  
  --HASH TAG TEST
/*  v_str_val1 := redis_cluster.GET_HASH_KEY('{USER1000}.FOLLOW');
  ASSERT_EQUAL(v_str_val1, 'USER1000');
  v_str_val1 := redis_cluster.GET_HASH_KEY('FOO{}{BAR}');
  ASSERT_EQUAL(v_str_val1, 'FOO{}{BAR}');
  v_str_val1 := redis_cluster.GET_HASH_KEY('FOO{{BAR}}');
  ASSERT_EQUAL(v_str_val1, '{BAR');
  v_str_val1 := redis_cluster.GET_HASH_KEY('FOO{BAR}{ZAP}');
  ASSERT_EQUAL(v_str_val1, 'BAR');*/
  
  v_response := redis_cluster.KEYS('*2017*');
  
  FOR i IN 1..v_response.ITEM_CNT LOOP
    v_str_val1 := v_response.ITEM(i).STR;

    DBMS_OUTPUT.PUT_LINE(v_str_val1);
  END LOOP;
  
    
  v_key1 := 'myKEY1';
    
  
  v_response := redis_cluster.EXEC('DEL ' || v_key1);
  v_response := redis_cluster.EXEC('GET ' || v_key1);
  
  --When the key is not exists, GET returns a response of REPLY_NIL type and the string value is '(nil)'.
  PKG_OREDIS.ASSERT_EQUAL(v_response.TYPE, PKG_OREDIS.REPLY_NIL);
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, PKG_OREDIS.NIL);  -- (nil)
  
  
  v_response := redis_cluster.EXEC('SET ' || v_key1 || ' ' || 'Hello_World');  --try to execute on slave node, when not possible executed on master node
  v_response := redis_cluster.EXEC('GET ' || v_key1);
   
  
  --If you know the type of response, you don't need to be rambling like this 
  --but you need to chek ERROR (or nil)
  IF v_response.TYPE = PKG_OREDIS.REPLY_ERROR THEN
    v_str_val1 := v_response.STR;
  ELSIF v_response.TYPE = PKG_OREDIS.REPLY_INTEGER THEN  -- If the response type is INTEGER, you can get both integer and string value.
    v_str_val1 := v_response.STR;
    v_int_val1 := v_response.INT;
  ELSIF v_response.TYPE = PKG_OREDIS.REPLY_ARRAY THEN
    FOR i IN 1..v_response.ITEM_CNT LOOP
      v_str_val1 := v_response.ITEM(i).STR;
      
      IF v_response.ITEM(i).TYPE = PKG_OREDIS.REPLY_INTEGER THEN
        v_int_val1 :=  v_response.ITEM(i).INT;
      END IF;
    END LOOP;
  ELSE                               --REPLY_STRING, REPLY_NIL, REPLY_STATUS
    v_str_val1 := v_response.STR;
  END IF;
  
  
  --When SET a value includes White Space, wrap the value with Double Quotation(") 
  v_response := redis_cluster.EXEC('SET ' || v_key1 || ' ' || '" abc def "');
  v_response := redis_cluster.EXEC('GET ' || v_key1);
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, ' abc def ', 'ASSERT FAIL : When SET a value includes White Space');
  
  --When SET a value includes Single Quotation('), use double Single Quotation('') instead of one Single Quotation(')
  v_response := redis_cluster.EXEC('SET ' || v_key1 || ' ' || 'abc''123''def');
  v_response := redis_cluster.EXEC('GET ' || v_key1);
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, 'abc''123''def', 'ASSERT FAIL : When SET a value includes ('')');
  
  --When SET a value includes Double Quotaion("), use (\") instead of Double Quotation(") 
  --v_response := redis_cluster.EXEC('SET ' || v_key1 || ' ' || 'abc\"123\"def');
  v_response := redis_cluster.EXEC('SET ' || v_key1 || ' ' || 'abc\"123\"def');
  v_response := redis_cluster.EXEC('GET ' || v_key1);
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, 'abc"123"def', 'ASSERT FAIL : When SET a value wraped with (")');
  
  --When SET a value wraped with Single Quotation('), wrap the value with Double Quotation(") and use Double Single Quotation('') instead of (')
  v_response := redis_cluster.EXEC('SET ' || v_key1 || ' ' || '"''abc''"');
  v_response := redis_cluster.EXEC('GET ' || v_key1);
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, '''abc''', 'ASSERT FAIL : When SET a value wraped with ('')');
  
  --When SET a value wraped with Double Quotation("), wrap the value with double Double Quotation("") instead of (")
  v_response := redis_cluster.EXEC('SET ' || v_key1 || ' ' || '""abc""');
  v_response := redis_cluster.EXEC('GET ' || v_key1);
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, '"abc"', 'ASSERT FAIL : When SET a value wraped with (")');
  
  
  
  v_response := redis_cluster.EXEC('DEL ' || v_key1);    
  v_response := redis_cluster.EXEC('EXISTS ' || v_key1);  
  v_int_val1 := v_response.INT;  
  PKG_OREDIS.ASSERT_EQUAL(v_int_val1, 0, 'EXISTS Returns ' || v_int_val1 || '. It shoud be ' || 0);
  
  v_response := redis_cluster.EXEC('SET ' || v_key1 || ' ' || 'Hello');
  PKG_OREDIS.ASSERT_EQUAL(v_response.TYPE, PKG_OREDIS.REPLY_STATUS);
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, PKG_OREDIS.OK);
    
  v_response := redis_cluster.EXEC('APPEND ' || v_key1 || ' ' || '" World"');
  PKG_OREDIS.ASSERT_EQUAL(v_response.TYPE, PKG_OREDIS.REPLY_INTEGER);
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, LENGTHB('Hello World'));  
  
  v_response := redis_cluster.EXEC('GET ' || v_key1);
  PKG_OREDIS.ASSERT_EQUAL(v_response.TYPE, PKG_OREDIS.REPLY_STRING);
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, 'Hello World');
  
    
  v_str_val1 := 'foobar';  
  
  v_response := redis_cluster.EXEC('DEL ' || v_key1);
  v_response := redis_cluster.EXEC('SET ' || v_key1 || ' ' || v_str_val1);
  
  
  v_response := redis_cluster.EXEC('BITCOUNT ' || v_key1);  
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 26);
  
  v_response := redis_cluster.EXEC('BITCOUNT ' || v_key1 || ' 0 0');  
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 4);
  
  v_response := redis_cluster.EXEC('BITCOUNT ' || v_key1 || ' 1 1');  
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 6);
  
  
  v_response := redis_cluster.EXEC('BITFIELD ' || v_key1 || ' incrby i5 100 1 get u4 0');  
  PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(1).INT, 1);
  PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(2).INT, 6);  
  
  
  v_response := redis_cluster.EXEC('DEL list2 ');  
  v_response := redis_cluster.EXEC('RPUSH list2 one');  
  
  v_response := redis_cluster.EXEC('LLEN list2'); 
  PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(1).INT, 1);
    
  v_response := redis_cluster.EXEC('LRANGE list2 0 -1'); 
  PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(1).STR, 'one');
  
  v_response := redis_cluster.EXEC('RPOP list2');
  PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(1).STR, 'one');
  
  v_response := redis_cluster.EXEC('LPUSH list2 two'); 
  
  v_response := redis_cluster.EXEC('LRANGE list2 0 -1'); 
  PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(1).STR, 'two');
  v_response := redis_cluster.EXEC('LINDEX list2 0'); 
  PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(1).STR, 'two');
    
  v_response := redis_cluster.EXEC('LPOP list2');
  PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(1).STR, 'two');
    
  
  v_response := redis_cluster.EXEC('SET ' || v_key1 || ' 10');
  v_response := redis_cluster.EXEC('INCR ' || v_key1);
  v_response := redis_cluster.EXEC('INCRBY ' || v_key1 || ' 2');
  v_response := redis_cluster.EXEC('DECR ' || v_key1);
  v_response := redis_cluster.EXEC('DECRBY ' || v_key1 || ' 3');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 9);
  
  v_response := redis_cluster.EXEC('SET ' || v_key1 || ' 5.0e3');
  v_response := redis_cluster.EXEC('INCRBYFLOAT ' || v_key1 || ' 2.0E2');
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, '5200');
    
  
  v_response := redis_cluster.EXEC('GEOADD sicily 13.361389 38.115556 palermo 15.087269 37.502669 catania');  
  v_response := redis_cluster.EXEC('GEODIST sicily palermo catania');
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, '166274.1516');
  
  v_response := redis_cluster.EXEC('GEORADIUS sicily 15 37 100 km');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(1).STR, 'catania');
  
  v_response := redis_cluster.EXEC('GEORADIUS sicily 15 37 200 km');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(1).STR, 'palermo');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(2).STR, 'catania');
  
  v_response := redis_cluster.EXEC('GEOHASH sicily palermo catania');
  v_str_val1  := v_response.item(1).STR;
  v_str_val2  := v_response.item(2).STR;
  
  
  v_key1 := 'myhash';
  
  v_response := redis_cluster.EXEC('DEL ' || v_key1);  
  v_response := redis_cluster.EXEC('HSET ' || v_key1 || ' field1 Hello');
  v_response := redis_cluster.EXEC('HSET ' || v_key1 || ' field2 world');
  v_response := redis_cluster.EXEC('HEXISTS ' || v_key1 || ' field1');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 1);
  
  
  v_response := redis_cluster.EXEC('HGET ' || v_key1 || ' field1');
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, 'Hello');
  
  v_response := redis_cluster.EXEC('HLEN ' || v_key1);
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 2);
  
  v_response := redis_cluster.EXEC('HKEYS ' || v_key1);
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(1).STR, 'field1');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(2).STR, 'field2');
  
  v_response := redis_cluster.EXEC('HMGET ' || v_key1 || ' field1 field2 nofield');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(1).STR, 'Hello');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(2).STR, 'world');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(3).STR, '(nil)');
  
  v_response := redis_cluster.EXEC('HSTRLEN ' || v_key1 || ' field1');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, LENGTHB('Hello'));
  
  v_response := redis_cluster.EXEC('HMSET ' || v_key1 || ' field3 "I am" field4 oredis');  
  v_response := redis_cluster.EXEC('HVALS ' || v_key1);
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(1).STR, 'Hello');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(4).STR, 'oredis');
  
  v_response := redis_cluster.EXEC('HGETALL ' || v_key1);
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(1).STR, 'field1');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(2).STR, 'Hello');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(3).STR, 'field2');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(4).STR, 'world');  
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(5).STR, 'field3');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(6).STR, 'I am');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(7).STR, 'field4');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(8).STR, 'oredis');
  
  v_response := redis_cluster.EXEC('HDEL ' || v_key1 || ' field1');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 1);
  
  v_response := redis_cluster.EXEC('HEXISTS ' || v_key1 || ' field1');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 0);
  
  v_response := redis_cluster.EXEC('HDEL ' || v_key1 || ' field1');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 0);
    
  v_response := redis_cluster.EXEC('HSET ' || v_key1 || ' field1 10.50');
  v_response := redis_cluster.EXEC('HINCRBYFLOAT ' || v_key1 || ' field1 0.1');
  v_response := redis_cluster.EXEC('HINCRBYFLOAT ' || v_key1 || ' field1 -5');
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, '5.6');            -- HINCRBYFLOAT  returns string value
  
  
  v_response := redis_cluster.EXEC('DEL mySET');  
  v_response := redis_cluster.EXEC('SADD mySET HELLO');
  v_response := redis_cluster.EXEC('SADD mySET WORLD');
  v_response := redis_cluster.EXEC('SADD mySET WORLD');
  v_response := redis_cluster.EXEC('SCARD mySET');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 2);
  
  v_response := redis_cluster.EXEC('SORT mySET ALPHA');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(1).STR, 'HELLO');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(2).STR, 'WORLD');  
  
  --v_response := redis_cluster.EXEC('SMEMBERS mySET');
  
  v_response := redis_cluster.EXEC('DEL myZSET');
  
  v_response := redis_cluster.EXEC('ZADD myZSET 1 one');
  v_response := redis_cluster.EXEC('ZADD myZSET 1 uno');
  v_response := redis_cluster.EXEC('ZADD myZSET 2 two 3 three');
  v_response := redis_cluster.EXEC('ZRANGE myZSET 0 -1 WITHSCORES');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(1).STR, 'one');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(3).STR, 'uno');  
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(5).STR, 'two');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(7).STR, 'three');    
  v_response := redis_cluster.EXEC('ZCARD myZSET');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 4);   
  v_response := redis_cluster.EXEC('ZCOUNT myZSET 1 2');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 3);  
  
  
  v_response := redis_cluster.EXEC('DEL myZSET');
  v_response := redis_cluster.EXEC('ZADD myZSET 0 a 0 b 0 c 0 d');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 4);
  v_response := redis_cluster.EXEC('ZLEXCOUNT myZSET - +');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 4);
  v_response := redis_cluster.EXEC('ZLEXCOUNT myZSET [a [c');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 3);
  v_response := redis_cluster.EXEC('ZRANGEBYLEX myZSET [a [c');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(1).STR, 'a');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(2).STR, 'b'); 
  v_response := redis_cluster.EXEC('ZRANK myZSET b');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(1).INT, 1);
  v_response := redis_cluster.EXEC('ZREM myZSET a');
  v_response := redis_cluster.EXEC('ZRANK myZSET b');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(1).INT, 0);
  
  
  v_response := redis_cluster.EXEC('set key12 1');
  v_str_val1 := v_response.str;
  v_response := redis_cluster.EXEC('get key12', 'S');
  v_str_val1 := v_response.str;
  
  v_response := redis_cluster.EXEC('GET myKEY1');
  v_str_val1 := v_response.str;
    

  redis_cluster.CLOSE();

  RETURN v_test_result;
  
EXCEPTION
  /*WHEN PKG_OREDIS.E_ASSERT THEN
    IF redis_cluster IS NOT NULL THEN
       redis_cluster.CLOSE();
    END IF;
    DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
    DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_STACK);
    --DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_CALL_STACK);    
    RETURN SQLERRM;
  WHEN PKG_OREDIS.E_NOT_SUPPORTED_COMMAND THEN
    IF redis_cluster IS NOT NULL THEN
       redis_cluster.CLOSE();
    END IF;
    DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
    DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_STACK);
    RETURN SQLERRM;*/
  WHEN OTHERS THEN 
    IF redis_cluster IS NOT NULL THEN
       redis_cluster.CLOSE();
    END IF;
    
    v_test_result := SQLERRM || PKG_OREDIS.NEWLINE || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    DBMS_OUTPUT.PUT_LINE(v_test_result);
    
    RETURN v_test_result;
    
END TEST_CLUSTER_BASIC;


FUNCTION TEST_CLUSTER_MULTIKEY RETURN VARCHAR2 IS
  redis_cluster OREDIS_CLUSTER;
  v_response OREDIS_RESP;
  v_test_result VARCHAR2(32767) := 'All Test Passed!';
  
BEGIN
    
  redis_cluster := NEW OREDIS_CLUSTER('10.3.10.52, 10.3.10.64:6379');
  --redis_cluster := NEW OREDIS_CLUSTER('10.3.10.52:6379, 10.3.11.35:6379,readSlave=true');  --('10.3.11.34:6379, 10.3.10.64:6379');
                                      
  
  v_response := redis_cluster.EXEC('MSET {user:1000}.fname Michael {user:1000}.lname Jackson');
  v_response := redis_cluster.EXEC('MGET {user:1000}.fname {user:1000}.lname');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(1).STR, 'Michael');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(2).STR, 'Jackson');
  
  v_response := redis_cluster.EXEC('EXISTS {user:1000}.fname {user:1000}.lname {user:1000}.nosuchkey ');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(1).INT, 2, 'at TEST_CLUSTER_MULTIKEY() EXISTS nokey');
  
  v_response := redis_cluster.EXEC('DEL {user:1000}.fname {user:1000}.lname');
  v_response := redis_cluster.EXEC('EXISTS {user:1000}.fname {user:1000}.lname {user:1000}.nosuchkey ');
  PKG_OREDIS.ASSERT_EQUAL(v_response.item(1).INT, 0, 'at TEST_CLUSTER_MULTIKEY() EXISTS nokey');
  
  

  redis_cluster.CLOSE();

  RETURN v_test_result;
  
EXCEPTION
  WHEN OTHERS THEN 
    IF redis_cluster IS NOT NULL THEN
       redis_cluster.CLOSE();
    END IF;
    
    v_test_result := SQLERRM || PKG_OREDIS.NEWLINE || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    DBMS_OUTPUT.PUT_LINE(v_test_result);
    
    RETURN v_test_result;
END TEST_CLUSTER_MULTIKEY;


FUNCTION TEST_CLUSTER_ASYNC RETURN VARCHAR2 IS
  --g_cluster_config VARCHAR2(100) := '10.3.10.64:6379, 10.3.11.34:6379';
  redis_cluster OREDIS_CLUSTER;
  v_response OREDIS_RESP;
  v_plain_response VARCHAR2(32767);
  v_responses OREDIS_RESP_TABLE;
  v_start_time DATE;
  
  v_key VARCHAR2(32767);
  v_int_val NUMBER;
  v_str_val VARCHAR2(32767);
  
  v_test_cnt NUMBER := 500;
  v_oredis_node OREDIS_NODE;
BEGIN
  
  redis_cluster := NEW OREDIS_CLUSTER('10.3.10.52:6379, 10.3.11.35:6379,readSlave=true');
  
  
  
  
  
  v_start_time := SYSDATE;
  
  FOR i IN 1..v_test_cnt LOOP
     v_int_val := redis_cluster.EXEC_ASYNC('SET a'||i || ' ' || (100000000 + i));
  END LOOP; 
    
  v_responses := redis_cluster.GET_ASYNC_RESPONSE();  
   
  v_plain_response := v_plain_response || chr(10) || 'EXEC_ASYNC takes ' || TO_CHAR(ROUND((SYSDATE - v_start_time) * 24 * 3600)) || ' second(s)'; 



  
  v_start_time := SYSDATE;
  
  FOR i IN 1..v_test_cnt LOOP
     v_response := redis_cluster.EXEC('SET r'||i || ' ' || (100000000 + i));
  END LOOP; 
    
  v_plain_response := v_plain_response || chr(10) || 'REPEAT EXEC takes ' || TO_CHAR(ROUND((SYSDATE - v_start_time) * 24 * 3600)) || ' second(s)';
    

  
  
  redis_cluster.CLOSE();

  RETURN v_plain_response;
  
EXCEPTION
  WHEN OTHERS THEN 
    IF redis_cluster IS NOT NULL THEN
       redis_cluster.CLOSE();
    END IF;
    
    v_plain_response := SQLERRM || PKG_OREDIS.NEWLINE || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    DBMS_OUTPUT.PUT_LINE(v_plain_response);
    
    RETURN v_plain_response;
END TEST_CLUSTER_ASYNC;



FUNCTION TEST_CLUSTER_SLAVE_READ RETURN VARCHAR2 IS
  --g_cluster_config VARCHAR2(100) := '10.3.10.64:6379, 10.3.11.34:6379';
  redis_cluster OREDIS_CLUSTER;
  v_response OREDIS_RESP;
  v_plain_response VARCHAR2(32767);
  v_responses OREDIS_RESP_TABLE;
  v_start_time DATE;
  
  v_key VARCHAR2(32767);
  v_int_val NUMBER;
  v_str_val VARCHAR2(32767);
  
  v_test_cnt NUMBER := 1000;
  v_oredis_node OREDIS_NODE;
BEGIN
  
  redis_cluster := NEW OREDIS_CLUSTER('10.3.10.52:6379, 10.3.11.35:6379,readSlave=true');
  
  v_start_time := SYSDATE;
  
  /*FOR i IN 1..v_test_cnt LOOP
     v_int_val := redis_cluster.EXEC_ASYNC('SET a'||i || ' ' || (100000000 + i));
  END LOOP; 
    
  v_responses := redis_cluster.GET_ASYNC_RESPONSE();  
   
  v_plain_response := 'EXEC_ASYNC takes ' || TO_CHAR(ROUND((SYSDATE - v_start_time) * 24 * 3600)) || ' second(s)'; 
    */
  
  
/*  v_start_time := SYSDATE;
  
  FOR i IN 1..500 LOOP
    --v_response := redis_cluster.EXEC('DEL r'||i);
    v_int_val := redis_cluster.EXEC_ASYNC('SET r'||i || ' ' || (100000000 + i));
  END LOOP; 
  
  v_responses := redis_cluster.GET_ASYNC_RESPONSE();  
    
  v_plain_response := v_plain_response || chr(10) || 'REPEAT EXEC takes ' || TO_CHAR(ROUND((SYSDATE - v_start_time) * 24 * 3600)) || ' second(s)';
  */
  
  v_start_time := SYSDATE;
  
  FOR i IN 1..v_test_cnt LOOP
    --v_response := redis_cluster.EXEC('DEL r'||i);
    v_response := redis_cluster.EXEC('SET r'||i || ' ' || (100000000 + i));
    --v_response := redis_cluster.EXEC('DEL r'||i);
  END LOOP; 
    
  v_plain_response := v_plain_response || chr(10) || 'REPEAT EXEC2 takes ' || TO_CHAR(ROUND((SYSDATE - v_start_time) * 24 * 3600)) || ' second(s)';
  
    

  redis_cluster.CLOSE();

  RETURN v_plain_response;
  
EXCEPTION
  WHEN OTHERS THEN 
    IF redis_cluster IS NOT NULL THEN
       redis_cluster.CLOSE();
    END IF;
    
    v_plain_response := SQLERRM || PKG_OREDIS.NEWLINE || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    DBMS_OUTPUT.PUT_LINE(v_plain_response);
    
    RETURN v_plain_response;
END TEST_CLUSTER_SLAVE_READ;

/*
FUNCTION TEST_CLUSTER_SLAVE_READ RETURN VARCHAR2 IS
  --g_cluster_config VARCHAR2(100) := '10.3.10.64:6379, 10.3.11.34:6379';
  redis_cluster OREDIS_CLUSTER;
  v_response OREDIS_RESP;
  v_plain_response VARCHAR2(32767);
  v_responses OREDIS_RESP_TABLE;
  v_start_time DATE;
  
  v_key VARCHAR2(32767);
  v_int_val NUMBER;
  v_str_val VARCHAR2(32767);
  
  v_test_cnt NUMBER := 100000;
  v_oredis_node OREDIS_NODE;
BEGIN
  
  redis_cluster := NEW OREDIS_CLUSTER('10.3.11.34:6379, 10.3.10.64:6379');
  
  v_start_time := SYSDATE;
  
  FOR i IN 1..v_test_cnt LOOP
     --v_response := redis_cluster.EXEC('SET key12 1');
     v_response := redis_cluster.EXEC('SET r'||i || ' ' || (100000000 + i));
  END LOOP; 
       
  v_plain_response := 'EXEC GET ON MASTER takes ' || TO_CHAR(ROUND((SYSDATE - v_start_time) * 24 * 3600)) || ' second(s)'; 
  
  
  v_start_time := SYSDATE;
  
  \*FOR i IN 1..v_test_cnt LOOP
     v_response := redis_cluster.EXEC('get key12', 'S');
  END LOOP; *\
    
  v_plain_response := v_plain_response || chr(10) || 'EXEC GET ON SLAVE takes ' || TO_CHAR(ROUND((SYSDATE - v_start_time) * 24 * 3600)) || ' second(s)';
      

  redis_cluster.CLOSE();

  RETURN v_plain_response;
  
EXCEPTION
  WHEN PKG_OREDIS.E_ASSERT THEN
    redis_cluster.CLOSE();
    RETURN SQLERRM;
  WHEN OTHERS THEN 
    redis_cluster.CLOSE();
    RAISE;
    RETURN 'Unknown Excption Occured! (' || SQLCODE || ') ' || SQLERRM;
    
END TEST_CLUSTER_SLAVE_READ;*/



FUNCTION EXEC_CMD_TEST RETURN VARCHAR2 IS
  redis OREDIS;
  v_response OREDIS_RESP;
  v_plain_response VARCHAR2(32767);
  v_cnt NUMBER;
  
BEGIN  
  
  redis := NEW OREDIS('10.3.11.34:6379,password=5693');
  
  v_response := redis.EXEC('PING');  
  
  v_plain_response := v_response.item(1).STR;
  
  
  v_response := redis.KEYS('*2017*');
  
  FOR i IN 1..v_response.ITEM_CNT LOOP
      v_plain_response := v_response.ITEM(i).STR;

      DBMS_OUTPUT.PUT_LINE(v_plain_response);
  END LOOP;
  
  redis.CLOSE;
    
  return v_plain_response;

EXCEPTION 
  WHEN OTHERS THEN 
    IF redis IS NOT NULL THEN
       redis.CLOSE();
    END IF;
    
    v_plain_response := SQLERRM || PKG_OREDIS.NEWLINE || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    DBMS_OUTPUT.PUT_LINE(v_plain_response);
    
    RETURN v_plain_response;
  
END EXEC_CMD_TEST;


FUNCTION EXEC_PERF_TEST RETURN VARCHAR2 IS
  redis OREDIS;
  v_response OREDIS_RESP;
  v_plain_response VARCHAR2(32767);
  v_responses OREDIS_RESP_TABLE;
  v_start_time DATE;
  
  v_key VARCHAR2(32767);
  v_int_val NUMBER;
  v_str_val VARCHAR2(32767);
  
  v_test_cnt NUMBER := 2;
  v_oredis_node OREDIS_NODE;
BEGIN
  
  
  redis := NEW OREDIS('10.3.11.34,password=5693');
  
    
  
  
  v_start_time := SYSDATE;
  
  FOR i IN 1..v_test_cnt LOOP
     v_int_val := redis.EXEC_ASYNC('SET a'||i || ' ' || (100000000 + i));
  END LOOP; 
    
  v_responses := redis.GET_ASYNC_RESPONSE();  
  
  --
  v_response := v_responses(1);
  v_plain_response := v_response.STR;
  --
   
  v_plain_response := v_plain_response || chr(10) || 'EXEC_ASYNC takes ' || TO_CHAR(ROUND((SYSDATE - v_start_time) * 24 * 3600)) || ' second(s)'; 
      
  
  
  v_start_time := SYSDATE;
  
  FOR i IN 1..v_test_cnt LOOP
     v_response := redis.EXEC('SET r'||i || ' ' || (100000000 + i));
  END LOOP; 
    
  v_plain_response := v_plain_response || chr(10) || 'REPEAT EXEC takes ' || TO_CHAR(ROUND((SYSDATE - v_start_time) * 24 * 3600)) || ' second(s)';
  
  
  
  

  redis.CLOSE();

  RETURN v_plain_response;
  
EXCEPTION
  WHEN OTHERS THEN 
    IF redis IS NOT NULL THEN
       redis.CLOSE();
    END IF;
    
    v_plain_response := SQLERRM || PKG_OREDIS.NEWLINE || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    DBMS_OUTPUT.PUT_LINE(v_plain_response);
    
    RETURN v_plain_response;
  
END EXEC_PERF_TEST;


FUNCTION EXEC_API_TEST RETURN VARCHAR2 IS
  redis OREDIS;
  v_response OREDIS_RESP;
  v_plain_response VARCHAR2(32767) := 'All Test Passed!';
  v_cnt NUMBER;
  
BEGIN  
  
  redis := NEW OREDIS('10.3.11.34:6379,password=5693');
  
  /* KEY */
  v_response := redis.SET_('hello', 'world');
  v_response := redis.GET('hello');
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, 'world');
  
  v_response := redis.EXIST('hello');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 1);
  v_response := redis.DEL('hello');
  v_response := redis.EXIST('hello');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 0);
  
  v_response := redis.SETEX('hello', 'world', 1);  
  v_response := redis.EXIST('hello');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 1);
  --v_response := redis.EXISTS_('hello');       -- SET BREAK POINT TO TEST
  --PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 0);
  
  
  /* HASH */
  v_response := redis.HSET('myhash', 'field1', 'hello');
  v_response := redis.HGET('myhash', 'field1');
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, 'hello');
  
  v_response := redis.HEXISTS('myhash', 'field1');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 1);
  
  v_response := redis.HDEL('myhash', 'field1');
  v_response := redis.HEXISTS('myhash', 'field1');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 0);
  
  
  /* LIST */
  v_response := redis.LPUSH('mylist', 'world');
  v_response := redis.LPUSH('mylist', 'hello');
  v_response := redis.LRANGE('mylist', 0, 1);
  PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(1).STR, 'hello');
  PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(2).STR, 'world');  
  
  v_response := redis.RPUSH('mylist', '!');
  v_response := redis.RPOP('mylist');
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, '!');
  
  
  /* SET */
  v_response := redis.SADD('myset', 'hello');
  v_response := redis.SADD('myset', 'world');
  v_response := redis.SMEMBERS('myset');
  PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(1).STR, 'hello');
  PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(2).STR, 'world');  
  v_response := redis.SREM('myset', 'hello');
  v_response := redis.SMEMBERS('myset');
  PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(1).STR, 'world');
  v_response := redis.SCARD('myset');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 1);
  
  
  /* SORTED SET */
  v_response := redis.ZADD('myzset', 1, 'one');
  v_response := redis.ZADD('myzset', 2, 'two');
  --v_response := redis.ZRANGE('myzset', 0, 1);
  v_response := redis.ZRANGEBYSCORE('myzset', '2', '2');
  PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(1).STR, 'two');
  --PKG_OREDIS.ASSERT_EQUAL(v_response.ITEM(2).STR, 'two'); 
  
  v_response := redis.ZCARD('myzset');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 2);
  
  v_response := redis.ZRANK('myzset', 'one');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 0);
  v_response := redis.ZRANK('myzset', 'two');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 1);
  
  v_response := redis.ZSCORE('myzset', 'two');
  PKG_OREDIS.ASSERT_EQUAL(v_response.STR, '2');
  
  v_response := redis.ZREM('myzset', 'one');
  v_response := redis.ZCARD('myzset');
  PKG_OREDIS.ASSERT_EQUAL(v_response.INT, 1);
  
  redis.CLOSE;
    
  return v_plain_response;

EXCEPTION 
  WHEN OTHERS THEN 
    IF redis IS NOT NULL THEN
       redis.CLOSE();
    END IF;
    
    v_plain_response := SQLERRM || PKG_OREDIS.NEWLINE || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    DBMS_OUTPUT.PUT_LINE(v_plain_response);
    
    RETURN v_plain_response;
  
END EXEC_API_TEST;

END PKG_OREDIS_TEST;
/

prompt
prompt Creating type body OREDIS
prompt =========================
prompt
CREATE OR REPLACE TYPE BODY OREDIS IS

/*
   OREDIS('127.0.0.1')
   OREDIS('127.0.0.1', 6380)
   OREDIS('127.0.0.1:6380')
   OREDIS('127.0.0.1, p_db => 1')
      ...
*/

CONSTRUCTOR FUNCTION OREDIS(SELF IN OUT NOCOPY OREDIS, 
                            p_config VARCHAR2
                            ) RETURN SELF AS RESULT IS     -- UTL_TCP.OPEN_CONNECTION => charset
  v_config_token Varchar2Table;  
  v_option_name  VARCHAR2(200);
  v_option_value VARCHAR2(200);  
  v_host_address VARCHAR2(200); 
  v_timeout PLS_INTEGER;
  v_password VARCHAR2(500);
  v_db VARCHAR2(10);
  v_in_buffer_size  PLS_INTEGER := NULL;
  v_out_buffer_size PLS_INTEGER := NULL;
BEGIN
  
  v_config_token := PKG_OREDIS.SPLIT(p_config, ',');
  
  FOR i IN 1..v_config_token.count LOOP
    IF INSTR(v_config_token(i), '=') > 0 THEN
      v_option_name  := UPPER(SUBSTR(v_config_token(i), 0, INSTR(v_config_token(i), '=') - 1));
      v_option_value := SUBSTR(v_config_token(i), INSTR(v_config_token(i), '=') + 1);
        
      IF v_option_name = 'TIMEOUT' THEN
        v_timeout := v_option_value;
      ELSIF v_option_name = 'PASSWORD' THEN
        v_password := v_option_value;
      ELSIF v_option_name = 'DB' THEN
        v_db := v_option_value;
      ELSIF v_option_name = 'INBUFFERSIZE' THEN
        v_in_buffer_size := v_option_value;
      ELSIF v_option_name = 'OUTBUFFERSIZE' THEN
        v_out_buffer_size := v_option_value;
      END IF;
    ELSE 
      v_host_address := v_config_token(i);
    END IF;
  END LOOP; 
  
  SELF.INITIALIZE(v_host_address, 6379, v_timeout, v_in_buffer_size, v_out_buffer_size, v_password);
  
  RETURN;

EXCEPTION  
  WHEN OTHERS THEN
    --DBMS_OUTPUT.PUT_LINE('CONSTRUCTOR OREDIS() FAILED!');
    RAISE;   

END OREDIS;


CONSTRUCTOR FUNCTION OREDIS(SELF IN OUT NOCOPY OREDIS, 
                            p_host VARCHAR2, 
                            p_port PLS_INTEGER, 
                            p_tx_timeout PLS_INTEGER DEFAULT NULL,      -- UTL_TCP.tx_timeout => tx_timeout
                            p_in_buffer_size PLS_INTEGER DEFAULT NULL,  -- UTL_TCP.in_buffer_size => tx_timeout
                            p_out_buffer_size PLS_INTEGER DEFAULT NULL,  -- UTL_TCP.out_buffer_size => tx_timeout
                            --p_charset VARCHAR2 DEFAULT NULL,
                            p_password VARCHAR2 DEFAULT NULL,
                            p_db PLS_INTEGER DEFAULT 0              -- Redis DB ID
                            ) RETURN SELF AS RESULT IS     
BEGIN
  
  SELF.INITIALIZE(p_host, p_port, p_tx_timeout, p_in_buffer_size, p_out_buffer_size, p_password, p_db);
  
  RETURN;

EXCEPTION  
  WHEN OTHERS THEN
    --DBMS_OUTPUT.PUT_LINE('CONSTRUCTOR OREDIS() FAILED!');
    RAISE;   

END OREDIS;


MEMBER PROCEDURE INITIALIZE(SELF IN OUT NOCOPY OREDIS, 
                              p_host VARCHAR2, 
                              p_port PLS_INTEGER, 
                              p_tx_timeout PLS_INTEGER DEFAULT NULL,
                              p_in_buffer_size PLS_INTEGER DEFAULT NULL,
                              p_out_buffer_size PLS_INTEGER DEFAULT NULL,
                              --p_charset VARCHAR2 DEFAULT NULL,
                              p_password VARCHAR2 DEFAULT NULL,
                              p_db PLS_INTEGER DEFAULT 0  ) IS
   v_response OREDIS_RESP; 
   v_tcpCon   UTL_TCP.CONNECTION;
BEGIN                  
              
  self.CONNECTION := PKG_OREDIS.OPEN_TCP(p_host, p_port, p_tx_timeout, p_in_buffer_size, p_out_buffer_size); --, p_charset);
  
  v_tcpCon := PKG_OREDIS.CONVERT_TCP_TYPE(self.connection);
  
  ASYNC_CMD_COUNT := 0;
  
  IF p_password IS NOT NULL THEN
    v_response := PKG_OREDIS.EXEC(v_tcpCon, 'AUTH ' || p_password);    
       
    IF v_response.STR <> PKG_OREDIS.OK THEN
      RAISE_APPLICATION_ERROR(-20203, v_response.STR || ', Your input : ' || p_password, TRUE); --PKG_OREDIS.E_AUTH_FAIL;
    END IF;       
  END IF;
    
  IF p_db > 0 THEN
     v_response := PKG_OREDIS.EXEC(v_tcpCon, 'SELECT ' || p_db);
       
     IF v_response.STR <> PKG_OREDIS.OK THEN
       RAISE_APPLICATION_ERROR(-20204, v_response.STR || ', Your input : ' || p_db, TRUE); --PKG_OREDIS.E_INVALID_CONFIG
     END IF;       
  END IF;
                                
EXCEPTION  
  WHEN UTL_TCP.NETWORK_ERROR THEN
    RAISE_APPLICATION_ERROR(-20205, 'Can''t access to the host , Your input : ' || p_host, TRUE);
  WHEN OTHERS THEN
    PKG_OREDIS.CLOSE_TCP(v_tcpCon);
    RAISE;   

END INITIALIZE;
                              

MEMBER PROCEDURE CLOSE IS
  v_tcpCon   UTL_TCP.CONNECTION;
BEGIN 
  v_tcpCon := PKG_OREDIS.CONVERT_TCP_TYPE(self.connection);
  PKG_OREDIS.CLOSE_TCP(v_tcpCon); 
END CLOSE;


MEMBER FUNCTION EXEC(self IN OUT NOCOPY OREDIS, p_cmd VARCHAR2) RETURN OREDIS_RESP IS
  v_response OREDIS_RESP;
  v_tcpCon   UTL_TCP.CONNECTION;
BEGIN 
  v_tcpCon := PKG_OREDIS.CONVERT_TCP_TYPE(self.connection);
  v_response := PKG_OREDIS.EXEC(v_tcpCon, p_cmd);
  RETURN v_response;
END EXEC;


/*
 Send command but do not wait response.
 You can get the response with GET_ASYNC_RESPONSE.
*/
MEMBER FUNCTION EXEC_ASYNC(self IN OUT NOCOPY OREDIS, p_cmd VARCHAR2) RETURN NUMBER IS
  v_tcpCon   UTL_TCP.CONNECTION;
BEGIN
  
  v_tcpCon := PKG_OREDIS.CONVERT_TCP_TYPE(self.connection);
  PKG_OREDIS.EXEC_ASYNC(v_tcpCon, p_cmd);
  
  ASYNC_CMD_COUNT := ASYNC_CMD_COUNT + 1;
  
  RETURN ASYNC_CMD_COUNT;
END EXEC_ASYNC;


/*
MEMBER FUNCTION EXEC_RAW(self IN OUT NOCOPY OREDIS, p_cmd VARCHAR2) RETURN VARCHAR2 IS
  v_tcpCon   UTL_TCP.CONNECTION;
  v_response VARCHAR2(32767);
BEGIN 
  v_tcpCon := PKG_OREDIS.CONVERT_TCP_TYPE(self.connection);  
  v_response := PKG_OREDIS.EXEC_RAW(v_tcpCon, p_cmd);
  RETURN v_response;
END EXEC_RAW;
*/


/* Get response after EXEC_ASYNC is executed. */
MEMBER FUNCTION GET_ASYNC_RESPONSE(self IN OUT NOCOPY OREDIS) RETURN OREDIS_RESP_TABLE IS
  v_tcpCon   UTL_TCP.CONNECTION;
  v_result VARCHAR2(32767);
  v_responses OREDIS_RESP_TABLE := OREDIS_RESP_TABLE();
BEGIN 
  
  v_tcpCon := PKG_OREDIS.CONVERT_TCP_TYPE(self.connection);
  
  UTL_TCP.FLUSH(v_tcpCon);
    
  FOR i IN 1..ASYNC_CMD_COUNT LOOP
    v_result :=  PKG_OREDIS.RECV_RESP(v_tcpCon); 
    v_responses.EXTEND;
    v_responses(v_responses.COUNT) := PKG_OREDIS.CREATE_RESPONSE(v_result);
  END LOOP;
  
  IF v_responses.COUNT <> ASYNC_CMD_COUNT THEN
    RAISE_APPLICATION_ERROR(-20209, 'Wrong Async response count! Command Count : '|| TO_CHAR(ASYNC_CMD_COUNT) || 
                                    ', Response Count : ' || TO_CHAR(v_responses.COUNT), TRUE);
  END IF;
  
  ASYNC_CMD_COUNT := 0;
  
  RETURN v_responses;
END GET_ASYNC_RESPONSE;


MEMBER FUNCTION GET_ADDRESS(self IN OUT NOCOPY OREDIS) RETURN VARCHAR2 IS
BEGIN
  RETURN SELF.CONNECTION.REMOTE_HOST || ':' || TO_CHAR(SELF.CONNECTION.REMOTE_PORT);
END GET_ADDRESS;



/*------------------  API START ---------------------------*/
/* KEY */
MEMBER FUNCTION SET_(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('SET ' || p_key || ' ' || p_value);
END SET_;

MEMBER FUNCTION PUT(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP IS  -- same with SET_()
BEGIN
  RETURN EXEC('SET ' || p_key || ' ' || p_value);
END PUT;

MEMBER FUNCTION SETEX(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_value VARCHAR2, p_expiry NUMBER) RETURN OREDIS_RESP  IS
BEGIN
  RETURN EXEC('SETEX ' || p_key || ' ' || TO_CHAR(p_expiry) || ' ' || p_value);
END SETEX;

MEMBER FUNCTION GET(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('GET ' || p_key);
END GET;

MEMBER FUNCTION DEL(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('DEL ' || p_key);
END DEL;

MEMBER FUNCTION EXIST(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('EXISTS ' || p_key);
END EXIST;


/*HASH*/
MEMBER FUNCTION HSET(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_field VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('HSET ' || p_key || ' ' || p_field || ' ' || p_value);
END HSET;

MEMBER FUNCTION HGET(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_field VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('HGET ' || p_key || ' ' || p_field);
END HGET;

MEMBER FUNCTION HDEL(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_field VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('HDEL ' || p_key || ' ' || p_field);
END HDEL;
                                  
MEMBER FUNCTION HEXISTS(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_field VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('HEXISTS ' || p_key || ' ' || p_field);
END HEXISTS;

/*LIST*/
MEMBER FUNCTION LPUSH(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('LPUSH ' || p_key || ' ' || p_value);
END LPUSH;

MEMBER FUNCTION LPOP(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('LPOP ' || p_key);
END LPOP;

MEMBER FUNCTION RPUSH(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('RPUSH ' || p_key || ' ' || p_value);
END RPUSH;

MEMBER FUNCTION RPOP(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('RPOP ' || p_key);
END RPOP;


MEMBER FUNCTION LRANGE(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_start NUMBER, p_end NUMBER) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('LRANGE ' || p_key || ' ' || TO_CHAR(p_start) || ' ' || TO_CHAR(p_end));
END LRANGE;


/*SET*/
MEMBER FUNCTION SADD(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('SADD ' || p_key || ' ' || p_member);
END SADD;

MEMBER FUNCTION SREM(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('SREM ' || p_key || ' ' || p_member);
END SREM;

MEMBER FUNCTION SMEMBERS(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('SMEMBERS ' || p_key);
END SMEMBERS;

MEMBER FUNCTION SCARD(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('SCARD ' || p_key);
END SCARD;
  
/*SORTED SET*/
MEMBER FUNCTION ZADD(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_score NUMBER, p_member VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('ZADD ' || p_key || ' ' || TO_CHAR(p_score) || ' ' || p_member);
END ZADD;

MEMBER FUNCTION ZREM(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('ZREM ' || p_key || ' ' || p_member);
END ZREM;

MEMBER FUNCTION ZCARD(self IN OUT NOCOPY OREDIS, p_key VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('ZCARD ' || p_key);
END ZCARD;

MEMBER FUNCTION ZRANGE(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_start NUMBER, p_end NUMBER) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('ZRANGE ' || p_key || ' ' || p_start || ' ' || p_end);
END ZRANGE;

MEMBER FUNCTION ZRANGEBYSCORE(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_start VARCHAR2, p_end VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('ZRANGEBYSCORE ' || p_key || ' ' || p_start || ' ' || p_end);
END ZRANGEBYSCORE;

MEMBER FUNCTION ZRANK(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('ZRANK ' || p_key || ' ' || p_member);
END ZRANK;

MEMBER FUNCTION ZSCORE(self IN OUT NOCOPY OREDIS, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('ZSCORE ' || p_key || ' ' || p_member);
END ZSCORE;

MEMBER FUNCTION KEYS(self IN OUT NOCOPY OREDIS, p_pattern VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('KEYS ' || p_pattern);
END KEYS;

/*------------------  API END ---------------------------*/


END;
/

prompt
prompt Creating type body OREDIS_CLUSTER
prompt =================================
prompt
CREATE OR REPLACE TYPE BODY OREDIS_CLUSTER IS

/* OREDIS_CLUSTER Constructor. p_config can includes some cluster node addresses and options. 
   ex) '10.3.10.xxx:6379, 10.3.11.xxx:6379'
   You don't need to list all the cluster nodes. Just one available node may be enough.
   OREDIS_CLUSTER will get all available nodes information from that node.
   But it is recommended to list two or more node addresses to prepare for node fail.  
*/
CONSTRUCTOR FUNCTION OREDIS_CLUSTER(SELF IN OUT NOCOPY OREDIS_CLUSTER, p_config VARCHAR2) RETURN SELF AS RESULT IS
BEGIN
 
  SELF.CONFIG := p_config;
  
  NODE_MAP := NEW OREDIS_CLUSTER_MAP(p_config);
  
  RETURN;

END OREDIS_CLUSTER;  


/* Close connetions to every cluster nodes */
MEMBER PROCEDURE CLOSE(SELF IN OUT NOCOPY OREDIS_CLUSTER) IS
  v_temp_master_node OREDIS_MASTER_NODE;  
  v_masterNodes OREDIS_MASTER_NODE_TABLE; 
  v_slave_cnt number := 0;
BEGIN
  v_masterNodes := SELF.NODE_MAP.MASTER_NODES;
 
  FOR i IN 1..v_masterNodes.COUNT LOOP
     v_temp_master_node := v_masterNodes(i);    
     
     v_temp_master_node.NODE.CLOSE;
    
     v_slave_cnt := v_temp_master_node.GET_SLAVE_COUNT();
        
     FOR j IN 1..v_slave_cnt LOOP
       v_temp_master_node.SLAVES(j).CLOSE;
     END LOOP;
  END LOOP;
      
END CLOSE;


/* Execute command synchronously. 
   When getting MOVED response, EXEC will refresh the inforamtion of clsuter map and execute the command again.
   When getting ASK response, EXEC will execue the command again on the node specified by ASK response with ASKING command.
*/
MEMBER FUNCTION EXEC(SELF IN OUT NOCOPY OREDIS_CLUSTER, p_cmd VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP IS
  v_node OREDIS_NODE;
  v_redirect_address VARCHAR2(20);
  v_response OREDIS_RESP;    
BEGIN
  
  IF ASYNC_CMD_COUNT > 0 THEN  -- IN ASYNC CMD MODE
    --RAISE PKG_OREDIS.E_IN_ASYNC_MODE;
    RAISE_APPLICATION_ERROR(-20206, 'It''s async mode, need to execute GET_ASYNC_RESPONSE() before calling EXEC()', TRUE);
  END IF;
        
  v_node := GET_NODE_BY_CMD(p_cmd, p_prefer_node_type);
          
  v_response := v_node.EXEC(p_cmd);
    
  IF REGEXP_SUBSTR(v_response.item(1).STR, '^MOVED') = 'MOVED' THEN    
    SELF.CLOSE();
    SELF.NODE_MAP := NEW OREDIS_CLUSTER_MAP(SELF.CONFIG);
    v_response := EXEC(p_cmd); -- When moved, excute on master node;
  END IF;
    
  IF REGEXP_SUBSTR(v_response.item(1).STR, '^ASK') = 'ASK' THEN
    v_redirect_address := TRIM(SUBSTRB(v_response.item(1).STR, INSTRB(v_response.item(1).STR, ' ', 1, 2) + 1));    
    v_node := NODE_MAP.GET_NODE_BY_ADDRESS(v_redirect_address);    
    v_response := v_node.EXEC('ASKING');
    v_response := v_node.EXEC(p_cmd);
  END IF;
    
  RETURN v_response;
  
EXCEPTION
  WHEN PKG_OREDIS.E_IN_ASYNC_MODE THEN
      RAISE;   
  WHEN PKG_OREDIS.E_NODE_NOT_FOUND THEN
      RAISE;   
  WHEN PKG_OREDIS.E_NOT_SUPPORTED_COMMAND THEN
      RAISE;  
  WHEN OTHERS THEN
    RAISE;   
END EXEC;


/*
 Send command but do not wait response.
 You can get the response with GET_ASYNC_RESPONSE.
*/
MEMBER FUNCTION EXEC_ASYNC(self IN OUT NOCOPY OREDIS_CLUSTER, p_cmd VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN NUMBER IS
  v_num_val  NUMBER;
  v_node OREDIS_NODE;
BEGIN
  
  v_node := GET_NODE_BY_CMD(p_cmd, p_prefer_node_type);
  
  v_num_val := v_node.EXEC_ASYNC(p_cmd);  
  
  ASYNC_CMD_COUNT := ASYNC_CMD_COUNT + 1;
  
  RETURN ASYNC_CMD_COUNT;
EXCEPTION
    WHEN PKG_OREDIS.E_NODE_NOT_FOUND THEN
      RAISE;   
  WHEN PKG_OREDIS.E_NOT_SUPPORTED_COMMAND THEN
    RAISE;    
  WHEN OTHERS THEN
    RAISE;   
END EXEC_ASYNC;


MEMBER FUNCTION GET_NODE_BY_CMD(self IN OUT NOCOPY OREDIS_CLUSTER, 
                                p_cmd VARCHAR2, 
                                p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_NODE IS
  v_node OREDIS_NODE;
  v_cmd_token Varchar2Table;
  v_cmd VARCHAR2(32767);
BEGIN
  
  v_cmd_token := PKG_OREDIS.SPLIT(p_cmd, ' ');    
   
  v_cmd := v_cmd_token(1);                        
                                                  
  IF PKG_OREDIS.CONTAINS(PKG_OREDIS.NOT_SUPPORTED_COMMAND_CLUSTER, UPPER(v_cmd)) THEN
    RAISE_APPLICATION_ERROR(-20202, 'Not supported command  : ' || p_cmd, TRUE);   --PKG_OREDIS.E_NOT_SUPPORTED_COMMAND;
  END IF;                                                
 
  IF v_cmd_token.COUNT > 1 THEN         
    v_node := GET_NODE_BY_KEY(v_cmd_token(2), p_prefer_node_type); 
  ELSE
    v_node := NODE_MAP.GET_NODE_ANY(p_prefer_node_type); 
  END IF;
   
  RETURN v_node;
  
END GET_NODE_BY_CMD;              
                                    

MEMBER FUNCTION GET_NODE_BY_KEY(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_NODE IS
  v_slot NUMBER;
  v_node OREDIS_NODE;
  v_hash_key VARCHAR2(32767);
BEGIN
  
  v_hash_key := GET_HASH_KEY(p_key);  
  v_slot := GET_HASH_SLOT(v_hash_key);
  v_node := NODE_MAP.GET_NODE_BY_SLOT(v_slot, p_prefer_node_type);
  
  RETURN v_node;
  
END GET_NODE_BY_KEY;


/*
MEMBER FUNCTION GET_NODE_BY_CMD(self IN OUT NOCOPY OREDIS_CLUSTER, p_cmd VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_NODE IS
  v_slot NUMBER;
  v_node OREDIS_NODE;
  v_hash_key VARCHAR2(32767);
  v_cmd_token Varchar2Table;
  v_cmd VARCHAR2(32767);
BEGIN
  
  v_cmd_token := PKG_OREDIS.SPLIT(p_cmd, ' ');    
   
  v_cmd := v_cmd_token(1);                        
                                                  
  IF PKG_OREDIS.CONTAINS(PKG_OREDIS.NOT_SUPPORTED_COMMAND_CLUSTER, UPPER(v_cmd)) THEN
    --RAISE PKG_OREDIS.E_NOT_SUPPORTED_COMMAND;
    RAISE_APPLICATION_ERROR(-20202, 'Not supported command  : ' || p_cmd, TRUE);
  END IF;                                                
 
  IF v_cmd_token.COUNT > 1 THEN                   
    v_hash_key := GET_HASH_KEY(v_cmd_token(2));  
    v_slot := HASH_SLOT(v_hash_key);
    v_node := NODE_MAP.GET_NODE_BY_SLOT(v_slot, p_prefer_node_type); 
  ELSE
    v_node := NODE_MAP.GET_NODE_ANY(p_prefer_node_type); 
  END IF;
   
  RETURN v_node;
  
END GET_NODE_BY_CMD;*/


/* Get responses from all cluster nodes after EXEC_ASYNC is executed.
   When use EXEC_ASYNC and GET_ASYNC_RESPONSE in cluster environment, MOVED and ASK response is not handled automaticaly.
   Application progam need to handle the MOVED and ASK response properly.
   I tried to handle it automatically but it getting too slow when massively excute commands.
*/
MEMBER FUNCTION GET_ASYNC_RESPONSE(self IN OUT NOCOPY OREDIS_CLUSTER) RETURN OREDIS_RESP_TABLE IS
  v_responses OREDIS_RESP_TABLE := OREDIS_RESP_TABLE();
  v_temp_responses OREDIS_RESP_TABLE;
  v_temp_response OREDIS_RESP;
  v_temp_master_node OREDIS_MASTER_NODE;  
  v_masterNodes OREDIS_MASTER_NODE_TABLE; 
  v_slave_cnt number := 0;
  v_has_moved_response BOOLEAN := FALSE;
  
BEGIN 
  
  v_masterNodes := SELF.NODE_MAP.MASTER_NODES;
 
  FOR i IN 1..v_masterNodes.COUNT LOOP
    v_temp_master_node := v_masterNodes(i);    
     
    v_temp_responses := v_temp_master_node.NODE.GET_ASYNC_RESPONSE();  
     
    FOR i IN 1..v_temp_responses.COUNT LOOP
      v_responses.EXTEND;
      v_temp_response := v_temp_responses(i);
       
      IF REGEXP_SUBSTR(v_temp_response.STR, '^MOVED') = 'MOVED' THEN
        v_has_moved_response := TRUE;
      END IF;
       
      v_responses(v_responses.COUNT) := v_temp_response;
    END LOOP;
    
    v_slave_cnt := v_temp_master_node.GET_SLAVE_COUNT();
        
    FOR j IN 1..v_slave_cnt LOOP
      v_temp_responses := v_temp_master_node.SLAVES(j).GET_ASYNC_RESPONSE();
       
      FOR k IN 1..v_temp_responses.COUNT LOOP
        v_responses.EXTEND;
        v_temp_response := v_temp_responses(k);
        
        IF REGEXP_SUBSTR(v_temp_response.STR, '^MOVED') = 'MOVED' THEN
          v_has_moved_response := TRUE;
        END IF;
      
        v_responses(v_responses.COUNT) := v_temp_response;
      END LOOP;
    END LOOP;
  END LOOP;    
  
  IF v_responses.COUNT <> ASYNC_CMD_COUNT THEN
    RAISE_APPLICATION_ERROR(-20209, 'Wrong Async response count! Command Count : '|| TO_CHAR(ASYNC_CMD_COUNT) || 
                                    ', Response Count : ' || TO_CHAR(v_responses.COUNT), TRUE);
  END IF;
  
  ASYNC_CMD_COUNT := 0;
  
  IF v_has_moved_response = TRUE THEN
    SELF.CLOSE();
    SELF.NODE_MAP := NEW OREDIS_CLUSTER_MAP(SELF.CONFIG);
  END IF;
        
  RETURN v_responses;
END GET_ASYNC_RESPONSE;


/* For supporting Hash Tags. Refer to https://redis.io/topics/cluster-spec */
MEMBER FUNCTION GET_HASH_KEY(p_params IN VARCHAR2) RETURN VARCHAR2 IS
  v_idx_first_space PLS_INTEGER;
  v_idx_first_left_parenth PLS_INTEGER;
  v_idx_first_right_parenth PLS_INTEGER;
  v_hash_key_len PLS_INTEGER;
BEGIN
  v_idx_first_space := INSTRB(p_params, ' ');
  v_idx_first_left_parenth := INSTRB(p_params, '{');
  v_idx_first_right_parenth := INSTRB(p_params, '}');
  
  IF v_idx_first_space = 0 THEN
    v_hash_key_len := LENGTHB(p_params);
  ELSE
    v_hash_key_len := v_idx_first_space - 1;
  END IF;
    
  IF v_idx_first_left_parenth = 0 OR v_idx_first_right_parenth = 0 OR (v_idx_first_right_parenth - v_idx_first_left_parenth) = 1 THEN
    RETURN SUBSTRB(p_params, 1, v_hash_key_len);
  END IF;  
  
  RETURN SUBSTRB(p_params, v_idx_first_left_parenth + 1, (v_idx_first_right_parenth - v_idx_first_left_parenth) - 1);

END GET_HASH_KEY;


/* Calculate Hash Slot by key */
MEMBER FUNCTION GET_HASH_SLOT(KEY VARCHAR2) RETURN NUMBER IS
  v_len NUMBER;
  v_len_pos NUMBER;
  v_len_delim VARCHAR2(4) := 'Len=';
  v_byte_data_delim VARCHAR2(1) := ':';
  v_byte_data_pos NUMBER;
  v_byte_data VARCHAR2(32767);
  v_key_dump VARCHAR2(32767);
  v_byte_table Varchar2Table;
  v_byte NUMBER;
  v_crc  NUMBER := 0;
BEGIN
    
  IF KEY IS NULL THEN
    RETURN -1;
  END IF;

  SELECT DUMP(KEY) INTO v_key_dump FROM DUAL;
  
  v_len_pos := INSTRB(v_key_dump, v_len_delim);
  v_byte_data_pos := INSTRB(v_key_dump, v_byte_data_delim);
  
  v_len := SUBSTRB(v_key_dump, v_len_pos + LENGTHB(v_len_delim), v_byte_data_pos - v_len_pos - LENGTHB(v_len_delim));
  v_byte_data := SUBSTRB(v_key_dump, v_byte_data_pos + 1);
  
  v_byte_table := PKG_OREDIS.SPLIT(v_byte_data, ',');
    
  FOR i IN 1..v_len LOOP
    v_byte := TO_NUMBER(v_byte_table(i));    
    v_crc := BITAND(BITXOR((v_crc * 256), PKG_OREDIS.CRC16TAB(BITAND(BITXOR(v_crc/256, v_byte), 255)+1)), 65535);                 
  END LOOP;  
  
  RETURN  MOD(v_crc, PKG_OREDIS.REDIS_CLUSTER_SLOT_CNT);
  
END GET_HASH_SLOT;


/* BIT OR operation for CRC calculation*/
MEMBER FUNCTION BITOR(exp1 IN NUMBER, exp2 IN NUMBER) RETURN NUMBER IS
BEGIN
  RETURN  exp1 + exp2 - BITAND(exp1, exp2);
END BITOR;


/* BIT XOR operation for CRC calculation */
MEMBER FUNCTION BITXOR(exp1 IN NUMBER, exp2 IN NUMBER) RETURN NUMBER IS
BEGIN
  RETURN BITOR(exp1, exp2) - BITAND(exp1, exp2);   
END BITXOR;


/*------------------  API START ---------------------------*/
/* KEY */
MEMBER FUNCTION SET_(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('SET ' || p_key || ' ' || p_value);
END SET_;

MEMBER FUNCTION PUT(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('SET ' || p_key || ' ' || p_value);
END PUT;

MEMBER FUNCTION SETEX(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_value VARCHAR2, p_expiry NUMBER) RETURN OREDIS_RESP  IS
BEGIN
  RETURN EXEC('SETEX ' || p_key || ' ' || TO_CHAR(p_expiry) || ' ' || p_value);
END SETEX;

MEMBER FUNCTION GET(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('GET ' || p_key, p_prefer_node_type);
END GET;

MEMBER FUNCTION DEL(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('DEL ' || p_key);
END DEL;

MEMBER FUNCTION EXIST(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('EXISTS ' || p_key, p_prefer_node_type);
END EXIST;


/*HASH*/
MEMBER FUNCTION HSET(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_field VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('HSET ' || p_key || ' ' || p_field || ' ' || p_value);
END HSET;

MEMBER FUNCTION HGET(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_field VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('HGET ' || p_key || ' ' || p_field, p_prefer_node_type);
END HGET;

MEMBER FUNCTION HDEL(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_field VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('HDEL ' || p_key || ' ' || p_field);
END HDEL;
                                  
MEMBER FUNCTION HEXISTS(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_field VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('HEXISTS ' || p_key || ' ' || p_field, p_prefer_node_type);
END HEXISTS;

/*LIST*/
MEMBER FUNCTION LPUSH(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('LPUSH ' || p_key || ' ' || p_value);
END LPUSH;

MEMBER FUNCTION LPOP(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('LPOP ' || p_key);
END LPOP;

MEMBER FUNCTION RPUSH(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_value VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('RPUSH ' || p_key || ' ' || p_value);
END RPUSH;

MEMBER FUNCTION RPOP(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('RPOP ' || p_key);
END RPOP;


MEMBER FUNCTION LRANGE(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_start NUMBER, p_end NUMBER, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('LRANGE ' || p_key || ' ' || TO_CHAR(p_start) || ' ' || TO_CHAR(p_end), p_prefer_node_type);
END LRANGE;


/*SET*/
MEMBER FUNCTION SADD(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('SADD ' || p_key || ' ' || p_member);
END SADD;

MEMBER FUNCTION SREM(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('SREM ' || p_key || ' ' || p_member);
END SREM;

MEMBER FUNCTION SMEMBERS(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('SMEMBERS ' || p_key, p_prefer_node_type);
END SMEMBERS;

MEMBER FUNCTION SCARD(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('SCARD ' || p_key, p_prefer_node_type);
END SCARD;
  
/*SORTED SET*/
MEMBER FUNCTION ZADD(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_score NUMBER, p_member VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('ZADD ' || p_key || ' ' || TO_CHAR(p_score) || ' ' || p_member);
END ZADD;

MEMBER FUNCTION ZREM(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_member VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('ZREM ' || p_key || ' ' || p_member);
END ZREM;

MEMBER FUNCTION ZCARD(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('ZCARD ' || p_key, p_prefer_node_type);
END ZCARD;

MEMBER FUNCTION ZRANGE(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_start NUMBER, p_end NUMBER, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('ZRANGE ' || p_key || ' ' || p_start || ' ' || p_end, p_prefer_node_type);
END ZRANGE;

MEMBER FUNCTION ZRANGEBYSCORE(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_start VARCHAR2, p_end VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('ZRANGEBYSCORE ' || p_key || ' ' || p_start || ' ' || p_end, p_prefer_node_type);
END ZRANGEBYSCORE;

MEMBER FUNCTION ZRANK(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_member VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('ZRANK ' || p_key || ' ' || p_member, p_prefer_node_type);
END ZRANK;

MEMBER FUNCTION ZSCORE(self IN OUT NOCOPY OREDIS_CLUSTER, p_key VARCHAR2, p_member VARCHAR2, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('ZSCORE ' || p_key || ' ' || p_member, p_prefer_node_type);
END ZSCORE;

MEMBER FUNCTION KEYS(self IN OUT NOCOPY OREDIS_CLUSTER, p_pattern VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN EXEC('KEYS ' || p_pattern);
END KEYS;

/*------------------  API END ---------------------------*/

END;
/

prompt
prompt Creating type body OREDIS_CLUSTER_MAP
prompt =====================================
prompt
CREATE OR REPLACE TYPE BODY OREDIS_CLUSTER_MAP  IS

CONSTRUCTOR FUNCTION OREDIS_CLUSTER_MAP(SELF IN OUT NOCOPY OREDIS_CLUSTER_MAP, p_config VARCHAR2) RETURN SELF AS RESULT IS
  v_config_token Varchar2Table;
  v_cluster_node_arr Varchar2Table := Varchar2Table();
  v_option_arr Varchar2Table := Varchar2Table();
  ---------------------------------------
  v_option_name  VARCHAR2(200);
  v_option_value VARCHAR2(500);
  
  v_timeout PLS_INTEGER;
  v_password VARCHAR2(500);
  v_slave_read VARCHAR2(10);
  v_in_buffer_size  PLS_INTEGER := NULL;
  v_out_buffer_size PLS_INTEGER := NULL;
  -------------------------------------  
  v_nodes_info VARCHAR2(32767);    
  v_nodes      Varchar2Table;
  v_node_info VARCHAR2(500);
  v_node_token Varchar2Table;
  v_slot_info  Varchar2Table;
  
  v_temp_node OREDIS_NODE;
  v_temp_master_node OREDIS_MASTER_NODE;  
   
  v_master_node_id VARCHAR2(100);
  v_live_master_node_cnt PLS_INTEGER := 0; 
  
  v_resp OREDIS_RESP;
BEGIN
  v_config_token := PKG_OREDIS.SPLIT(p_config, ',');
  
  FOR i IN 1..v_config_token.count LOOP
    IF INSTR(v_config_token(i), '=') > 0 THEN
      v_option_arr.EXTEND;
      v_option_arr(v_option_arr.COUNT) := v_config_token(i);
    ELSE
      v_cluster_node_arr.EXTEND;
      v_cluster_node_arr(v_cluster_node_arr.COUNT) := v_config_token(i);
    END IF;
  END LOOP; 
    
  FOR i IN 1..v_option_arr.count LOOP
    v_option_name  := UPPER(SUBSTR(v_option_arr(i), 0, INSTR(v_option_arr(i), '=') - 1));
    v_option_value := SUBSTR(v_option_arr(i), INSTR(v_option_arr(i), '=') + 1);
    
    IF v_option_name = 'TIMEOUT' THEN
      v_timeout := v_option_value;    
    ELSIF v_option_name = 'INBUFFERSIZE' THEN
      v_in_buffer_size := v_option_value;
    ELSIF v_option_name = 'OUTBUFFERSIZE' THEN
      v_out_buffer_size := v_option_value;
    ELSIF v_option_name = 'PASSWORD' THEN
      v_password := v_option_value;    
    ELSIF v_option_name = 'READSLAVE' THEN
      v_slave_read := UPPER(v_option_value);
    END IF;
  END LOOP;
  
      
  v_nodes_info := GET_CLUSTER_INFO(v_cluster_node_arr, v_password);  
  v_nodes := PKG_OREDIS.SPLIT(v_nodes_info, CHR(10));
  
  SELF.MASTER_NODES := NEW OREDIS_MASTER_NODE_TABLE();
  
  FOR i IN 1..v_nodes.count - 1 LOOP
    v_node_info := v_nodes(i);
    v_node_token := PKG_OREDIS.SPLIT(v_node_info, ' ');
    
    IF INSTRB(UPPER(v_node_token(3)), 'MASTER') > 0 AND INSTRB(UPPER(v_node_token(3)), 'FAIL') = 0 THEN
      v_slot_info := PKG_OREDIS.SPLIT(v_node_token(9), '-');
      v_temp_node := NEW OREDIS_NODE(v_node_token(1), 
                                     NEW OREDIS(p_host => v_node_token(2), 
                                                p_port => NULL,
                                                p_tx_timeout => v_timeout, 
                                                p_in_buffer_size => v_in_buffer_size, 
                                                p_out_buffer_size => v_out_buffer_size, 
                                                p_password => v_password),
                                     PKG_OREDIS.MASTER_NODE_TYPE, 
                                     TO_NUMBER(v_slot_info(1)), 
                                     TO_NUMBER(v_slot_info(2)));
                                     
          
      v_temp_master_node := new OREDIS_MASTER_NODE(v_temp_node.NODEID,
                                                   v_temp_node,                                                   
                                                   v_temp_node.SLOT_FROM,
                                                   v_temp_node.SLOT_TO);
     
      SELF.MASTER_NODES.EXTEND;      
      v_live_master_node_cnt := v_live_master_node_cnt + 1;                                        
      SELF.MASTER_NODES(v_live_master_node_cnt) := v_temp_master_node;     
    END IF;
  END LOOP;
  
  FOR i IN 1..v_nodes.count - 1 LOOP
    v_node_info := v_nodes(i);
    v_node_token := PKG_OREDIS.SPLIT(v_node_info, ' ');
    
    IF INSTRB(UPPER(v_node_token(3)), 'SLAVE') > 0 AND INSTRB(UPPER(v_node_token(3)), 'FAIL') = 0 THEN
             
      v_temp_node := NEW OREDIS_NODE(v_node_token(1), 
                                     NEW OREDIS(p_host => v_node_token(2), 
                                                p_PORT => NULL,
                                                p_tx_timeout => v_timeout, 
                                                p_in_buffer_size => v_in_buffer_size, 
                                                p_out_buffer_size => v_out_buffer_size,  
                                                p_password => v_password),
                                     PKG_OREDIS.SLAVE_NODE_TYPE, 
                                     v_temp_node.SLOT_FROM, 
                                     v_temp_node.SLOT_TO);
                                  
      IF v_slave_read = 'TRUE' THEN   
        v_resp := v_temp_node.EXEC('READONLY');
      END IF;
      
      v_master_node_id := v_node_token(4);    -- Master Nodeid
      
      FOR j IN 1..SELF.MASTER_NODES.COUNT LOOP
        IF SELF.MASTER_NODES(j).NODEID =  v_master_node_id THEN
          SELF.MASTER_NODES(j).ADD_SLAVE(v_temp_node);
        END IF;
      END LOOP;                         
            
    END IF;
  END LOOP;
  
  RETURN;

END OREDIS_CLUSTER_MAP;  


MEMBER FUNCTION GET_CLUSTER_INFO(p_endpoint_arr Varchar2Table, p_password VARCHAR2 DEFAULT NULL) RETURN VARCHAR2 IS
  v_redis OREDIS;
  v_resp  OREDIS_RESP;
  v_nodes_info VARCHAR2(32767);
BEGIN
    
  FOR i IN 1..p_endpoint_arr.count LOOP
    BEGIN
      v_redis := NEW OREDIS(p_endpoint_arr(i));
      
      IF p_password IS NOT NULL THEN
        v_resp := v_redis.EXEC('AUTH ' || p_password);
       
        IF v_resp.STR <> PKG_OREDIS.OK THEN
          RAISE_APPLICATION_ERROR(-20203, v_resp.STR || ', Your input : ' || p_password, TRUE); --PKG_OREDIS.E_AUTH_FAIL
        END IF;       
      END IF;
      
      v_resp := v_redis.exec('CLUSTER NODES');
      
      IF v_resp.TYPE = PKG_OREDIS.REPLY_ERROR THEN
          RAISE_APPLICATION_ERROR(-20210, 'Error : ' || v_resp.STR , TRUE); --PKG_OREDIS.E_GENERAL_EXCEPTION
      END IF; 
            
      v_redis.CLOSE();
            
      RETURN v_resp.STR;
    EXCEPTION
      WHEN PKG_OREDIS.E_CONNECTION THEN
        NULL;
      WHEN OTHERS THEN
        v_redis.CLOSE();
        RAISE;
    END;   
  END LOOP;  
  
  FOR i IN 1..p_endpoint_arr.COUNT LOOP
    v_nodes_info := v_nodes_info || p_endpoint_arr(i) || ', ';
  END LOOP;
  
  RAISE_APPLICATION_ERROR(-20205, 'Can''t find any avaliable cluster node , Your input : ' || v_nodes_info, TRUE); --PKG_OREDIS.E_CONNECTION
      
END GET_CLUSTER_INFO;


MEMBER FUNCTION GET_NODE_ANY(SELF IN OUT NOCOPY OREDIS_CLUSTER_MAP, 
                             p_prefer_node_type VARCHAR2 DEFAULT 'M', 
                             p_master_node OREDIS_MASTER_NODE DEFAULT NULL) RETURN OREDIS_NODE IS
  v_masterNodes OREDIS_MASTER_NODE_TABLE;  
  v_temp_node OREDIS_NODE;
  v_temp_master_node OREDIS_MASTER_NODE;
  v_random_val NUMBER;
  v_slave_cnt NUMBER := 0;
BEGIN
  v_masterNodes := SELF.MASTER_NODES;
  
  IF p_master_node IS NULL THEN
    v_random_val := ROUND(DBMS_RANDOM.VALUE(1, v_masterNodes.COUNT));    
    v_temp_master_node := v_masterNodes(v_random_val);
  ELSE
    v_temp_master_node := p_master_node;
  END IF;
  
  IF p_prefer_node_type = PKG_OREDIS.MASTER_NODE_TYPE THEN  
    v_temp_node := v_temp_master_node.NODE;    
    RETURN v_temp_node;
  ELSE
    v_slave_cnt := v_temp_master_node.GET_SLAVE_COUNT(); 
    
    IF v_slave_cnt = 0 THEN
      v_temp_node := v_temp_master_node.NODE;  
      RETURN v_temp_node;  -- RETURN MASTER NODE
    END IF;
    
    v_random_val := ROUND(DBMS_RANDOM.VALUE(1, v_slave_cnt));   
    v_temp_node := v_temp_master_node.SLAVES(v_random_val);  
    
    RETURN v_temp_node;  
  END IF;
  
  RAISE_APPLICATION_ERROR(-20207, 'Can''t find any avaliable node', TRUE);
    
  --RETURN NULL;
  
END GET_NODE_ANY;


MEMBER FUNCTION GET_NODE_BY_SLOT(SELF IN OUT NOCOPY OREDIS_CLUSTER_MAP, p_slot NUMBER, p_prefer_node_type VARCHAR2 DEFAULT 'M') RETURN OREDIS_NODE IS
  v_masterNodes OREDIS_MASTER_NODE_TABLE;  
  v_temp_node OREDIS_NODE;
  v_temp_master_node OREDIS_MASTER_NODE;
BEGIN  
  
  v_masterNodes := SELF.MASTER_NODES;
 
  FOR i IN 1..v_masterNodes.COUNT LOOP
    
    v_temp_master_node := v_masterNodes(i);    
     
    IF p_slot BETWEEN v_temp_master_node.SLOT_FROM AND v_temp_master_node.SLOT_TO THEN
      
      v_temp_node := v_temp_master_node.NODE;
      
      IF p_prefer_node_type = PKG_OREDIS.MASTER_NODE_TYPE THEN
        return v_temp_node;
      ELSE
        v_temp_node := GET_NODE_ANY(p_prefer_node_type, v_temp_master_node);
        RETURN v_temp_node;
      END IF;
    END IF;
  END LOOP;
      
  --RAISE PKG_OREDIS.E_NODE_NOT_FOUND;
  RAISE_APPLICATION_ERROR(-20207, 'Can''t find the node by SLOT! SLOT NO : ' ||  p_slot, TRUE);
  
  RETURN NULL;
  
END GET_NODE_BY_SLOT;


MEMBER FUNCTION GET_NODE_BY_ID(SELF IN OUT NOCOPY OREDIS_CLUSTER_MAP, p_nodeid VARCHAR2) RETURN OREDIS_NODE IS
  v_masterNodes OREDIS_MASTER_NODE_TABLE;  
  v_temp_node OREDIS_NODE;
  v_temp_master_node OREDIS_MASTER_NODE;
  v_slave_cnt NUMBER := 0;
BEGIN
  v_masterNodes := SELF.MASTER_NODES;
 
  FOR i IN 1..v_masterNodes.COUNT LOOP
    
    v_temp_master_node := v_masterNodes(i);
    v_temp_node := v_temp_master_node.NODE;    
    
    IF v_temp_node.NODEID = p_nodeid THEN
      RETURN v_temp_node;
    END IF;
    
    v_slave_cnt := v_temp_master_node.GET_SLAVE_COUNT(); 
       
    FOR j in 1..v_slave_cnt LOOP
      IF v_temp_master_node.SLAVES(j).NODEID = p_nodeid THEN
       RETURN v_temp_master_node.SLAVES(j);
      END IF;
    END LOOP; 
    
  END LOOP;
      
  --RAISE PKG_OREDIS.E_NODE_NOT_FOUND;
  RAISE_APPLICATION_ERROR(-20207, 'Can''t find the node by Node ID! Node ID : ' ||  p_nodeid, TRUE);
  
  RETURN NULL;
END GET_NODE_BY_ID;


MEMBER FUNCTION GET_NODE_BY_ADDRESS(SELF IN OUT NOCOPY OREDIS_CLUSTER_MAP, p_address VARCHAR2) RETURN OREDIS_NODE IS
  v_masterNodes OREDIS_MASTER_NODE_TABLE;  
  v_temp_node OREDIS_NODE;
  v_temp_master_node OREDIS_MASTER_NODE;
  v_slave_cnt NUMBER := 0;
BEGIN  
  
  v_masterNodes := SELF.MASTER_NODES;
 
  FOR i IN 1..v_masterNodes.COUNT LOOP
    
    v_temp_master_node := v_masterNodes(i);
    v_temp_node := v_temp_master_node.NODE;    
    
    IF v_temp_node.GET_ADDRESS() = p_address THEN
      RETURN v_temp_node;
    END IF;
    
    v_slave_cnt := v_temp_master_node.GET_SLAVE_COUNT(); 
    
    FOR j in 1..v_slave_cnt LOOP
      v_temp_node := v_temp_master_node.SLAVES(j);
            
       IF v_temp_node.GET_ADDRESS() = p_address THEN
         return v_temp_node;
       END IF;
    END LOOP;
    
  END LOOP;
    
  --RAISE PKG_OREDIS.E_NODE_NOT_FOUND;
  RAISE_APPLICATION_ERROR(-20207, 'Can''t find the node by Node ID! Address : ' ||  p_address, TRUE);
  
  RETURN NULL;
  
END GET_NODE_BY_ADDRESS;
  
END;
/

prompt
prompt Creating type body OREDIS_MASTER_NODE
prompt =====================================
prompt
CREATE OR REPLACE TYPE BODY OREDIS_MASTER_NODE  IS
  

CONSTRUCTOR FUNCTION OREDIS_MASTER_NODE(SELF IN OUT NOCOPY OREDIS_MASTER_NODE,
                                        p_nodeid      VARCHAR2,
                                        p_master_node OREDIS_NODE,
                                        p_slot_from   NUMBER,
                                        p_slot_to     NUMBER) RETURN SELF AS RESULT IS
BEGIN
  SELF.NODEID := p_nodeid;
  SELF.NODE:= p_master_node;
  --SELF.SLAVES := p_slaves;
  SELF.SLOT_FROM := p_slot_from;
  SELF.SLOT_TO := p_slot_to;

  RETURN;

END OREDIS_MASTER_NODE;


MEMBER PROCEDURE CLOSE(SELF IN OUT NOCOPY OREDIS_MASTER_NODE) IS
  v_slave_cnt PLS_INTEGER;
BEGIN 
  NULL;
END CLOSE;


MEMBER PROCEDURE ADD_SLAVE(SELF IN OUT NOCOPY OREDIS_MASTER_NODE, 
                          p_slave_node OREDIS_NODE) IS
  v_slave_cnt PLS_INTEGER;  
BEGIN
  IF SELF.SLAVES IS NULL THEN
    SELF.SLAVES  := NEW OREDIS_NODE_TABLE();
  END IF;   
  
  v_slave_cnt := SELF.SLAVES.COUNT;
  
  FOR i IN 1..v_slave_cnt LOOP
    IF SELF.SLAVES(i).NODEID = p_slave_node.NODEID THEN
      RETURN;
    END IF;
  END LOOP;  
  
  SELF.SLAVES.EXTEND;
  SELF.SLAVES(v_slave_cnt + 1) := p_slave_node;
  
END ADD_SLAVE;

MEMBER FUNCTION GET_SLAVE_COUNT(SELF IN OUT NOCOPY OREDIS_MASTER_NODE) RETURN NUMBER IS
BEGIN
  IF SELF.SLAVES IS NULL THEN
    RETURN 0;
  END IF;
  
  RETURN SELF.SLAVES.COUNT;
END GET_SLAVE_COUNT;
  
END;
/

prompt
prompt Creating type body OREDIS_NODE
prompt ==============================
prompt
create or replace type body OREDIS_NODE IS
  
CONSTRUCTOR FUNCTION OREDIS_NODE(SELF IN OUT NOCOPY OREDIS_NODE, 
                                     p_nodeid VARCHAR2,  
                                     p_oredis  OREDIS,
                                     p_type VARCHAR2,                                     
                                     p_slot_from NUMBER,
                                     p_slot_to   NUMBER) RETURN SELF AS RESULT IS
BEGIN
  SELF.NODEID := p_nodeid;
  SELF.AGENT := p_oredis;
  SELF.TYPE:= p_type;
  SELF.SLOT_FROM := p_slot_from;
  SELF.SLOT_TO := p_slot_to;

  RETURN;

END OREDIS_NODE;


MEMBER PROCEDURE CLOSE(SELF IN OUT NOCOPY OREDIS_NODE) IS
BEGIN 
  SELF.AGENT.CLOSE();
END CLOSE;


MEMBER FUNCTION EXEC(self IN OUT NOCOPY OREDIS_NODE, p_cmd VARCHAR2) RETURN OREDIS_RESP IS
BEGIN
  RETURN SELF.AGENT.EXEC(p_cmd);
END EXEC;

  
MEMBER FUNCTION EXEC_ASYNC(self IN OUT NOCOPY OREDIS_NODE, p_cmd VARCHAR2) RETURN NUMBER IS
BEGIN
  RETURN SELF.AGENT.EXEC_ASYNC(p_cmd);
END EXEC_ASYNC;


MEMBER FUNCTION GET_ASYNC_RESPONSE(self IN OUT NOCOPY OREDIS_NODE) RETURN OREDIS_RESP_TABLE IS
BEGIN
  RETURN SELF.AGENT.GET_ASYNC_RESPONSE();
END GET_ASYNC_RESPONSE;


MEMBER FUNCTION GET_ADDRESS(self IN OUT NOCOPY OREDIS_NODE) RETURN VARCHAR2 IS
BEGIN
  RETURN SELF.AGENT.GET_ADDRESS();
END GET_ADDRESS;
  
end;
/


spool off
