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

CREATE OR REPLACE TYPE BODY OREDIS_CLUSTER  IS

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

