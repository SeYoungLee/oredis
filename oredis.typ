CREATE OR REPLACE TYPE OREDIS FORCE AS OBJECT
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

