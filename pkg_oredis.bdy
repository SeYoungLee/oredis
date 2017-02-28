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
 
  v_redis_response OREDIS_RESP;
  
  v_idx NUMBER;  
  v_item_cnt NUMBER;
  
  v_prev_item_idx NUMBER;
  v_cur_item_idx NUMBER := 1;
  
  v_resp_item VARCHAR2(32767);
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
    
    v_idx := INSTRB(p_resp_str, NEWLINE);
    v_item_cnt := TO_NUMBER(SUBSTRB(p_resp_str, 2, v_idx - 2));
            
    v_redis_response := NEW OREDIS_RESP(NULL, v_item_cnt, OREDIS_RESP_ITEM_TABLE(), NULL, NULL);  
    v_redis_response.TYPE := REPLY_ARRAY;  
    
    v_prev_item_idx := REGEXP_INSTR(p_resp_str, '[+:$]'); 
    
    FOR i IN 1..v_item_cnt LOOP
      
      v_cur_item_idx := REGEXP_INSTR(p_resp_str, '[+:$]', v_prev_item_idx + 1); 
      
      IF v_cur_item_idx <> 0 THEN
        v_resp_item := SUBSTRB(p_resp_str, v_prev_item_idx, v_cur_item_idx - v_prev_item_idx);   -- if v_response > 32767
        v_redis_response.item.EXTEND;
        v_redis_response.item(i) := CREATE_RESPONSE_ITEM(v_resp_item);
        v_prev_item_idx := v_cur_item_idx;
      ELSE
        v_resp_item := SUBSTRB(p_resp_str, v_prev_item_idx);
        v_redis_response.item.EXTEND;
        v_redis_response.item(i) := CREATE_RESPONSE_ITEM(v_resp_item);
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

