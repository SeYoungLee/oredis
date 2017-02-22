CREATE OR REPLACE TYPE OREDIS_CLUSTER_MAP FORCE AS OBJECT (
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

