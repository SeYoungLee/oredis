CREATE OR REPLACE TYPE OREDIS_MASTER_NODE force IS OBJECT (
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

