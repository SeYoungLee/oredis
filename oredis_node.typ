create or replace type OREDIS_NODE FORCE AS OBJECT
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

