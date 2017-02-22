CREATE OR REPLACE TYPE OREDIS_TCP_CONNECTION FORCE AS OBJECT
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

