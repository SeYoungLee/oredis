#OREDIS
OREDIS is a Redis client library for Oracle PL/SQL.<br>
OREDIS is written in OOP style by using PL/SQL Object type.<br>
(I think PL/SQL is not enough to be called true OOP.)
<br><br>


#Features
##Support
* Redis Cluster
* Asynchronous command execution (Instead of Pipelining)


##Not support
* Sentinel
* Pipelining
* Pubish/Subscribe
* Lua Scripting

##Not supported commands
BGREWRITEAOF|BGSAVE|CLIENT|CONFIG|DEBUG
---|---|---|---|--- 
DUMP|EVAL|EVALSHA|KEYS|LASTSAVE
MIGRATE|MONITOR|MOVE|OBJECT|PSUBSCRIBE
PUBSUB|PUBLISH|PUNSUBSCRIBE|QUIT|SAVE
SUBSCRIBE|SCRIPT|SHUTDOWN|SLAVEOF|SYNC 
UNSUBSCRIBE|WAIT


##Not supported commands in cluster environment
BITOP | DBSIZE | DISCARD | EXEC | FLUSHALL
---|---|---|---|--- 
FLUSHDB|INFO|MULTI|PING|RANDOMKEY
ROLE|SWAPDB|SELECT|SLOWLOG|WATCH
UNWATCH|SCAN|SSCAN|HSCAN|ZSCAN


<br>
#Requirements
* Oracle 10g or higher
* Privilege to SYS.UTL_TCP package<br>
  Login as sys user and execute below<br>
`sql>grant execute on UTL_TCP to username`
<br>
* Network Privilage when use oracle 11g or higer 
 (Refer to [Link1](https://docs.oracle.com/cd/B28359_01/appdev.111/b28419/d_networkacl_adm.htm#BABCJDGC) 
 [Link2](http://www.dba-oracle.com/t_11g_new_acls_plsql.htm))<br>
example:
```PL/SQL
BEGIN
  DBMS_NETWORK_ACL_ADMIN.CREATE_ACL (
                                      acl => 'acl_xml_file_name',
                                      description => 'Permissions for network',
                                      principal => 'username',
                                      is_grant => TRUE,
                                      privilege => 'connect');

  DBMS_NETWORK_ACL_ADMIN.ASSIGN_ACL (
                                      acl => 'acl_xml_file_name',
                                      host => '*'); 
END;
```

<br>
#Install and Update 
###Oracle 11g :
1. Download all files 
2. Login to oracle as redis client user
3. Execute 'install_oredis.sql'
4. When update, download files and just execute 'install_oredis.sql'
```
sql>@install_oredis.sql
```

###Oracle 10g :
1. Download all files 
2. Login to oracle as redis client user
3. Execute 'install_oredis_10g.sql'
4. When update, drop all the oredis packages and types and execute 'install_oredis_10g.sql'

```
sql>@install_oredis_10g.sql
```


<br>
#Simple Usage Example
There are two ways to send commands to redis.
* Use APIs (set_, get...)
* Send command as in redis-cli by using exec() or exec_async()

In any case, don't forget to call close() at the end. 

```pl/sql
DECLARE
  redis OREDIS;
  v_response OREDIS_RESP;
  v_result VARCHAR2(32767);
BEGIN

  redis := new OREDIS('127.0.0.1:6379');

  v_response := redis.set_('hello', 'world');
  --v_response := redis.put('hello', 'world');
  v_response := redis.get('hello');
  v_result   := v_response.str;
  DBMS_OUTPUT.PUT_LINE(v_result);

  v_response := redis.exec('get hello');
  v_result   := v_response.STR;
  DBMS_OUTPUT.PUT_LINE(v_result);

  redis.close();
END;
```

<br>
#APIs
Oredis presents some APIs to use Redis simply.<br>
'SET' and 'EXISTS' are PL/SQL keyword, so we can't use them as a function name.
PUT() is same with SET_().

###SIMPLE KEY : 

SET_|PUT|SETEX|GET|DEL|EXIST 
---|---|---|---|---|---


###HASH :

HSET|HGET|HDEL|HEXISTS 
---|---|---|---|---


###LIST :

LPUSH|LPOP|RPUSH|RPOP|LRANGE
---|---|---|---|---


###SET : 

SADD|SREM|SMEMBERS|SCARD
---|---|---|---


###SORTED SET : 

ZADD|ZREM|ZCARD|ZRANGE|ZRANGEBYSCORE|ZRANK|ZSCORE
---|---|---|---|---|---|---



<br>
#Exec() and Exec_async()
##Exec()
When you need to execute some complex commnads, use exec().<br>
And by using exec(), you can write your own APIs easily.
```
v_response := redis.EXEC('MSET {user:1000}.fname Michael {user:1000}.lname Jackson');
```
<br>
##Exec_async()
Oredis does not support Pipelining, but you can gain similar benefits by using Exec_async().<br>
Exec 
```
DECLARE
  redis_cluster OREDIS_CLUSTER;
  v_responses OREDIS_RESP_TABLE;
  v_response OREDIS_RESP;
BEGIN
  FOR i IN 1..1000000 LOOP
    v_int_val := redis_cluster.EXEC_ASYNC('SET a'||i || ' ' || (100000000 + i));
  END LOOP; 
    
  v_responses := redis_cluster.GET_ASYNC_RESPONSE();
  v_response := v_responses(1);
END;
```
<br>
#Support Cluster
In a cluster environment, use OREDIS_CLUSTER instead of OREDIS.

```
DECLARE
  redis_cluster OREDIS_CLUSTER;
  v_response OREDIS_RESP;
BEGIN
  redis_cluster := new OREDIS_CLUSTER('10.3.10.10:6379, 10.3.10.11:6379,readSlave=true');
  v_response := redis_cluster.get('foo');

  redis_cluster.close();
END;
```

<br>
#Connection string
The constructor of OREDIS and OREDIS_CLUSTER have one parameter 'p_config'(connection string).
Of course, most important part of connetcion string is specifying redis host address.
You can specify host address like below : <br>
```
new OREDIS('127.0.0.1') --use default port 6379
new OREDIS('127.0.0.1:6380')
```

In cluster environment, you can specify some cluster node addresses in connection string.
You don't need to list all the clster node addresses, only one available cluster node address is enough.
If OREDIS fail to get cluster nodes information, OREDIS will try to get the info from next cluster node. 
```
redis_cluster := new OREDIS_CLUSTER('10.3.10.10:6379, 10.3.11.10:6379,readSlave=true');
```

You can set several options in connection string.<br>
(Option names and values are not case sensitive.)

Option|Description
---|---
db|specify db id to connect (only in stand-alone environment)
password|when redis server require password use this option
timeout|pass to UTL_TCP.OPEN_CONNECTION()
inbuffersize|pass to UTL_TCP.OPEN_CONNECTION()
outbuffersize|pass to UTL_TCP.OPEN_CONNECTION()
readSlave|Enable read queries for connections to Redis cluster slave nodes. To read from slave nodes, you also need to set the 'preferNodeType' param  to 'S'.Read APIs and Exec() have 'preferNodeType' param. This instruction is not absolute. If there is no available salve node, Oredis try to read from master node automatically.

###Example : 
```
redis := new OREDIS('10.3.11.34:6379,password=1234,db=1,timeout=1,inBufferSize=1000');
redis_cluster := new OREDIS_CLUSTER('10.3.10.10:6379, 10.3.11.10:6379,readSlave=true');
```

<br>
#OREDIS_RESP
OREDIS_RESP holds the response of Redis server.<br>
OREDIS_RESP has 5 attributes :

Attribute|Description
---|---
type|reprsents response type in number<br>there are 7 response types(STRING, ARRAY, INTEGER, NIL, STATUS, ERROR, RAW)
str|response value in string
int|if the response value is number foramt, the value is converted to number type and set to 'int'
item|if the response type is array, you can access all the item by this attr.<br>PL/SQL does not allows recrursive data structure, so it becames somewhat messy.<br>OREDIS_RESP_ITEM_TABLE is a array of OREDIS_RESP_ITEM. OREDIS_RESP_ITEM has 3 attributes 'type', 'str', 'int' like OREDIS_RESP
item_cnt|item count

###Example : 
```
IF v_response.TYPE = PKG_OREDIS.REPLY_ERROR THEN
  v_str_val1 := v_response.STR;
ELSIF v_response.TYPE = PKG_OREDIS.REPLY_INTEGER THEN  
  --If the response type is INTEGER, you can get both integer and string value.
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
```

<br>
#Testing and Debugging
* Modify 'PKG_OREDIS_TEST' package and run.
* OREDIS presents some ASSERT() functions to debug and test. Refer to 'PKG_OREDIS' and 'PKG_OREDIS_TEST' packages.  
* OREDIS declares some user-defined exception and error number to define own error message. They are declared in spec of 'PKG_OREDIS'.
* You can trace the call stack by examining DBMS_UTILITY.FORMAT_ERROR_BACKTRACE when exception occurs.


<br>
#Setting a value which has quotation marks or white spaces
```
--When SET a value includes White Space, wrap the value with double quotation(") 
v_response := redis.EXEC('SET ' || v_key1 || ' ' || '" abc def "');  -- SET " abc def "

--When SET a value includes Single Quotation('), use double Single Quotation('') instead of one Single Quotation(')
v_response := redis_cluster.EXEC('SET ' || v_key1 || ' ' || 'abc''123''def');  -- SET abc'123'def

--When SET a value includes Double Quotaion("), use (\") instead of Double Quotation(") 
v_response := redis_cluster.EXEC('SET ' || v_key1 || ' ' || 'abc\"123\"def');  --SET abc"123"def

--When SET a value wraped with Single Quotation('), wrap the value with Double Quotation(") and use Double Single Quotation('') instead of (')
v_response := redis_cluster.EXEC('SET ' || v_key1 || ' ' || '"''abc''"');  --SET "'abc'"

--When SET a value wraped with Double Quotation("), wrap the value with double Double Quotation("") instead of (")
v_response := redis_cluster.EXEC('SET ' || v_key1 || ' ' || '""abc""');

```


<br>
#Tested Environment
  - Oracle 10g on Windows Server 2008(32bit), Redis 3.2.1 on Windows 7(64bit)
  - Oracle 11g on Windows Server 2012(64bit), Redis 3.2.1 on Windows Server 2012(64bit)
  - Oracle 11g on Windows Server 2012(64bit), Redis 3.2.6 on Ubuntu 14.04(64bit)
  

<br>
#Known Issue
1. delay when can not connect to Redis Server<br>
When the Redis server is not accessible UTL_TCP.OPEN_CONNECTION() hangs over 10 seconds.<br>
I think we need another way to determine if the server is reachable.


<br>
#License
Unless otherwise noted, the source files are distributed under the MIT License found in the LICENSE.txt file.
