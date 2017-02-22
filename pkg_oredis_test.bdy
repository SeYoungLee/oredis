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
        
  redis_cluster := NEW OREDIS_CLUSTER('10.3.10.53:6379, 10.3.11.35:6379,readSlave=true'); --,TIMEOUT=1,inBufferSize=1000,outBufferSize=2000,password=pass
  
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

