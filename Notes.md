- RESP
    - REdis Serialization Protocol
    - 5 data types
        1. Simple string
        2. Error
        3. Integer
        4. Bulk string
        5. Array
    - client sends a command to redis always in RESP Array of Bulk strings
    - If you type ECHO "hello world" in redis-cli, the CLI serializes it into this raw string:
        *2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n