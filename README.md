# Key-Value-Store-Go

- An interactive shell that allows access to a transactional in-memory key/value store built in golang

- The shell accepts the following commands:

| Command | Desription |
| ---|---|
|`SET`| Sets the given key to the specified value. A key can also be updated.|
|`GET`|Prints out the current value of the specified key.|
|`DELETE`|Deletes the given key. If the key has not been set, ignore.|
|`COUNT`|Returns the number of keys that have been set to the specified value. If no keys have been set to that value, prints 0.|
|`BEGIN`|Starts a transaction. These transactions allow you to modify the state of the system and commit or rollback your changes.|
|`END`|Ends a transaction. Everything done within the "active" transaction is lost.||
|`ROLLBACK`|Throws away changes made within the context of the active transaction. If no transaction is active, prints "No Active Transaction".|
|`COMMIT`|Commits the changes made within the context of the active transaction and ends the active transaction.|

- Sample Transaction :

```
> BEGIN
> SET X 200
> SET Y 14
> GET Y
14
```
