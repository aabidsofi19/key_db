# Layer-Db

Layer Db is a lightweight and blazingly fast embedded key-value database. It is designed to be an in-memory but persisted database that uses append-only logging for persistence in a separate thread, to reduce performance hits due to persistence.

I created Layer Db to provide a simple and efficient way to store and retrieve data in a key-value format, without the need for a separate database server or external dependencies. It is written in Rust for speed and safety, and is easy to install and use.

With Layer Db, you can quickly and easily store and retrieve data using simple key-value pairs, and access the data from any application or process that has access to the database file. It is ideal for applications that require fast and efficient access to data, without the overhead of a full-featured database server.

I hope that you will find Layer Db to be a valuable tool in your development efforts, and I welcome any feedback or suggestions for improvements.

Written in Rust for speed and safety :) .

## Installation

To install Layer Db, use pip:

```bash
pip install layer-db
```

## Usage

To use Layer Db, import the layer_db module and call the load() function to load a database file:

```python
import layer_db

db = layer_db.load("test.db")
```
This will load an existing database file called test.db, or create a new, empty database if the file does not exist.

Once the database is loaded, you can use the set() method to store key-value pairs in the database:

```python
db.set("hello", "world")
```
This will store the value "world" in the database, associated with the key "hello". The set() method returns True if the operation is successful, or raises an exception if there is an error.

To retrieve a value from the database, use the get() method, passing in the key for the value you want to retrieve:

```python
value = db.get("hello")
```
This will return the value associated with the key "hello", or None if the key does not exist in the database.

To remove a key-value pair from the database, use the remove() method, passing in the key for the pair you want to remove:

```python
value = db.remove("hello")
```
This will remove the key-value pair associated with the key "hello" from the database, and return the value that was removed.

When you are finished using the database, call the close() method to close the database and shut down any background threads:

```python
db.close()
```
This will ensure that all data is properly persisted to disk and that all background threads are terminated.

## Todo

Make Layer Db compatible with the Redis API
Add an option to disable persistence, for use in situations where data can be safely discarded after the application exits.
