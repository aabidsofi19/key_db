# Layer-Db

Layer Db is Lightweight and Blazingly Fast embedded Key-Value Database.
It is an in-memory but persisted database which uses Append only logging for persistence
in an separate thread to reduce performance hits due to persistence.

Written in Rust for speed and safety :) .

# Installation

```bash

  pip install layer-db

```

# Its Simple , Fast and works .

```python

  import layer_db

  db = layer_db.load("test.db") # Loads A Database .
                                # Intializes and empty database if file doesnt exist

  db.set("Hello","World")       # Returns True if succesfull else raises an exception
  True

  db.get("Hello")               # returns the value if exists otherwise returns None
  'world'

  db.remove("Hello")            # Removes the value from database and returns it .

  db.close()                    # Closes the Database and background threads


```

## Todo

- Make Compatible with Redis
- Add Non-persisting option ( Currently only working in persisting mode)
