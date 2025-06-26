# KVStorage
Implementation of a key-value storage

### Usage
```python src/cli.py [-h] storage {get,set,delete} items [items ...]```

<span style="color:gray">set command expects a list of key=value pairs separated by spaces</span><br>
<span style="color:gray">get and delete commands expect a list of keys separated by spaces</span>

### Examples
- ```python src/cli.py storage set name=Egor age=18```
- ```python src/cli.py storage get name```
- ```python src/cli.py storage delete name```

---
© 2025 Egor Vetoshkin