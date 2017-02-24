# Build Documentation

```
pip install sphinx
# make sure the resource package is in your PYTHONPATH
sphinx-apidoc -f -M -o source/api ..
make.bat html # Windows
Makefile html # Linux/Mac
```