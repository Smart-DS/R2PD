# Sphinx Documentation

The documentation is built with [Sphinx](http://sphinx-doc.org/index.html). See their documentation for (a lot) more details.

## Installation

To generate the docs yourself, you'll need the appropriate package:

```
pip install sphinx
```

## Refreshing the API Documentation

- Make sure R2PD is in your PYTHONPATH
- Delete the contents of `source/R2PD`.
- Run `sphinx-apidoc -o source/R2PD ..` from the `docs` folder.
- Compare `source/R2PD/modules.rst` to `source/R2PD.rst`.
- 'git push' changes to the documentation source code as needed.
- Make the documentation per below

## Building HTML Docs

### Mac/Linux

```
make html
```

### Windows

```
make.bat html
```

## Building PDF Docs

To build a PDF, you'll need a latex distribution for your system.

### Mac/Linux

```
make latexpdf
```

### Windows

```
make.bat latexpdf
```

## Pushing to GitHub Pages

### Mac/Linux

```
make github
```

### Windows

```
make.bat html
```

Then run the github-related commands by hand:

```
git branch -D gh-pages
git push origin --delete gh-pages
ghp-import -n -b gh-pages -m "Update documentation" ./build/html
git checkout gh-pages
git push origin gh-pages
git checkout master # or whatever branch you were on
```
