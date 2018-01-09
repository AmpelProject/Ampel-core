
Updating the documentation on GitHub
====================================

The project documentation is rendered with Sphinx and hosted on GitHub using
the `gh-pages` branch mechanism. The workflow for updating the rendered
documentation mostly follows `this
tutorial <https://daler.github.io/sphinxdoc-test/includeme.html>`_.

.. highlight:: shell

1. Clone the master branch of the Ampel repository to your machine::
     
     git clone git@github.com:AmpelProject/Ampel.git Ampel

2. Clone the gh-pages branch into a directory Ampel-docs/html at the same level
   (the relative path is important, since it's hard-coded into the documentation
   Makefile)::
     
     git clone -b gh-pages --single-branch git@github.com:AmpelProject/Ampel.git Ampel-docs/html

3. Build the documentation::
     
     cd Ampel/docs
     make html

4. Commit and push the changes to the gh-pages branch::
     
     cd ../../Ampel-docs/html
     git commit -m "Updated docs" *
     git push origin gh-pages
