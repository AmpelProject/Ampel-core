
.. _testing:

Running the automated tests
===========================

Ampel ships with a suite of automated tests. You should run these before
pushing new code to GitHub.

Why test?
*********

  Vertrauen ist gut. Kontrolle ist besser.
    -- German proverb

It is important to test software, and there are many ways to do it. The most
basic kind of testing that nearly everyone does without thinking about it is
the "manual" variety: run a piece of code (either at the console or in an
iPython notebook) using inputs that you choose, check the output, and compare
it with your mental model of what the code should have done. This is fine for
small, throw-away, or single-developer/single-user projects. As soon as the
project is expected to have multiple contributors and users, needs to be
maintained for a long time, or is too large to understand in an afternoon, it
needs complete documentation. Automated tests document the way each component
is supposed to work (unit tests) and how they are supposed to work together
(integration tests). Because their setup, testing, and teardown phases are
automated and self-contained, they can be run often, without detailed knowledge
of the entire project. This makes it harder for changes to accidentally break
apparently unrelated parts of the codebase.

Expected installation layout
****************************

Choose a root directory, e.g. on my machine, /Users/jakob/Documents/ZTF/Ampel.
For simplicity, we will call this `$AMPEL_ROOT` from here on out. Clone Ampel
and the plugins you choose to use into subdirectories, e.g.::
  
  cd $AMPEL_ROOT
  git clone git@github.com:AmpelProject/Ampel.git ampel-core
  git clone git@github.com:AmpelProject/ampel-contrib-hu.git ampel-contrib-hu
  git clone git@github.com:robertdstein/Ampel-ZTFbh ampel-contrib-ztfbh
  git clone git@github.com:AmpelProject/Ampel-Neutrino.git ampel-neutrino

Now, create a `conda` environment for development, and activate it::
  
  conda env create --file ampel-core/deploy/docker-images/devel/base-environment.yml -n ampeltest
  . activate ampeltest

When this is done, you should see a prompt like the following::
  
  (ampeltest) [jakob@znb34-w:ZTF/Ampel]$

.. note:: While you can use whatever Python environment you have lying around,
   creating the enviroment from the provided yml file ensures that it
   matches the one used in production. If you need to add
   more packages, be sure to add them to the environment.yml file as well.

Inside your environment, use `pip install -e` to register each component::
  
  for dir in ampel-co*; do
    pip install -e $dir
  done

Next, `install and start Docker <https://docs.docker.com/get-started/>`_. The
tests use Docker to provide single-use instances of the databases that Ampel
interacts with.

Running the tests
*****************

From `$AMPEL_ROOT`, `pytest` will discover and run all tests in the
subdirectories named '*test*'. You'll get a report of the tests as they run, e.g.::
  
  (ampeltest) [jakob@znb34-w:ZTF/Ampel]$ pytest ampel-*/test                                                         (07-02 16:01)
  ====================================================== test session starts ======================================================
  platform darwin -- Python 3.6.5, pytest-3.6.2, py-1.5.4, pluggy-0.6.0
  rootdir: /Users/jakob/Documents/ZTF/Ampel, inifile:
  plugins: remotedata-0.3.0, openfiles-0.2.0, doctestplus-0.1.2, arraydiff-0.2
  collected 30 items

  ampel-contrib-hu/test/test_catalog_ingester.py ..                                                                         [  6%]
  ampel-contrib-hu/test/test_marshal_publisher.py ..                                                                        [ 13%]
  ampel-contrib-neutrino/test/test_t3.py ..                                                                                 [ 20%]
  ampel-core/test/test_alertprocessor.py ..                                                                                 [ 26%]
  ampel-core/test/test_archivedb.py .....s..                                                                                [ 53%]
  ampel-core/test/test_delayed_t0.py .ss                                                                                    [ 63%]
  ampel-core/test/test_resources.py .....                                                                                   [ 80%]
  ampel-core/test/test_t3_controller.py x.....                                                                              [100%]

Failures will produce a detailed traceback of exactly which assertion failed.

Test coverage
*************

Tests can only help pinpoint errors if they actually run the code in question.
Unless you were thorough enough to write your tests before the actual code they
test, you are likely to not have tests that exercise every part of your code.
Pytest has a coverage plugin to help you find these places. To generate a
coverage report, pass some extra options to `pytest`, e.g.::
  
  pytest --cov-report html --cov ampel ampel-*/test
  open htmlcov/index.html

This will open a web page with a listing of all the modules `ampel` that were
imported along with the fraction of lines that were executing during the test.
Examining this listing will help you find places to put new tests.

Writing (good) tests
********************

Any Python file named 'test/test_*.py' will be searched for functions starting
with `test_`. Use `assert` to document your assumptions about function return
values and shared state.
