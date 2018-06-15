
Ampel: Alert Management, Photometry and Evaluation of Light curves
==================================================================

Astronomers have during the past century continuously refined tools for
analyzing individual astronomical transients. Simultaneously, progress in instrument and CCD
manufacturing as well as new data processing capabilities have led to a new generation of transient
surveys that can repeatedly scan large volumes of the Universe. With thousands of potential candidates
available, scientists are faced with a new kind of questions: Which transient should I focus on?
What were those things that I dit not look at? Can I have them all?

Ampel is a software framework meant to assist in answering such questions.
In short, Ampel assists in the the transition from studies of individual objects
(based on more or less random selection) to systematically selected samples.
Our design goals are to find a system where past experience (i.e. existing algorithms and code) can consistently be applied to large samples, and with built-in tools for controlling sample selection.


Intalling Ampel
===============

This installation assumes your python is under the Anaconda environment, using python 3+

1. ``git clone https://github.com/AmpelProject/Ampel.git"``

2. ``conda create -n ampel --file Ampel/requirements.txt python=3`` this will create an autonomous and independent python environment on your computer. (No conflict with you current installation)

3. ``source activate ampel`` (you will enter the AMPEL environment)

4. ``pip install -e Ampel`` (Ampel actually being path/to/Ampel, if you are not in the directory where Ampel is)
