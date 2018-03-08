#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AmpelABC.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 27.12.2017
# Last Modified Date: 08.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import inspect

def abstractmethod(func):
	"""
	Custom decorator to mark selected method as abstract.
	It populates the static array "_abstract_methods" of class AmpelABC.
	"""
	AmpelABC._abstract_methods.add(func.__name__)
	return func


class AmpelABC(type):
	"""
	Metaclass that implements similar functionalities to python standart 
	ABC module (Abstract Base Class) while additionaly checking method 
	signatures and enforcing the definition of the class variable 'version'.
	As a consequence, a child class that extends a parent class whose 
	metaclass is AmpelABC, will not be able to implement the abstract methods 
	of the parent class with different method arguments.
	"""

	_abstract_methods = set()


	@staticmethod
	def generate_new(abclass):
		"""
		Forbids instanciation of abstract classes.
		The function generated in this function is included
		in the abstract base class.
		Parameter is the abstract base *class*
		"""
		def __new__(mcs, *args, **kwargs):
			if (mcs is abclass):
				raise TypeError("Abstract class "+ abclass.__name__ + " cannot be instantiated")
			return object.__new__(mcs)
		return __new__


	@staticmethod
	def generate__init_subclass__(abclass):
		"""
		__init_subclass__() was added to Python 3.6 allowing customisation of class creation.
		It is a method of a parent class called by the child class during class creation.
		The function generate__init_subclass__ generates a __init_subclass__ function 
		that checks if the signatures of the abstract methods of the parent object
		are equal to the signatures of the child object.

		A NotImplementedError exception is throwed if the child object:
			* Does not implement an abstract method
			* Does not define a class variable named 'version' 

		A ValueError exception is throwed if the child object:
			* Does implement an abstract method with a divergent signature
			* The class variable 'version' is not a float

		Parameter is the abstract base *class*
		"""

		def __init_subclass__(cls):

			for method_name in cls._abstract_methods:
	
				# Check if method was implemented by child
				func = getattr(cls, method_name, False)
				if func:
					if func.__qualname__.split(".")[0] == abclass.__name__:
						raise NotImplementedError(
							"Method " + method_name  + " is not implemented"
						)
	
				# Check if method signatures are equal
				abstract_sig = inspect.signature(getattr(abclass, method_name))
				child_sig = inspect.signature(getattr(cls, method_name))
	
				# Check that number of parameters are equal rather than checking 
				# if parameter names are identical (if abstract_sig != child_sig)
				if len(abstract_sig.parameters) != len(child_sig.parameters):
					raise ValueError(
						"Method " + method_name +
						"() has a wrong signature, please check defined arguments"
					)
	
				# Retrieve class variable version
				child_version = getattr(cls, "version", None)

				if child_version is None:
					raise NotImplementedError(
						"Please define a class variable named 'version' in %s" % cls
					)
				
				if type(child_version) is not float:
					raise NotImplementedError(
						"Class variable 'version' must be float (now: %s)" % 
						type(child_version)
					)


		return __init_subclass__


	def __new__(metacls, name, bases, d):
		"""
		Creates the class
		"""

		# If the static array _abstract_methods is populated, 
		# then we are creating the abstract base class
		if len(AmpelABC._abstract_methods) > 0:

			# reference empty list of abstract methods in the abstract base class properties
			d['_abstract_methods'] = []

			# add the abstract method names (the ones marked with custom decorator) to class properties
			for el in AmpelABC._abstract_methods:
				d['_abstract_methods'].append(el)

			# reset static array (AmpelABC should be usable by different classes)
			AmpelABC._abstract_methods = set()

			# Generated custom __init_subclass__() method. 
			# Since this function requires a reference to the *class* (getattr(abclass, method_name)), 
			# and since we don't have one yet (we are creating it), we create one on the fly 
			# just for the sake of generating a working __init_subclass__() method 
			d['__init_subclass__'] = AmpelABC.generate__init_subclass__(
				type.__new__(metacls, name, bases, d)
			)

			# It gets trickier here. To forbid instanciation of the abstract base class
			# we customize the method __new__ of the ab class.
			# We thereby check if the provided class parameter *is* the abstract class.
			# For *is* to return true, we need to create the abstract class now and 
			# later insert the method __new__ to the class attributes. 
			# Only then, 'is' will return True for the ab class and False for the child class
			abclass = type.__new__(metacls, name, bases, d)
			setattr(abclass, '__new__', AmpelABC.generate_new(abclass))
			return abclass

		return type.__new__(metacls, name, bases, d)
